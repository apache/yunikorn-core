/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package placement

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

// Interface that all placement rules need to implement.
type rule interface {
	// Initialise the rule from the configuration.
	// An error may only be returned if the configuration is not correct.
	initialise(conf configs.PlacementRule) error

	// Execute the rule and return the queue getName the application is placed in.
	// Returns the fully qualified queue getName if the rule finds a queue or an empty string if the rule did not match.
	// The error must only be set if there is a failure while executing the rule not if the rule did not match.
	placeApplication(app *objects.Application, queueFn func(string) *objects.Queue) (string, error)

	// Return the getName of the rule which is defined in the rule.
	// The basicRule provides a "unnamed rule" implementation.
	getName() string

	// Return the parent rule.
	// This method is implemented in the basicRule which each rule must be based on.
	getParent() rule

	// Returns the rule in a form that can be exposed via the REST api
	// This method is implemented in the basicRule which each rule must be based on.
	ruleDAO() *dao.RuleDAO
}

// Basic structure that every placement rule uses.
// The rules themselves should include the basicRule struct.
// Linter does not pick up on the usage in the implementation(s).
//
//nolint:structcheck
type basicRule struct {
	create bool
	parent rule
	filter Filter
}

// getParent gets the parent rule used in testing only.
// Should not be implemented in rules.
func (r *basicRule) getParent() rule {
	return r.parent
}

const unnamedRuleName = "unnamed rule"

// getName returns the name if not overwritten by the rule.
// Marked as nolint as rules should override this.
//
//nolint:unused
func (r *basicRule) getName() string {
	return unnamedRuleName
}

// ruleDAO returns the RuleDAO object if not overwritten by the rule.
// Marked as nolint as rules should override this.
//
//nolint:unused
func (r *basicRule) ruleDAO() *dao.RuleDAO {
	return &dao.RuleDAO{
		Name: r.getName(),
	}
}

// newRule creates a new rule based on the getName of the rule requested. The rule is initialised with the configuration
// and can be used directly.
// Note that the recoveryRule should not be added to the list as it is an internal rule only that should not be part of
// the configuration.
func newRule(conf configs.PlacementRule) (rule, error) {
	// create the rule from the config
	var r rule
	var err error
	// create the new rule fail if the name is unknown
	switch normalise(conf.Name) {
	// rule that uses the user's name as the queue
	case types.User:
		r = &userRule{}
	// rule that uses a fixed queue name
	case types.Fixed:
		r = &fixedRule{}
	// rule that uses the queue provided on submit
	case types.Provided:
		r = &providedRule{}
	// rule that uses a tag from the application (like namespace)
	case types.Tag:
		r = &tagRule{}
	// recovery rule must not be specified in the config
	case types.Recovery:
		return nil, fmt.Errorf("recovery rule cannot be part of the config, failing placement rule config")
	// test rule not to be used outside of testing code
	case types.Test:
		r = &testRule{}
	default:
		return nil, fmt.Errorf("unknown rule name specified %s, failing placement rule config", conf.Name)
	}

	// initialise the rule: do not expect the rule to log errors
	err = r.initialise(conf)
	if err != nil {
		log.Log(log.Config).Error("Rule init failed", zap.Error(err))
		return nil, err
	}
	log.Log(log.Config).Debug("New rule created", zap.Any("ruleConf", conf))
	return r, nil
}

// Normalise the rule name from the config.
// We do not have to check all possible permutations for capitalisation, just a lower case match.
func normalise(name string) string {
	return strings.ToLower(name)
}

// Replace all dots in the generated queue name before making it a fully qualified name.
func replaceDot(name string) string {
	return strings.ReplaceAll(name, configs.DOT, configs.DotReplace)
}
