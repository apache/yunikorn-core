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

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

// Interface that all placement rules need to implement.
type rule interface {
	// Initialise the rule from the configuration.
	// An error may only be returned if the configuration is not correct.
	initialise(conf configs.PlacementRule) error

	// Execute the rule and return the queue getName the application is placed in.
	// Returns the fully qualified queue getName if the rule finds a queue or an empty string if the rule did not match.
	// The error must only be set if there is a failure while executing the rule not if the rule did not match.
	placeApplication(app *cache.ApplicationInfo, info *cache.PartitionInfo) (string, error)

	// Return the getName of the rule which is defined in the rule.
	// The basicRule provides a "unnamed rule" implementation.
	getName() string

	// Return the parent rule.
	// This method is implemented in the basicRule which each rule must be based on.
	getParent() rule
}

// Basic structure that every placement rule uses.
// The rules themselves should include the basicRule struct.
// Linter does not pick up on the usage in the implementation(s).
//nolint:structcheck
type basicRule struct {
	create bool
	parent rule
	filter Filter
}

// Get the parent rule used in testing only.
// Should not be implemented in rules.
func (r *basicRule) getParent() rule {
	return r.parent
}

// Return the name if not overwritten by the rule.
// Marked as nolint as rules should override this.
//nolint:unused
func (r *basicRule) getName() string {
	return "unnamed rule"
}

func (r *basicRule) executeParentPlacement(app *cache.ApplicationInfo, info *cache.PartitionInfo) (string, error) {
	var parentName string
	var err error

	// run the parent rule if set
	parent := r.getParent()
	if parent != nil {
		parentName, err = parent.placeApplication(app, info)
		// failed parent rule, fail this rule
		if err != nil {
			return "", err
		}
		// rule did not return a parent: this could be filter or create flag related
		if parentName == "" {
			return "", nil
		}
		// check if this is a parent queue and qualify it
		if !strings.HasPrefix(parentName, configs.RootQueue+cache.DOT) {
			parentName = configs.RootQueue + cache.DOT + parentName
		}
		if info.GetQueue(parentName).IsLeafQueue() {
			return "", fmt.Errorf("parent rule returned a leaf queue: %s", parentName)
		}
	}
	// the parent is set from the rule otherwise set it to the root
	if parentName == "" {
		parentName = configs.RootQueue
	}
	return parentName, nil
}

// Create a new rule based on the getName of the rule requested. The rule is initialised with the configuration and can
// be used directly.
func newRule(conf configs.PlacementRule) (rule, error) {
	// create the rule from the config
	var newRule rule
	var err error
	// create the new rule fail if the name is unknown
	switch normalise(conf.Name) {
	// rule that uses the user's name as the queue
	case "user":
		newRule = &userRule{}
	// rule that uses a fixed queue name
	case "fixed":
		newRule = &fixedRule{}
	// rule that uses the queue provided on submit
	case "provided":
		newRule = &providedRule{}
	// rule that uses a tag from the application (like namespace)
	case "tag":
		newRule = &tagRule{}
	// rule that uses a mapping of a tag from the application
	case "mapping":
		newRule = &mappingRule{}
	// test rule not to be used outside of testing code
	case "test":
		newRule = &testRule{}
	default:
		return nil, fmt.Errorf("unknown rule name specified %s, failing placement rule config", conf.Name)
	}

	// initialise the rule: do not expect the rule to log errors
	err = newRule.initialise(conf)
	if err != nil {
		log.Logger().Error("Rule init failed", zap.Error(err))
		return nil, err
	}
	log.Logger().Debug("New rule created", zap.Any("ruleConf", conf))
	return newRule, nil
}

// Normalise the rule name from the config.
// We do not have to check all possible permutations for capitalisation, just a lower case match.
func normalise(name string) string {
	return strings.ToLower(name)
}

// Replace all dots in the generated queue name before making it a fully qualified name.
func replaceDot(name string) string {
	return strings.Replace(name, cache.DOT, cache.DotReplace, -1)
}
