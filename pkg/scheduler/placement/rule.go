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

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

// Interface that all placement rules need to implement.
type Rule interface {
	// Initialise the Rule from the configuration.
	// An error may only be returned if the configuration is not correct.
	initialise(conf configs.PlacementRule) error

	// Execute the Rule and return the queue getName the application is placed in.
	// Returns the fully qualified queue getName if the Rule finds a queue or an empty string if the Rule did not match.
	// The error must only be set if there is a failure while executing the Rule not if the Rule did not match.
	// FIXME: Now disabled placement
	// PlaceApplication(app *cache.ApplicationInfo, info *cache.PartitionInfo) (string, error)

	// Return the getName of the Rule which is defined in the Rule.
	// The basicRule provides a "unnamed Rule" implementation.
	GetName() string

	// Return the parent Rule.
	// This method is implemented in the basicRule which each Rule must be based on.
	getParent() Rule
}

// Basic structure that every placement Rule uses.
// The rules themselves should include the basicRule struct.
// Linter does not pick up on the usage in the implementation(s).
//nolint:structcheck
type basicRule struct {
	create bool
	parent Rule
	filter Filter
}

// Get the parent Rule used in testing only.
// Should not be implemented in rules.
func (r *basicRule) getParent() Rule {
	return r.parent
}

// Return the name if not overwritten by the Rule.
// Marked as nolint as rules should override this.
//nolint:unused
func (r *basicRule) getName() string {
	return "unnamed Rule"
}

// Create a new Rule based on the GetName of the Rule requested. The Rule is initialised with the configuration and can
// be used directly.
func NewRule(conf configs.PlacementRule) (Rule, error) {
	// create the Rule from the config
	var newRule Rule
	var err error

	// FIXME: now disabled placement
	// // create the new Rule fail if the name is unknown
	// switch normalise(conf.Name) {
	// // Rule that uses the user's name as the queue
	// case "user":
	// 	newRule = &userRule{}
	// // Rule that uses a fixed queue name
	// case "fixed":
	// 	newRule = &fixedRule{}
	// // Rule that uses the queue provided on submit
	// case "provided":
	// 	newRule = &providedRule{}
	// // Rule that uses a tag from the application (like namespace)
	// case "tag":
	// 	newRule = &tagRule{}
	// // test Rule not to be used outside of testing code
	// case "test":
	// 	newRule = &testRule{}
	// default:
	// 	return nil, fmt.Errorf("unknown Rule name specified %s, failing placement Rule config", conf.Name)
	// }

	// initialise the Rule: do not expect the Rule to log errors
	err = newRule.initialise(conf)
	if err != nil {
		log.Logger().Error("Rule init failed", zap.Error(err))
		return nil, err
	}
	log.Logger().Debug("New Rule created", zap.Any("ruleConf", conf))
	return newRule, nil
}

// Normalise the Rule name from the config.
// We do not have to check all possible permutations for capitalisation, just a lower case match.
func normalise(name string) string {
	return strings.ToLower(name)
}

// Replace all dots in the generated queue name before making it a fully qualified name.
// FIXME; Now disabled placement
// func replaceDot(name string) string {
// 	return strings.Replace(name, cache.DOT, cache.DotReplace, -1)
// }

// Build the rule set based on the config.
// If the rule set is correct and can be used the new set is returned.
// If any error is encountered a nil array is returned and the error set
func BuildRules(rules []configs.PlacementRule) ([]Rule, error) {
	// catch an empty list
	if len(rules) == 0 {
		return nil, fmt.Errorf("placement manager rule list request is empty")
	}
	// build temp list from new config
	var newRules []Rule
	for _, conf := range rules {
		buildRule, err := NewRule(conf)
		if err != nil {
			return nil, err
		}
		newRules = append(newRules, buildRule)
	}
	return newRules, nil
}