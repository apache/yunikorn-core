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
	"errors"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
)

// RejectedError is the standard error returned if placement has failed
var RejectedError = errors.New("application rejected: no placement rule matched")

type AppPlacementManager struct {
	rules   []rule
	queueFn func(string) *objects.Queue

	sync.RWMutex
}

func NewPlacementManager(rules []configs.PlacementRule, queueFunc func(string) *objects.Queue) *AppPlacementManager {
	m := &AppPlacementManager{
		queueFn: queueFunc,
	}
	if err := m.initialise(rules); err != nil {
		log.Log(log.Config).Error("Placement manager created without rules: not active", zap.Error(err))
	}
	return m
}

// Update the rules for an active placement manager
// Note that this will only be called when the manager is created earlier and the config is updated.
func (m *AppPlacementManager) UpdateRules(rules []configs.PlacementRule) error {
	log.Log(log.Config).Info("Building new rule list for placement manager")
	if err := m.initialise(rules); err != nil {
		log.Log(log.Config).Info("Placement manager rules not reloaded", zap.Error(err))
		return err
	}
	return nil
}

// Initialise the rules from a parsed config.
func (m *AppPlacementManager) initialise(rules []configs.PlacementRule) error {
	log.Log(log.Config).Info("Building new rule list for placement manager")
	// build temp list from new config
	tempRules, err := m.buildRules(rules)
	if err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()

	log.Log(log.Config).Info("Activated rule set in placement manager")
	m.rules = tempRules
	// all done manager is initialised
	for rule := range m.rules {
		log.Log(log.Config).Debug("rule set",
			zap.Int("ruleNumber", rule),
			zap.String("ruleName", m.rules[rule].getName()))
	}
	return nil
}

// Build the rule set based on the config.
// If the rule set is correct and can be used the new set is returned.
// If any error is encountered a nil array is returned and the error set
func (m *AppPlacementManager) buildRules(rules []configs.PlacementRule) ([]rule, error) {
	// empty list should result in a single "provided" rule
	if len(rules) == 0 {
		log.Log(log.Config).Info("Placement manager configured without rules: using implicit provided rule")
		rules = []configs.PlacementRule{{
			Name:   types.Provided,
			Create: false,
		}}
	}
	// build temp list from new config
	var newRules []rule
	for _, conf := range rules {
		buildRule, err := newRule(conf)
		if err != nil {
			return nil, err
		}
		newRules = append(newRules, buildRule)
	}
	// ensure the recovery rule is always present
	newRules = append(newRules, &recoveryRule{})

	return newRules, nil
}

func (m *AppPlacementManager) PlaceApplication(app *objects.Application) error {
	m.RLock()
	defer m.RUnlock()

	var queueName string
	var err error
	for _, checkRule := range m.rules {
		log.Log(log.SchedApplication).Debug("Executing rule for placing application",
			zap.String("ruleName", checkRule.getName()),
			zap.String("application", app.ApplicationID))
		queueName, err = checkRule.placeApplication(app, m.queueFn)
		if err != nil {
			log.Log(log.SchedApplication).Error("rule execution failed",
				zap.String("ruleName", checkRule.getName()),
				zap.Error(err))
			app.SetQueuePath("")
			return err
		}
		// no queue name next rule
		if queueName == "" {
			continue
		}
		// We have the recovery queue bail out: only if we are doing forced placement
		// Recovery rule is last in the list. Recovery queue cannot be returned by other rules.
		// We do not want to trigger any checks for this queue.
		if queueName == common.RecoveryQueueFull && app.IsCreateForced() {
			log.Log(log.SchedApplication).Info("Placing application in recovery queue",
				zap.String("application", app.ApplicationID))
			break
		}
		// queueName returned make sure ACL allows access and set the queueName in the app
		queue := m.queueFn(queueName)
		// walk up the tree if the queue does not exist
		if queue == nil {
			current := queueName
			for queue == nil {
				current = current[0:strings.LastIndex(current, configs.DOT)]
				// check if the queue exist
				queue = m.queueFn(current)
			}
			// Check if the user is allowed to submit to this queueName, if not next rule
			if !queue.CheckSubmitAccess(app.GetUser()) {
				log.Log(log.SchedApplication).Debug("Submit access denied on queue",
					zap.String("queueName", queue.GetQueuePath()),
					zap.String("ruleName", checkRule.getName()),
					zap.String("application", app.ApplicationID))
				// reset the queue name for the last rule in the chain
				queueName = ""
				continue
			}
		} else {
			// Check if this final queue is a leaf queue, if not next rule
			if !queue.IsLeafQueue() {
				log.Log(log.SchedApplication).Debug("Rule returned parent queue",
					zap.String("queueName", queueName),
					zap.String("ruleName", checkRule.getName()),
					zap.String("application", app.ApplicationID))
				// reset the queue name for the last rule in the chain
				queueName = ""
				continue
			}
			// Check if the user is allowed to submit to this queueName, if not next rule
			if !queue.CheckSubmitAccess(app.GetUser()) {
				log.Log(log.SchedApplication).Debug("Submit access denied on queue",
					zap.String("queueName", queueName),
					zap.String("ruleName", checkRule.getName()),
					zap.String("application", app.ApplicationID))
				// reset the queue name for the last rule in the chain
				queueName = ""
				continue
			}
		}
		// we have a queue that allows submitting and can be created: app placed
		log.Log(log.SchedApplication).Info("Rule result for placing application",
			zap.String("application", app.ApplicationID),
			zap.String("ruleName", checkRule.getName()),
			zap.String("queueName", queueName))
		break
	}
	// no more rules to check no queueName found reject placement
	if queueName == "" {
		app.SetQueuePath("")
		return RejectedError
	}
	// Add the queue into the application, overriding what was submitted
	app.SetQueuePath(queueName)
	return nil
}
