/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
	"sync"

	"go.uber.org/zap"

	"github.com/cloudera/yunikorn-core/pkg/cache"
	"github.com/cloudera/yunikorn-core/pkg/common/configs"
	"github.com/cloudera/yunikorn-core/pkg/log"
)

type AppPlacementManager struct {
	name        string
	info        *cache.PartitionInfo
	rules       []rule
	initialised bool
	lock        sync.RWMutex
}

func NewPlacementManager(info *cache.PartitionInfo) *AppPlacementManager {
	m := &AppPlacementManager{
		name: info.Name,
		info: info,
	}
	rules := info.GetRules()
	if len(rules) > 0 {
		if err := m.initialise(rules); err != nil {
			log.Logger().Info("Placement manager created without rules: not active",
				zap.Error(err))
		}
	}
	return m
}

// Update the rules for an active placement manager
// Note that this will only be called when the manager is created earlier and the config is updated.
func (m *AppPlacementManager) UpdateRules(rules []configs.PlacementRule) error {
	if len(rules) > 0 {
		log.Logger().Info("Building new rule list for placement manager")
		if err := m.initialise(rules); err != nil {
			log.Logger().Info("Placement manager rules not reloaded",
				zap.Error(err))
			return err
		}
	}
	// if there are no rules in the config we should turn off the placement manager
	if len(rules) == 0 && m.initialised {
		m.lock.Lock()
		defer m.lock.Unlock()
		log.Logger().Info("Placement manager rules removed on config reload")
		m.initialised = false
		m.rules = make([]rule, 0)
	}
	return nil
}

// Return the state of the placement manager
func (m *AppPlacementManager) IsInitialised() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.initialised
}

// Initialise the rules from a parsed config.
func (m *AppPlacementManager) initialise(rules []configs.PlacementRule) error {
	log.Logger().Info("Building new rule list for placement manager")
	// build temp list from new config
	tempRules, err := m.buildRules(rules)
	if err == nil {
		m.lock.Lock()
		defer m.lock.Unlock()
		log.Logger().Info("Activated rule set in placement manager")
		m.rules = tempRules
		// all done manager is initialised
		m.initialised = true
		if log.IsDebugEnabled() {
			for rule := range m.rules {
				log.Logger().Debug("rule set",
					zap.Int("ruleNumber", rule),
					zap.String("ruleName", m.rules[rule].getName()))
			}
		}
	}
	return err
}

// Build the rule set based on the config.
// If the rule set is correct and can be used the new set is returned.
// If any error is encountered a nil array is returned and the error set
func (m *AppPlacementManager) buildRules(rules []configs.PlacementRule) ([]rule, error) {
	// catch an empty list
	if len(rules) == 0 {
		return nil, fmt.Errorf("placement manager rule list request is empty")
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
	return newRules, nil
}

func (m *AppPlacementManager) PlaceApplication(app *cache.ApplicationInfo) error {
	// Placement manager not initialised cannot place application, just return
	m.lock.RLock()
	defer m.lock.RUnlock()
	if !m.initialised {
		return nil
	}
	var queueName string
	var err error
	for _, checkRule := range m.rules {
		log.Logger().Debug("Executing rule for placing application",
			zap.String("ruleName", checkRule.getName()),
			zap.String("application", app.ApplicationID))
		queueName, err = checkRule.placeApplication(app, m.info)
		if err != nil {
			log.Logger().Error("rule execution failed",
				zap.String("ruleName", checkRule.getName()),
				zap.Error(err))
			app.QueueName = ""
			return err
		}
		// queueName returned make sure ACL allows access and create the queueName if not exist
		if queueName != "" {
			// get the queue object
			queue := m.info.GetQueue(queueName)
			// we create the queueName if it does not exists
			if queue == nil {
				current := queueName
				for queue == nil {
					current = current[0:strings.LastIndex(current, cache.DOT)]
					// check if the queue exist
					queue = m.info.GetQueue(current)
				}
				// Check if the user is allowed to submit to this queueName, if not next rule
				if !queue.CheckSubmitAccess(app.GetUser()) {
					log.Logger().Debug("Submit access denied on queue",
						zap.String("queueName", queue.GetQueuePath()),
						zap.String("ruleName", checkRule.getName()),
						zap.String("application", app.ApplicationID))
					// reset the queue name for the last rule in the chain
					queueName = ""
					continue
				}
				err = m.info.CreateQueues(queueName)
				// errors can occur when the parent queueName is already a leaf queueName
				if err != nil {
					app.QueueName = ""
					return err
				}
			} else {
				// Check if the user is allowed to submit to this queueName, if not next rule
				if !queue.CheckSubmitAccess(app.GetUser()) {
					log.Logger().Debug("Submit access denied on queue",
						zap.String("queueName", queueName),
						zap.String("ruleName", checkRule.getName()),
						zap.String("application", app.ApplicationID))
					// reset the queue name for the last rule in the chain
					queueName = ""
					continue
				}
			}
			// we have a queue that allows submitting and can be created: app placed
			break
		}
	}
	log.Logger().Debug("Rule result for placing application",
		zap.String("application", app.ApplicationID),
		zap.String("queueName", queueName))
	// no more rules to check no queueName found reject placement
	if queueName == "" {
		app.QueueName = ""
		return fmt.Errorf("application rejected: no placment rule matched")
	}
	// Add the queue into the application, overriding what was submitted
	app.SetQueue(m.info.GetQueue(queueName))
	return nil
}
