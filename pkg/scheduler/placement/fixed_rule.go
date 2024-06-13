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
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

// A rule to place an application based on the queue in the configuration.
// If the queue provided is fully qualified, starts with "root.", the parent rule is skipped and the queue is created as
// configured. If the queue is not qualified all "." characters will be replaced and the parent rule run before making
// the queue name fully qualified.
type fixedRule struct {
	basicRule
	queue     string
	qualified bool
}

func (fr *fixedRule) getName() string {
	return types.Fixed
}

func (fr *fixedRule) ruleDAO() *dao.RuleDAO {
	var pDAO *dao.RuleDAO
	if fr.parent != nil {
		pDAO = fr.parent.ruleDAO()
	}
	return &dao.RuleDAO{
		Name: fr.getName(),
		Parameters: map[string]string{
			"queue":     fr.queue,
			"create":    strconv.FormatBool(fr.create),
			"qualified": strconv.FormatBool(fr.qualified),
		},
		ParentRule: pDAO,
		Filter:     fr.filter.filterDAO(),
	}
}

func (fr *fixedRule) initialise(conf configs.PlacementRule) error {
	fr.queue = normalise(conf.Value)
	if fr.queue == "" {
		return fmt.Errorf("a fixed queue rule must have a queue name set")
	}
	fr.create = conf.Create
	fr.filter = newFilter(conf.Filter)
	// if we have a fully qualified queue name already we should not have a parent
	fr.qualified = strings.HasPrefix(fr.queue, configs.RootQueue)
	if fr.qualified && conf.Parent != nil {
		return fmt.Errorf("cannot have a fixed queue rule with qualified queue getName and a parent rule: %v", conf)
	}
	var err = error(nil)
	if conf.Parent != nil {
		fr.parent, err = newRule(*conf.Parent)
	}
	return err
}

func (fr *fixedRule) placeApplication(app *objects.Application, queueFn func(string) *objects.Queue) (string, error) {
	// before anything run the filter
	if !fr.filter.allowUser(app.GetUser()) {
		log.Log(log.SchedApplication).Debug("Fixed rule filtered",
			zap.String("application", app.ApplicationID),
			zap.Any("user", app.GetUser()),
			zap.String("queueName", fr.queue))
		return "", nil
	}
	queueName := fr.queue
	// if the fixed queue is already fully qualified skip the parent check
	if !fr.qualified {
		var parentName string
		var err error
		// run the parent rule if set
		if fr.parent != nil {
			parentName, err = fr.parent.placeApplication(app, queueFn)
			// failed parent rule, fail this rule
			if err != nil {
				return "", err
			}
			// rule did not return a parent: this could be filter or create flag related
			if parentName == "" {
				return "", nil
			}
			// check if this is a parent queue and qualify it
			if !strings.HasPrefix(parentName, configs.RootQueue+configs.DOT) {
				parentName = configs.RootQueue + configs.DOT + parentName
			}
			// if the parent queue exists it cannot be a leaf
			parentQueue := queueFn(parentName)
			if parentQueue != nil && parentQueue.IsLeafQueue() {
				return "", fmt.Errorf("parent rule returned a leaf queue: %s", parentName)
			}
		}
		// the parent is set from the rule otherwise set it to the root
		if parentName == "" {
			parentName = configs.RootQueue
		}
		childQueueName := replaceDot(fr.queue)
		if err = configs.IsQueueNameValid(childQueueName); err != nil {
			return "", err
		}
		queueName = parentName + configs.DOT + childQueueName
	}
	// Log the result before we check the create flag
	log.Log(log.SchedApplication).Debug("Fixed rule intermediate result",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	if err := configs.IsQueuePathValid(queueName); err != nil {
		return "", err
	}
	// get the queue object
	queue := queueFn(queueName)
	// if we cannot create the queue must exist
	if !fr.create && queue == nil {
		return "", nil
	}
	log.Log(log.SchedApplication).Info("Fixed rule application placed",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	return queueName, nil
}
