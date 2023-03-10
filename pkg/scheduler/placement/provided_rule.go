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
)

// A rule to place an application based on the queue provided by the user on submission.
// If the queue provided is fully qualified, starts with "root.", the parent rule is skipped and the queue is created as
// provided. If the queue is not qualified all "." characters will be replaced and the parent rule run before making the
// queue name fully qualified.
type providedRule struct {
	basicRule
}

func (pr *providedRule) getName() string {
	return types.Provided
}

func (pr *providedRule) initialise(conf configs.PlacementRule) error {
	pr.create = conf.Create
	pr.filter = newFilter(conf.Filter)
	var err = error(nil)
	if conf.Parent != nil {
		pr.parent, err = newRule(*conf.Parent)
	}
	return err
}

func (pr *providedRule) placeApplication(app *objects.Application, queueFn func(string) *objects.Queue) (string, error) {
	// since this is the provided rule we must have a queue in the info already
	queueName := app.GetQueuePath()
	if queueName == "" {
		return "", nil
	}
	// before anything run the filter
	if !pr.filter.allowUser(app.GetUser()) {
		log.Logger().Debug("Provided rule filtered",
			zap.String("application", app.ApplicationID),
			zap.Any("user", app.GetUser()))
		return "", nil
	}
	var parentName string
	var err error
	// if we have a fully qualified queue passed in do not run the parent rule
	if !strings.HasPrefix(queueName, configs.RootQueue+configs.DOT) {
		// run the parent rule if set
		if pr.parent != nil {
			parentName, err = pr.parent.placeApplication(app, queueFn)
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
		// Make it a fully qualified queue
		queueName = parentName + configs.DOT + replaceDot(queueName)
	}
	log.Logger().Debug("Provided rule intermediate result",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	// get the queue object
	queue := queueFn(queueName)
	// if we cannot create the queue must exist
	if !pr.create && queue == nil {
		return "", nil
	}
	log.Logger().Info("Provided rule application placed",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	return queueName, nil
}
