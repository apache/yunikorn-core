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

// A rule to place an application based on the user name of the submitting user.
type userRule struct {
	basicRule
}

func (ur *userRule) getName() string {
	return types.User
}

func (ur *userRule) initialise(conf configs.PlacementRule) error {
	ur.create = conf.Create
	ur.filter = newFilter(conf.Filter)
	var err = error(nil)
	if conf.Parent != nil {
		ur.parent, err = newRule(*conf.Parent)
	}
	return err
}

func (ur *userRule) placeApplication(app *objects.Application, queueFn func(string) *objects.Queue) (string, error) {
	// before anything run the filter
	userName := app.GetUser().User
	if !ur.filter.allowUser(app.GetUser()) {
		log.Logger().Debug("User rule filtered",
			zap.String("application", app.ApplicationID),
			zap.Any("user", app.GetUser()))
		return "", nil
	}
	var parentName string
	var err error
	// run the parent rule if set
	if ur.parent != nil {
		parentName, err = ur.parent.placeApplication(app, queueFn)
		// failed parent rule, fail this rule
		if err != nil {
			return "", err
		}
		// rule did not match: this could be filter or create flag related
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
	queueName := parentName + configs.DOT + replaceDot(userName)
	log.Logger().Debug("User rule intermediate result",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	// get the queue object
	queue := queueFn(queueName)
	// if we cannot create the queue it must exist, rule does not match otherwise
	if !ur.create && queue == nil {
		return "", nil
	}
	log.Logger().Info("User rule application placed",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	return queueName, nil
}
