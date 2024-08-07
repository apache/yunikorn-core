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

// A rule to place an application based on the a tag on the application.
// The tag will be part of the application that is submitted. An application can have 0 or more tags.
// If the tag is present the value will be used as the queue name.
// NOTE: tags are normalised and only use lower case (not case sensitive)
type tagRule struct {
	basicRule
	tagName string
}

func (tr *tagRule) getName() string {
	return types.Tag
}

func (tr *tagRule) ruleDAO() *dao.RuleDAO {
	var pDAO *dao.RuleDAO
	if tr.parent != nil {
		pDAO = tr.parent.ruleDAO()
	}
	return &dao.RuleDAO{
		Name: tr.getName(),
		Parameters: map[string]string{
			"tagName": tr.tagName,
			"create":  strconv.FormatBool(tr.create),
		},
		ParentRule: pDAO,
		Filter:     tr.filter.filterDAO(),
	}
}

func (tr *tagRule) initialise(conf configs.PlacementRule) error {
	tr.tagName = normalise(conf.Value)
	if tr.tagName == "" {
		return fmt.Errorf("a tag queue rule must have a tag name set")
	}
	tr.create = conf.Create
	tr.filter = newFilter(conf.Filter)
	var err = error(nil)
	if conf.Parent != nil {
		tr.parent, err = newRule(*conf.Parent)
	}
	return err
}

func (tr *tagRule) placeApplication(app *objects.Application, queueFn func(string) *objects.Queue) (string, error) {
	// if the tag is not present we can skipp all other processing
	tagVal := app.GetTag(tr.tagName)
	if tagVal == "" {
		return "", nil
	}
	// before anything run the filter
	if !tr.filter.allowUser(app.GetUser()) {
		log.Log(log.SchedApplication).Debug("Tag rule filtered",
			zap.String("application", app.ApplicationID),
			zap.Any("user", app.GetUser()),
			zap.String("tagName", tr.tagName))
		return "", nil
	}
	var parentName string
	var err error
	queueName := tagVal
	// fully qualified queue, do not run the parent rule
	if strings.HasPrefix(queueName, configs.RootQueue+configs.DOT) {
		parts := strings.Split(queueName, configs.DOT)
		for _, part := range parts {
			if err = configs.IsQueueNameValid(part); err != nil {
				return "", err
			}
		}
	} else {
		// not fully qualified queue
		childQueueName := replaceDot(tagVal)
		if err = configs.IsQueueNameValid(childQueueName); err != nil {
			return "", err
		}
		// run the parent rule if set
		if tr.parent != nil {
			parentName, err = tr.parent.placeApplication(app, queueFn)
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
		queueName = parentName + configs.DOT + childQueueName
	}
	// Log the result before we check the create flag
	log.Log(log.SchedApplication).Info("Tag rule intermediate result",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	// get the queue object
	queue := queueFn(queueName)
	// if we cannot create the queue it must exist, rule does not match otherwise
	if !tr.create && queue == nil {
		return "", nil
	}
	log.Log(log.SchedApplication).Info("Tag rule application placed",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	return queueName, nil
}
