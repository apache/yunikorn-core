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

type mappingRule struct {
	basicRule
	tagName   string
	values    []string
	queueName string
	qualified bool
}

func (mr *mappingRule) getName() string {
	return "mapping"
}

func (mr *mappingRule) initialise(conf configs.PlacementRule) error {
	input := normalise(conf.Value)
	if input == "" {
		return fmt.Errorf("a mapping queue rule must have a mapping set")
	}
	// parse the input and assert it is in the desired format
	parts := strings.Split(input, ":")
	if len(parts) != 3 {
		return fmt.Errorf("a mapping queue rule's mapping must have three parts separated by a \":\" character")
	}
	mr.tagName = parts[0]
	if mr.tagName == "" {
		return fmt.Errorf("the mapping queue rules's tag part should be nonempty")
	}
	mr.values = strings.Split(parts[1], ",")
	if len(mr.values) == 0 {
		return fmt.Errorf("the mapping queue rules's from part should be nonempty")
	}
	mr.queueName = parts[2]
	if mr.queueName == "" {
		return fmt.Errorf("the mapping queue rules's queue part should be nonempty")
	}
	mr.qualified = strings.HasPrefix(mr.queueName, configs.RootQueue)
	// set the generic fields
	mr.create = conf.Create
	mr.filter = newFilter(conf.Filter)
	var err = error(nil)
	if conf.Parent != nil {
		mr.parent, err = newRule(*conf.Parent)
	}
	return err
}

func (mr *mappingRule) placeApplication(app *cache.ApplicationInfo, info *cache.PartitionInfo) (string, error) {
	if !mr.filter.allowUser(app.GetUser()) {
		log.Logger().Debug("Tag rule filtered",
			zap.String("application", app.ApplicationID),
			zap.Any("user", app.GetUser()),
			zap.String("tagName", mr.tagName))
		return "", nil
	}
	// check if tag exists
	tagValue := app.GetTag(mr.tagName)
	if tagValue == "" {
		return "", nil
	}
	// check if rule matches on tag
	match := false
	for _, value := range mr.values {
		if tagValue == value {
			match = true
		}
	}
	if !match {
		return "", nil
	}
	queueName := mr.queueName
	// if the queue is already fully qualified skip the parent check
	if !mr.qualified {
		parentName, err := mr.executeParentPlacement(app, info)
		if parentName == "" {
			return parentName, err
		}
		queueName = parentName + cache.DOT + mr.queueName
	}
	log.Logger().Debug("Mapping rule intermediate result",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	// get the queue object
	queue := info.GetQueue(queueName)
	// if we cannot create the queue it must exist, rule does not match otherwise
	if !mr.create && queue == nil {
		return "", nil
	}
	log.Logger().Info("Mapping rule application placed",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	return queueName, nil
}
