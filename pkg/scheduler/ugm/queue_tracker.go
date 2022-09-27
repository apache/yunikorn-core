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

package ugm

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
)

type QueueTracker struct {
	queueName           string
	resourceUsage       *resources.Resource
	runningApplications map[string]bool
	childQueueTrackers  map[string]*QueueTracker
}

func NewQueueTracker(queueName string) *QueueTracker {
	queueTracker := &QueueTracker{
		queueName:           queueName,
		resourceUsage:       resources.NewResource(),
		runningApplications: make(map[string]bool),
		childQueueTrackers:  make(map[string]*QueueTracker),
	}
	return queueTracker
}

func (qt *QueueTracker) increaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource) error {
	if queuePath == "" || applicationID == "" || usage == nil {
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s",
			queuePath, applicationID, usage.String())
	}
	qt.resourceUsage.AddTo(usage)
	qt.runningApplications[applicationID] = true
	idx := strings.Index(queuePath, configs.DOT)
	childQueuePath := ""
	if idx != -1 {
		childQueuePath = queuePath[idx+1:]
	}

	childIndex := strings.Index(childQueuePath, configs.DOT)
	immediateChildQueueName := childQueuePath
	if childIndex != -1 {
		immediateChildQueueName = childQueuePath[:childIndex]
	}
	if childQueuePath != "" {
		childQueueTracker := qt.childQueueTrackers[immediateChildQueueName]
		if childQueueTracker == nil {
			childQueueTracker = NewQueueTracker(immediateChildQueueName)
			qt.childQueueTrackers[immediateChildQueueName] = childQueueTracker
		}
		err := childQueueTracker.increaseTrackedResource(childQueuePath, applicationID, usage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qt *QueueTracker) decreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, removeApp bool) error {
	if queuePath == "" || usage == nil {
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s",
			queuePath, applicationID, usage.String())
	}
	qt.resourceUsage.SubFrom(usage)
	if removeApp {
		delete(qt.runningApplications, applicationID)
	}
	idx := strings.Index(queuePath, configs.DOT)
	childQueuePath := ""
	if idx != -1 {
		childQueuePath = queuePath[idx+1:]
	}
	childIndex := strings.Index(childQueuePath, configs.DOT)
	immediateChildQueueName := childQueuePath
	if childIndex != -1 {
		immediateChildQueueName = childQueuePath[:childIndex]
	}
	if childQueuePath != "" {
		childQueueTracker := qt.childQueueTrackers[immediateChildQueueName]
		if childQueueTracker != nil {
			qt.childQueueTrackers[immediateChildQueueName] = childQueueTracker
			err := childQueueTracker.decreaseTrackedResource(childQueuePath, applicationID, usage, removeApp)
			if err != nil {
				return err
			}
		} else {
			log.Logger().Error("Child queueTracker tracker must be available in child queues map",
				zap.String("child queueTracker name", immediateChildQueueName))
			return fmt.Errorf("child queueTracker tracker for %s is missing in child queues map", immediateChildQueueName)
		}
	}
	return nil
}
