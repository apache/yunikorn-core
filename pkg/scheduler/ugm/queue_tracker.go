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
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type QueueTracker struct {
	queueName           string
	resourceUsage       *resources.Resource
	runningApplications map[string]bool
	childQueueTrackers  map[string]*QueueTracker
}

func NewQueueTracker() *QueueTracker {
	return newQueueTracker("root")
}

func newQueueTracker(queueName string) *QueueTracker {
	log.Logger().Debug("Creating queue tracker object for queue",
		zap.String("queue", queueName))
	queueTracker := &QueueTracker{
		queueName:           queueName,
		resourceUsage:       resources.NewResource(),
		runningApplications: make(map[string]bool),
		childQueueTrackers:  make(map[string]*QueueTracker),
	}
	return queueTracker
}

func (qt *QueueTracker) increaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource) error {
	log.Logger().Debug("Increasing resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.String("resource", usage.String()))
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
		if qt.childQueueTrackers[immediateChildQueueName] == nil {
			qt.childQueueTrackers[immediateChildQueueName] = newQueueTracker(immediateChildQueueName)
		}
		err := qt.childQueueTrackers[immediateChildQueueName].increaseTrackedResource(childQueuePath, applicationID, usage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qt *QueueTracker) decreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, removeApp bool) error {
	log.Logger().Debug("Decreasing resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.String("resource", usage.String()),
		zap.Bool("removeApp", removeApp))
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
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			err := qt.childQueueTrackers[immediateChildQueueName].decreaseTrackedResource(childQueuePath, applicationID, usage, removeApp)
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

func (qt *QueueTracker) getResourceUsageDAOInfo(parentQueuePath string, queueName string, queueTracker *QueueTracker) *dao.ResourceUsageDAOInfo {
	usage := &dao.ResourceUsageDAOInfo{}
	fullQueuePath := ""
	if queueName == "root" {
		fullQueuePath = parentQueuePath
	} else {
		fullQueuePath = parentQueuePath + "." + queueTracker.queueName
	}
	usage.QueueName = fullQueuePath
	usage.ResourceUsage = queueTracker.resourceUsage
	for app, active := range queueTracker.runningApplications {
		if active {
			usage.RunningApplications = append(usage.RunningApplications, app)
		}
	}
	if len(queueTracker.childQueueTrackers) > 0 {
		for childQueueName, childQueueTracker := range queueTracker.childQueueTrackers {
			childUsage := qt.getResourceUsageDAOInfo(fullQueuePath, childQueueName, childQueueTracker)
			usage.Children = append(usage.Children, childUsage)
		}
	}
	return usage
}
