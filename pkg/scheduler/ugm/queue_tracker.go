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
	maxResourceUsage    *resources.Resource
	maxRunningApps      uint64
	childQueueTrackers  map[string]*QueueTracker
}

func newRootQueueTracker() *QueueTracker {
	return newQueueTracker(configs.RootQueue)
}

func newQueueTracker(queueName string) *QueueTracker {
	log.Log(log.SchedUGM).Debug("Creating queue tracker object for queue",
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
	log.Log(log.SchedUGM).Debug("Increasing resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	if queuePath == "" || applicationID == "" || usage == nil {
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s",
			queuePath, applicationID, usage.String())
	}
	qt.resourceUsage.AddTo(usage)
	qt.runningApplications[applicationID] = true

	log.Log(log.SchedUGM).Debug("Successfully increased resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Stringer("total resource after increasing", qt.resourceUsage),
		zap.Int("total applications after increasing", len(qt.runningApplications)))

	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
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

func (qt *QueueTracker) decreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, removeApp bool) (bool, error) {
	log.Log(log.SchedUGM).Debug("Decreasing resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	if queuePath == "" || usage == nil {
		return false, fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s",
			queuePath, applicationID, usage.String())
	}
	qt.resourceUsage.SubFrom(usage)
	if removeApp {
		delete(qt.runningApplications, applicationID)
	}
	log.Log(log.SchedUGM).Debug("Successfully decreased resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Stringer("total resource after decreasing", qt.resourceUsage),
		zap.Int("total applications after decreasing", len(qt.runningApplications)))

	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			removeQT, err := qt.childQueueTrackers[immediateChildQueueName].decreaseTrackedResource(childQueuePath, applicationID, usage, removeApp)
			if err != nil {
				return false, err
			}
			if removeQT {
				delete(qt.childQueueTrackers, immediateChildQueueName)
			}
		} else {
			log.Log(log.SchedUGM).Error("Child queueTracker tracker must be available in child queues map",
				zap.String("child queueTracker name", immediateChildQueueName))
			return false, fmt.Errorf("child queueTracker tracker for %s is missing in child queues map", immediateChildQueueName)
		}
	}

	// Determine if the queue tracker should be removed
	removeQT := len(qt.childQueueTrackers) == 0 && len(qt.runningApplications) == 0 && resources.IsZero(qt.resourceUsage)
	return removeQT, nil
}

func (qt *QueueTracker) getChildQueueTracker(queuePath string) *QueueTracker {
	var childQueuePath, immediateChildQueueName string
	childQueuePath, immediateChildQueueName = getChildQueuePath(queuePath)
	childQueueTracker := qt
	if childQueuePath != "" {
		for childQueuePath != "" {
			if childQueueTracker != nil {
				if len(childQueueTracker.childQueueTrackers) == 0 || childQueueTracker.childQueueTrackers[immediateChildQueueName] == nil {
					newChildQt := newQueueTracker(immediateChildQueueName)
					childQueueTracker.childQueueTrackers[immediateChildQueueName] = newChildQt
					childQueueTracker = newChildQt
				} else {
					childQueueTracker = childQueueTracker.childQueueTrackers[immediateChildQueueName]
				}
			}
			childQueuePath, immediateChildQueueName = getChildQueuePath(childQueuePath)
		}
	}
	return childQueueTracker
}

func (qt *QueueTracker) setMaxApplications(count uint64, queuePath string) error {
	log.Log(log.SchedUGM).Debug("Setting max applications",
		zap.String("queue path", queuePath),
		zap.Uint64("max applications", count))
	childQueueTracker := qt.getChildQueueTracker(queuePath)
	if childQueueTracker.maxRunningApps != 0 && count != 0 && len(childQueueTracker.runningApplications) > int(count) {
		log.Log(log.SchedUGM).Warn("Current running applications is greater than config max applications",
			zap.String("queue path", queuePath),
			zap.Uint64("current max applications", childQueueTracker.maxRunningApps),
			zap.Int("total running applications", len(childQueueTracker.runningApplications)),
			zap.Uint64("config max applications", count))
		return fmt.Errorf("current running applications is greater than config max applications for %s", queuePath)
	} else {
		childQueueTracker.maxRunningApps = count
	}
	return nil
}

func (qt *QueueTracker) setMaxResources(resource *resources.Resource, queuePath string) error {
	log.Log(log.SchedUGM).Debug("Setting max resources",
		zap.String("queue path", queuePath),
		zap.String("max resources", resource.String()))
	childQueueTracker := qt.getChildQueueTracker(queuePath)
	if (!resources.Equals(childQueueTracker.maxResourceUsage, resources.NewResource()) && !resources.Equals(resource, resources.NewResource())) && resources.StrictlyGreaterThan(childQueueTracker.resourceUsage, resource) {
		log.Log(log.SchedUGM).Warn("Current resource usage is greater than config max resource",
			zap.String("queue path", queuePath),
			zap.String("current max resource usage", childQueueTracker.maxResourceUsage.String()),
			zap.String("total resource usage", childQueueTracker.resourceUsage.String()),
			zap.String("config max resources", resource.String()))
		return fmt.Errorf("current resource usage is greater than config max resource for %s", queuePath)
	} else {
		childQueueTracker.maxResourceUsage = resource
	}
	return nil
}

func (qt *QueueTracker) getResourceUsageDAOInfo(parentQueuePath string) *dao.ResourceUsageDAOInfo {
	if qt == nil {
		return &dao.ResourceUsageDAOInfo{}
	}
	fullQueuePath := parentQueuePath + "." + qt.queueName
	if parentQueuePath == "" {
		fullQueuePath = qt.queueName
	}
	usage := &dao.ResourceUsageDAOInfo{
		QueuePath:     fullQueuePath,
		ResourceUsage: qt.resourceUsage.Clone(),
	}
	for app := range qt.runningApplications {
		usage.RunningApplications = append(usage.RunningApplications, app)
	}
	usage.MaxResources = qt.maxResourceUsage
	usage.MaxApplications = qt.maxRunningApps
	for _, cqt := range qt.childQueueTrackers {
		childUsage := cqt.getResourceUsageDAOInfo(fullQueuePath)
		usage.Children = append(usage.Children, childUsage)
	}
	return usage
}
