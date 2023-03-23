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

func newRootQueueTracker() *QueueTracker {
	return newQueueTracker(configs.RootQueue)
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

func (qt *QueueTracker) increaseTrackedResource(queuePath string, applicationID string, hasApplicationID, skipRoot bool, usage *resources.Resource) error {
	log.Logger().Debug("Increasing resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	if queuePath == "" || (hasApplicationID && applicationID == "") || usage == nil {
		return fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s",
			queuePath, applicationID, usage.String())
	}

	if qt.canUpdate(skipRoot) {
		qt.resourceUsage.AddTo(usage)
		if hasApplicationID {
			qt.runningApplications[applicationID] = true
		}
	}

	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if qt.childQueueTrackers[immediateChildQueueName] == nil {
			qt.childQueueTrackers[immediateChildQueueName] = newQueueTracker(immediateChildQueueName)
		}
		err := qt.childQueueTrackers[immediateChildQueueName].increaseTrackedResource(childQueuePath, applicationID, hasApplicationID, skipRoot, usage)
		if err != nil {
			return err
		}
	}
	return nil
}

func (qt *QueueTracker) decreaseTrackedResource(queuePath string, applicationID string, skipRoot bool, usage *resources.Resource, removeApp bool) (bool, error) {
	log.Logger().Debug("Decreasing resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	if queuePath == "" || usage == nil {
		return false, fmt.Errorf("mandatory parameters are missing. queuepath: %s, application id: %s, resource usage: %s",
			queuePath, applicationID, usage.String())
	}
	if qt.canUpdate(skipRoot) {
		qt.resourceUsage.SubFrom(usage)
		if removeApp {
			delete(qt.runningApplications, applicationID)
		}
	}

	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			removeQT, err := qt.childQueueTrackers[immediateChildQueueName].decreaseTrackedResource(childQueuePath, applicationID, skipRoot, usage, removeApp)
			if err != nil {
				return false, err
			}
			if removeQT {
				delete(qt.childQueueTrackers, immediateChildQueueName)
			}
		} else {
			log.Logger().Error("Child queueTracker tracker must be available in child queues map",
				zap.String("child queueTracker name", immediateChildQueueName))
			return false, fmt.Errorf("child queueTracker tracker for %s is missing in child queues map", immediateChildQueueName)
		}
	}

	// Determine if the queue tracker should be removed
	removeQT := len(qt.childQueueTrackers) == 0 && len(qt.runningApplications) == 0 && resources.IsZero(qt.resourceUsage)
	return removeQT, nil
}

func (qt *QueueTracker) addApplicationIDs(appIDs []string, queuePath string, skipRoot bool) {
	if qt.canUpdate(skipRoot) {
		for _, appID := range appIDs {
			qt.runningApplications[appID] = true
		}
	}

	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			qt.childQueueTrackers[immediateChildQueueName].addApplicationIDs(appIDs, childQueuePath, skipRoot)
		}
	}
}

func (qt *QueueTracker) removeApplicationIDs(appIDs []string, queuePath string, skipRoot bool) {
	if qt.canUpdate(skipRoot) {
		for _, appID := range appIDs {
			delete(qt.runningApplications, appID)
		}
	}

	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			qt.childQueueTrackers[immediateChildQueueName].removeApplicationIDs(appIDs, childQueuePath, skipRoot)
		}
	}
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
	for _, cqt := range qt.childQueueTrackers {
		childUsage := cqt.getResourceUsageDAOInfo(fullQueuePath)
		usage.Children = append(usage.Children, childUsage)
	}
	return usage
}

func getChildQueuePath(queuePath string) (string, string) {
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

	return childQueuePath, immediateChildQueueName
}

func (qt *QueueTracker) getRunningApplicationsForQueue(queuePath string) []string {
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			return qt.childQueueTrackers[immediateChildQueueName].getRunningApplicationsForQueue(childQueuePath)
		}
		return nil
	}

	apps := make([]string, 0)
	for appID := range qt.runningApplications {
		apps = append(apps, appID)
	}

	return apps
}

func (qt *QueueTracker) getResourceUsageForQueue(queuePath string) (*resources.Resource, bool) {
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != "" {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			return qt.childQueueTrackers[immediateChildQueueName].getResourceUsageForQueue(childQueuePath)
		}
		return nil, false
	}

	return qt.resourceUsage.Clone(), true
}

func (qt *QueueTracker) canUpdate(skipRoot bool) bool {
	return (qt.queueName == configs.RootQueue && !skipRoot) || qt.queueName != configs.RootQueue
}
