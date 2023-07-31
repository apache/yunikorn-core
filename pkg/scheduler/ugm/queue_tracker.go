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
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type QueueTracker struct {
	queueName           string
	queuePath           string
	resourceUsage       *resources.Resource
	runningApplications map[string]*resources.Resource
	maxResources        *resources.Resource
	maxRunningApps      uint64
	childQueueTrackers  map[string]*QueueTracker
}

func newRootQueueTracker() *QueueTracker {
	qt := newQueueTracker(common.Empty, configs.RootQueue)
	return qt
}

func newQueueTracker(queuePath string, queueName string) *QueueTracker {
	qp := queueName
	if queuePath != common.Empty {
		qp = queuePath + "." + queueName
	}
	queueTracker := &QueueTracker{
		queueName:           queueName,
		queuePath:           qp,
		resourceUsage:       resources.NewResource(),
		runningApplications: make(map[string]*resources.Resource),
		maxResources:        resources.NewResource(),
		maxRunningApps:      0,
		childQueueTrackers:  make(map[string]*QueueTracker),
	}
	log.Log(log.SchedUGM).Debug("Created queue tracker object for queue",
		zap.String("queue", queueName))
	return queueTracker
}

type trackingType int

const (
	none trackingType = iota
	user
	group
)

func (qt *QueueTracker) increaseTrackedResource(queuePath string, applicationID string, trackType trackingType, usage *resources.Resource) bool {
	log.Log(log.SchedUGM).Debug("Increasing resource usage",
		zap.Int("tracking type", int(trackType)),
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage))
	finalResourceUsage := qt.resourceUsage.Clone()
	finalResourceUsage.AddTo(usage)
	wildCardQuotaExceeded := false
	_, existingApp := qt.runningApplications[applicationID]

	// apply user/group specific limit settings set if configured, otherwise use wild card limit settings
	if qt.maxRunningApps != 0 && !resources.Equals(resources.NewResource(), qt.maxResources) {
		log.Log(log.SchedUGM).Debug("applying enforcement checks using limit settings of specific user/group",
			zap.Int("tracking type", int(trackType)),
			zap.String("queue path", queuePath),
			zap.Bool("existing app", existingApp),
			zap.Uint64("max running apps", qt.maxRunningApps),
			zap.Stringer("max resources", qt.maxResources))
		if (!existingApp && len(qt.runningApplications)+1 > int(qt.maxRunningApps)) ||
			resources.StrictlyGreaterThan(finalResourceUsage, qt.maxResources) {
			log.Log(log.SchedUGM).Warn("Unable to increase resource usage as allowing new application to run would exceed either configured max applications or max resources limit of specific user/group",
				zap.Int("tracking type", int(trackType)),
				zap.String("queue path", queuePath),
				zap.Bool("existing app", existingApp),
				zap.Int("current running applications", len(qt.runningApplications)),
				zap.Uint64("max running applications", qt.maxRunningApps),
				zap.Stringer("current resource usage", qt.resourceUsage),
				zap.Stringer("max resource usage", qt.maxResources))
			return false
		}
	}

	// Try wild card settings
	if qt.maxRunningApps == 0 && resources.Equals(resources.NewResource(), qt.maxResources) {
		// Is there any wild card settings? Do we need to apply enforcement checks using wild card limit settings?
		var config *LimitConfig
		if trackType == user {
			config = m.getUserWildCardLimitsConfig(qt.queuePath)
		} else if trackType == group {
			config = m.getGroupWildCardLimitsConfig(qt.queuePath)
		}
		if config != nil {
			log.Log(log.SchedUGM).Debug("applying enforcement checks using limit settings of wild card user/group",
				zap.Int("tracking type", int(trackType)),
				zap.String("queue path", queuePath),
				zap.Bool("existing app", existingApp),
				zap.Uint64("wild card max running apps", config.maxApplications),
				zap.Stringer("wild card max resources", config.maxResources),
				zap.Bool("wild card quota exceeded", wildCardQuotaExceeded))
			wildCardQuotaExceeded = (config.maxApplications != 0 && !existingApp && len(qt.runningApplications)+1 > int(config.maxApplications)) ||
				(!resources.Equals(resources.NewResource(), config.maxResources) && resources.StrictlyGreaterThan(finalResourceUsage, config.maxResources))
			if wildCardQuotaExceeded {
				log.Log(log.SchedUGM).Warn("Unable to increase resource usage as allowing new application to run would exceed either configured max applications or max resources limit of wild card user/group",
					zap.Int("tracking type", int(trackType)),
					zap.String("queue path", queuePath),
					zap.Bool("existing app", existingApp),
					zap.Int("current running applications", len(qt.runningApplications)),
					zap.Uint64("max running applications", config.maxApplications),
					zap.Stringer("current resource usage", qt.resourceUsage),
					zap.Stringer("max resource usage", config.maxResources))
				return false
			}
		}
	}

	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != common.Empty {
		if qt.childQueueTrackers[immediateChildQueueName] == nil {
			qt.childQueueTrackers[immediateChildQueueName] = newQueueTracker(qt.queuePath, immediateChildQueueName)
		}
		allowed := qt.childQueueTrackers[immediateChildQueueName].increaseTrackedResource(childQueuePath, applicationID, trackType, usage)
		if !allowed {
			return allowed
		}
	}

	qt.resourceUsage.AddTo(usage)
	if !existingApp {
		qt.runningApplications[applicationID] = resources.NewResource()
	}
	qt.runningApplications[applicationID].AddTo(usage)

	log.Log(log.SchedUGM).Debug("Successfully increased resource usage",
		zap.Int("tracking type", int(trackType)),
		zap.String("queue path", queuePath),
		zap.String("qt queue path", qt.queuePath),
		zap.String("application", applicationID),
		zap.Bool("existing app", existingApp),
		zap.Stringer("resource", usage),
		zap.Uint64("max running applications", qt.maxRunningApps),
		zap.Stringer("max resource usage", qt.maxResources),
		zap.Stringer("total resource after increasing", qt.resourceUsage),
		zap.Int("total applications after increasing", len(qt.runningApplications)))
	return true
}

func (qt *QueueTracker) decreaseTrackedResource(queuePath string, applicationID string, usage *resources.Resource, removeApp bool) (bool, bool) {
	log.Log(log.SchedUGM).Debug("Decreasing resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != common.Empty {
		if qt.childQueueTrackers[immediateChildQueueName] == nil {
			log.Log(log.SchedUGM).Error("Child queueTracker tracker must be available in child queues map",
				zap.String("child queueTracker name", immediateChildQueueName))
			return false, false
		}
		removeQT, decreased := qt.childQueueTrackers[immediateChildQueueName].decreaseTrackedResource(childQueuePath, applicationID, usage, removeApp)
		if !decreased {
			return false, decreased
		}
		if removeQT {
			log.Log(log.SchedUGM).Debug("Removed queue tracker linkage from its parent",
				zap.String("queue path ", queuePath),
				zap.String("removed queue name", immediateChildQueueName),
				zap.String("parent queue", qt.queueName))
			delete(qt.childQueueTrackers, immediateChildQueueName)
		}
	}

	qt.resourceUsage.SubFrom(usage)
	resUsage := qt.runningApplications[applicationID].Clone()
	resUsage.SubFrom(usage)
	qt.runningApplications[applicationID] = resUsage
	if removeApp {
		log.Log(log.SchedUGM).Debug("Removed application from running applications",
			zap.String("application", applicationID),
			zap.String("queue path", queuePath),
			zap.String("queue name", qt.queueName))
		delete(qt.runningApplications, applicationID)
	}
	log.Log(log.SchedUGM).Debug("Successfully decreased resource usage",
		zap.String("queue path", queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Stringer("total resource after decreasing", qt.resourceUsage),
		zap.Int("total applications after decreasing", len(qt.runningApplications)))

	// Determine if the queue tracker should be removed
	removeQT := len(qt.childQueueTrackers) == 0 && len(qt.runningApplications) == 0 && resources.IsZero(qt.resourceUsage) &&
		qt.maxRunningApps == 0 && resources.Equals(resources.NewResource(), qt.maxResources)
	log.Log(log.SchedUGM).Debug("Remove queue tracker",
		zap.String("queue path ", queuePath),
		zap.Bool("remove QT", removeQT))
	return removeQT, true
}

func (qt *QueueTracker) getChildQueueTracker(queuePath string) *QueueTracker {
	var childQueuePath, immediateChildQueueName string
	childQueuePath, immediateChildQueueName = getChildQueuePath(queuePath)
	childQueueTracker := qt
	if childQueuePath != common.Empty {
		for childQueuePath != common.Empty {
			if childQueueTracker != nil {
				if len(childQueueTracker.childQueueTrackers) == 0 || childQueueTracker.childQueueTrackers[immediateChildQueueName] == nil {
					newChildQt := newQueueTracker(qt.queuePath, immediateChildQueueName)
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

func (qt *QueueTracker) setLimit(queuePath string, maxResource *resources.Resource, maxApps uint64) {
	log.Log(log.SchedUGM).Debug("Setting limits",
		zap.String("queue path", queuePath),
		zap.Uint64("max applications", maxApps),
		zap.Stringer("max resources", maxResource))
	childQueueTracker := qt.getChildQueueTracker(queuePath)
	childQueueTracker.maxRunningApps = maxApps
	childQueueTracker.maxResources = maxResource
}

func (qt *QueueTracker) headroom(queuePath string) *resources.Resource {
	log.Log(log.SchedUGM).Debug("Calculating headroom",
		zap.String("queue path", queuePath))
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if childQueuePath != common.Empty {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			headroom := qt.childQueueTrackers[immediateChildQueueName].headroom(childQueuePath)
			if headroom != nil {
				return resources.ComponentWiseMinPermissive(headroom, qt.maxResources)
			}
		} else {
			log.Log(log.SchedUGM).Error("Child queueTracker tracker must be available in child queues map",
				zap.String("child queueTracker name", immediateChildQueueName))
			return nil
		}
	}

	if !resources.Equals(resources.NewResource(), qt.maxResources) {
		headroom := qt.maxResources.Clone()
		headroom.SubOnlyExisting(qt.resourceUsage)
		log.Log(log.SchedUGM).Debug("Calculated headroom",
			zap.String("queue path", queuePath),
			zap.String("queue", qt.queueName),
			zap.Stringer("max resource", qt.maxResources),
			zap.Stringer("headroom", headroom))
		return headroom
	}
	return nil
}

func (qt *QueueTracker) getResourceUsageDAOInfo(parentQueuePath string) *dao.ResourceUsageDAOInfo {
	if qt == nil {
		return &dao.ResourceUsageDAOInfo{}
	}
	fullQueuePath := parentQueuePath + "." + qt.queueName
	if parentQueuePath == common.Empty {
		fullQueuePath = qt.queueName
	}
	usage := &dao.ResourceUsageDAOInfo{
		QueuePath:     fullQueuePath,
		ResourceUsage: qt.resourceUsage.Clone(),
	}
	for app := range qt.runningApplications {
		usage.RunningApplications = append(usage.RunningApplications, app)
	}
	usage.MaxResources = qt.maxResources
	usage.MaxApplications = qt.maxRunningApps
	for _, cqt := range qt.childQueueTrackers {
		childUsage := cqt.getResourceUsageDAOInfo(fullQueuePath)
		usage.Children = append(usage.Children, childUsage)
	}
	return usage
}

// IsQueuePathTrackedCompletely Traverse queue path upto the end queue through its linkage
// to confirm entire queuePath has been tracked completely or not
func (qt *QueueTracker) IsQueuePathTrackedCompletely(queuePath string) bool {
	if queuePath == configs.RootQueue || queuePath == qt.queueName {
		return true
	}
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if immediateChildQueueName != common.Empty {
		if childUt, ok := qt.childQueueTrackers[immediateChildQueueName]; ok {
			return childUt.IsQueuePathTrackedCompletely(childQueuePath)
		}
	}
	return false
}

// IsUnlinkRequired Traverse queue path upto the leaf queue and decide whether
// linkage needs to be removed or not based on the running applications.
// If there are any running applications in end leaf queue, we should remove the linkage between
// the leaf and its parent queue using UnlinkQT method. Otherwise, we should leave as it is.
func (qt *QueueTracker) IsUnlinkRequired(queuePath string) bool {
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)
	if immediateChildQueueName != common.Empty {
		if childUt, ok := qt.childQueueTrackers[immediateChildQueueName]; ok {
			return childUt.IsUnlinkRequired(childQueuePath)
		}
	}
	if queuePath == configs.RootQueue || queuePath == qt.queueName {
		if len(qt.runningApplications) == 0 {
			log.Log(log.SchedUGM).Debug("Is Unlink Required?",
				zap.String("queue path", queuePath),
				zap.Int("no. of applications", len(qt.runningApplications)))
			return true
		}
	}
	return false
}

// UnlinkQT Traverse queue path upto the end queue. If end queue has any more child queue trackers,
// then goes upto each child queue and removes the linkage with its immediate parent
func (qt *QueueTracker) UnlinkQT(queuePath string) bool {
	log.Log(log.SchedUGM).Debug("Unlinking current queue tracker from its parent",
		zap.String("current queue ", qt.queueName),
		zap.String("queue path", queuePath),
		zap.Int("no. of child queue trackers", len(qt.childQueueTrackers)))
	childQueuePath, immediateChildQueueName := getChildQueuePath(queuePath)

	if childQueuePath == common.Empty && len(qt.childQueueTrackers) > 0 {
		for qName := range qt.childQueueTrackers {
			qt.UnlinkQT(qt.queueName + configs.DOT + qName)
		}
	}

	if childQueuePath != common.Empty {
		if qt.childQueueTrackers[immediateChildQueueName] != nil {
			unlink := qt.childQueueTrackers[immediateChildQueueName].UnlinkQT(childQueuePath)
			if unlink {
				delete(qt.childQueueTrackers, immediateChildQueueName)
			}
		}
	}
	if len(qt.runningApplications) == 0 && len(qt.childQueueTrackers) == 0 {
		return true
	}
	return false
}

// decreaseTrackedResourceUsage Collect the app resource usage at the leaf queue level and decrease resource usage
// using the leaf queue's queue path
func (qt *QueueTracker) decreaseTrackedResourceUsage(appID string) (bool, bool) {
	usage, app := qt.runningApplications[appID]
	queuePath := qt.queuePath
	childQueueTrackers := qt.childQueueTrackers

	// start traversing from the root and reach upto leaf queue for the given app
	for app {
		if len(childQueueTrackers) == 0 {
			break
		}
		for _, childQT := range childQueueTrackers {
			queuePath = childQT.queuePath
			usage, app = childQT.runningApplications[appID]
			childQueueTrackers = childQT.childQueueTrackers
			if app {
				break
			}
		}
	}

	// once identify the leaf queue, using its queue path decrement resource usage like regular way of decrementing resource usage
	// so that it decrements the resource usage for every queue in the queue path
	return qt.decreaseTrackedResource(queuePath, appID, usage, true)
}
