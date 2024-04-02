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

// The QueueTracker is designed to be lock free and should remain as such.
// Each QueueTracker object is always only linked to single UserTracker or GroupTracker. The responsibility of managing locks is delegated to those objects.
type QueueTracker struct {
	queueName           string
	queuePath           string
	resourceUsage       *resources.Resource
	runningApplications map[string]bool
	maxResources        *resources.Resource
	maxRunningApps      uint64
	childQueueTrackers  map[string]*QueueTracker
	useWildCard         bool
}

func newRootQueueTracker(trackType trackingType) *QueueTracker {
	qt := newQueueTracker(common.Empty, configs.RootQueue, trackType)
	return qt
}

func newQueueTracker(queuePath string, queueName string, trackType trackingType) *QueueTracker {
	fullPath := queueName
	if queuePath != common.Empty {
		fullPath = queuePath + "." + queueName
	}
	queueTracker := &QueueTracker{
		queueName:           queueName,
		queuePath:           fullPath,
		resourceUsage:       nil,
		runningApplications: make(map[string]bool),
		maxResources:        nil,
		maxRunningApps:      0,
		childQueueTrackers:  make(map[string]*QueueTracker),
	}

	// Override user/group specific limits with wild card limit settings
	if trackType == user {
		if config := m.getUserWildCardLimitsConfig(fullPath); config != nil {
			log.Log(log.SchedUGM).Debug("Use wild card limit settings as there is no limit set explicitly",
				zap.String("queue name", queueName),
				zap.String("queue path", queuePath),
				zap.Uint64("max applications", config.maxApplications),
				zap.Stringer("max resources", config.maxResources))
			queueTracker.maxResources = config.maxResources.Clone()
			queueTracker.maxRunningApps = config.maxApplications
			queueTracker.useWildCard = true
		}
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

// Note: Lock free call. The Lock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) increaseTrackedResource(hierarchy []string, applicationID string, trackType trackingType, usage *resources.Resource) {
	log.Log(log.SchedUGM).Debug("Increasing resource usage",
		zap.Int("tracking type", int(trackType)),
		zap.String("queue path", qt.queuePath),
		zap.Strings("hierarchy", hierarchy),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("use wild card", qt.useWildCard))
	// depth first: all the way to the leaf, create if not exists
	// more than 1 in the slice means we need to recurse down
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] == nil {
			qt.childQueueTrackers[childName] = newQueueTracker(qt.queuePath, childName, trackType)
		}
		qt.childQueueTrackers[childName].increaseTrackedResource(hierarchy[1:], applicationID, trackType, usage)
	}
	if qt.resourceUsage == nil {
		qt.resourceUsage = resources.NewResource()
	}
	qt.resourceUsage.AddTo(usage)
	qt.runningApplications[applicationID] = true
	log.Log(log.SchedUGM).Debug("Successfully increased resource usage",
		zap.Int("tracking type", int(trackType)),
		zap.String("queue path", qt.queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Uint64("max running applications", qt.maxRunningApps),
		zap.Stringer("max resource usage", qt.maxResources),
		zap.Bool("use wild card", qt.useWildCard),
		zap.Stringer("total resource after increasing", qt.resourceUsage),
		zap.Int("total applications after increasing", len(qt.runningApplications)))
}

// Note: Lock free call. The Lock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) decreaseTrackedResource(hierarchy []string, applicationID string, usage *resources.Resource, removeApp bool) bool {
	log.Log(log.SchedUGM).Debug("Decreasing resource usage",
		zap.String("queue path", qt.queuePath),
		zap.Strings("hierarchy", hierarchy),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Bool("removeApp", removeApp))
	// depth first: all the way to the leaf, return false if not exists
	// more than 1 in the slice means we need to recurse down
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] == nil {
			log.Log(log.SchedUGM).Error("Child queueTracker tracker must be available in child queues map",
				zap.String("child queueTracker name", childName))
			return false
		}
		removeQT := qt.childQueueTrackers[childName].decreaseTrackedResource(hierarchy[1:], applicationID, usage, removeApp)
		if removeQT {
			log.Log(log.SchedUGM).Debug("Removed queue tracker linkage from its parent",
				zap.String("queue path ", qt.queuePath),
				zap.String("removed queue name", childName),
				zap.String("parent queue name", qt.queueName))
			delete(qt.childQueueTrackers, childName)
		}
	}
	qt.resourceUsage.SubFrom(usage)
	if removeApp {
		log.Log(log.SchedUGM).Debug("Removed application from running applications",
			zap.String("application", applicationID),
			zap.String("queue path", qt.queuePath),
			zap.String("queue name", qt.queueName))
		delete(qt.runningApplications, applicationID)
	}
	log.Log(log.SchedUGM).Debug("Successfully decreased resource usage",
		zap.String("queue path", qt.queuePath),
		zap.String("application", applicationID),
		zap.Stringer("resource", usage),
		zap.Stringer("total resource after decreasing", qt.resourceUsage),
		zap.Int("total applications after decreasing", len(qt.runningApplications)))

	// Determine if the queue tracker should be removed
	removeQT := len(qt.childQueueTrackers) == 0 && len(qt.runningApplications) == 0 && resources.IsZero(qt.resourceUsage) &&
		qt.maxRunningApps == 0 && resources.IsZero(qt.maxResources)
	log.Log(log.SchedUGM).Debug("Remove queue tracker",
		zap.String("queue path ", qt.queuePath),
		zap.Bool("remove QT", removeQT))
	return removeQT
}

// Note: Lock free call. The Lock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) setLimit(hierarchy []string, maxResource *resources.Resource, maxApps uint64, useWildCard bool, trackType trackingType, doWildCardCheck bool) {
	log.Log(log.SchedUGM).Debug("Setting limits",
		zap.String("queue path", qt.queuePath),
		zap.Strings("hierarchy", hierarchy),
		zap.Uint64("max applications", maxApps),
		zap.Stringer("max resources", maxResource),
		zap.Bool("use wild card", useWildCard))
	// depth first: all the way to the leaf, create if not exists
	// more than 1 in the slice means we need to recurse down
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] == nil {
			qt.childQueueTrackers[childName] = newQueueTracker(qt.queuePath, childName, trackType)
		}
		qt.childQueueTrackers[childName].setLimit(hierarchy[1:], maxResource, maxApps, useWildCard, trackType, false)
	} else if len(hierarchy) == 1 {
		// don't override named user/group specific limits with wild card limits
		if doWildCardCheck && !qt.useWildCard {
			return
		}
		qt.maxRunningApps = maxApps
		qt.maxResources = maxResource
		qt.useWildCard = useWildCard
	}
}

// Note: Lock free call. The Lock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
// Note: headroom is not read-only, it also traverses the queue hierarchy and creates childQueueTracker if it does not exist.
func (qt *QueueTracker) headroom(hierarchy []string, trackType trackingType) *resources.Resource {
	// depth first: all the way to the leaf, create if not exists
	// more than 1 in the slice means we need to recurse down
	var headroom, childHeadroom *resources.Resource
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] == nil {
			qt.childQueueTrackers[childName] = newQueueTracker(qt.queuePath, childName, trackType)
		}
		childHeadroom = qt.childQueueTrackers[childName].headroom(hierarchy[1:], trackType)
	}

	// arrived at the leaf or on the way out: check against current max if set
	if !resources.IsZero(qt.maxResources) {
		headroom = qt.maxResources.Clone()
		headroom.SubOnlyExisting(qt.resourceUsage)
	}

	if headroom == nil {
		return childHeadroom
	}
	return resources.ComponentWiseMinPermissive(headroom, childHeadroom)
}

// Note: Lock free call. The RLock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
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
// Note: Lock free call. The RLock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) IsQueuePathTrackedCompletely(hierarchy []string) bool {
	// depth first: all the way to the leaf, ignore if not exists
	// more than 1 in the slice means we need to recurse down
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] != nil {
			return qt.childQueueTrackers[childName].IsQueuePathTrackedCompletely(hierarchy[1:])
		}
	} else if len(hierarchy) == 1 {
		// reach end of hierarchy
		if hierarchy[0] == configs.RootQueue || hierarchy[0] == qt.queueName {
			return true
		}
	}
	return false
}

// IsUnlinkRequired Traverse queue path upto the leaf queue and decide whether
// linkage needs to be removed or not based on the running applications.
// If there are any running applications in end leaf queue, we should remove the linkage between
// the leaf and its parent queue using UnlinkQT method. Otherwise, we should leave as it is.
// Note: Lock free call. The RLock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) IsUnlinkRequired(hierarchy []string) bool {
	// depth first: all the way to the leaf, ignore if not exists
	// more than 1 in the slice means we need to recurse down
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] != nil {
			return qt.childQueueTrackers[childName].IsUnlinkRequired(hierarchy[1:])
		}
	} else if len(hierarchy) == 1 {
		// reach end of hierarchy
		if hierarchy[0] == configs.RootQueue || hierarchy[0] == qt.queueName {
			if len(qt.runningApplications) == 0 {
				log.Log(log.SchedUGM).Debug("Is Unlink Required?",
					zap.String("queue path", qt.queuePath),
					zap.Int("no. of applications", len(qt.runningApplications)))
				return true
			}
		}
	}
	return false
}

// UnlinkQT Traverse queue path upto the end queue. If end queue has any more child queue trackers,
// then goes upto each child queue and removes the linkage with its immediate parent
// Note: Lock free call. The Lock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) UnlinkQT(hierarchy []string) bool {
	log.Log(log.SchedUGM).Debug("Unlinking current queue tracker from its parent",
		zap.String("current queue ", qt.queueName),
		zap.String("queue path", qt.queuePath),
		zap.Strings("hierarchy", hierarchy),
		zap.Int("no. of child queue trackers", len(qt.childQueueTrackers)))
	// depth first: all the way to the leaf, ignore if not exists
	// more than 1 in the slice means we need to recurse down
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] != nil {
			if qt.childQueueTrackers[childName].UnlinkQT(hierarchy[1:]) {
				delete(qt.childQueueTrackers, childName)
				// returning false, so that it comes out when end queue detach itself from its immediate parent.
				// i.e., once leaf detached from root.parent for root.parent.leaf queue path.
				// otherwise, detachment continues all the way upto the root, even parent from root. which is not needed.
				return false
			}
		}
	} else if len(hierarchy) <= 1 {
		// reach end of hierarchy, unlink all queues under this queue
		for childName, childQT := range qt.childQueueTrackers {
			if childQT.UnlinkQT([]string{childName}) {
				delete(qt.childQueueTrackers, childName)
			}
		}
	}

	if len(qt.runningApplications) == 0 && len(qt.childQueueTrackers) == 0 {
		return true
	}
	return false
}

// decreaseTrackedResourceUsageDownwards queuePath either could be parent or leaf queue path.
// If it is parent queue path, then reset resourceUsage and runningApplications for all child queues,
// If it is leaf queue path, reset resourceUsage and runningApplications for queue trackers in this queue path.
// Note: Lock free call. The Lock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) decreaseTrackedResourceUsageDownwards(hierarchy []string) map[string]bool {
	// depth first: all the way to the leaf, ignore if not exists
	// more than 1 in the slice means we need to recurse down
	removedApplications := make(map[string]bool)
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] != nil {
			removedApplications = qt.childQueueTrackers[childName].decreaseTrackedResourceUsageDownwards(hierarchy[1:])
		}
	} else if len(hierarchy) <= 1 {
		// reach end of hierarchy, remove all resources under this queue
		removedApplications = qt.runningApplications
		for childName, childQT := range qt.childQueueTrackers {
			if len(childQT.runningApplications) > 0 && !resources.IsZero(childQT.resourceUsage) {
				// runningApplications in parent queue should contain all the running applications in child queues,
				// so we don't need to update removedApplications from child queue result.
				childQT.decreaseTrackedResourceUsageDownwards([]string{childName})
			}
		}
	}
	if len(qt.runningApplications) > 0 && !resources.IsZero(qt.resourceUsage) {
		qt.resourceUsage = nil
		qt.runningApplications = make(map[string]bool)
	}
	return removedApplications
}

// Note: Lock free call. The Lock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
// Note: canRunApp is not read-only, it also traverses the queue hierarchy and creates a childQueueTracker if it does not exist.
func (qt *QueueTracker) canRunApp(hierarchy []string, applicationID string, trackType trackingType) bool {
	// depth first: all the way to the leaf, create if not exists
	// more than 1 in the slice means we need to recurse down
	childCanRunApp := true
	if len(hierarchy) > 1 {
		childName := hierarchy[1]
		if qt.childQueueTrackers[childName] == nil {
			qt.childQueueTrackers[childName] = newQueueTracker(qt.queuePath, childName, trackType)
		}
		childCanRunApp = qt.childQueueTrackers[childName].canRunApp(hierarchy[1:], applicationID, trackType)
	}

	if !childCanRunApp {
		return false
	}

	// arrived at the leaf or on the way out: check against current max if set
	var running int
	if existingApp := qt.runningApplications[applicationID]; existingApp {
		return true
	} else {
		running = len(qt.runningApplications) + 1
	}

	// apply user/group specific limit settings set if configured, otherwise use wild card limit settings
	if qt.maxRunningApps != 0 && running > int(qt.maxRunningApps) {
		return false
	}
	return true
}

// canBeRemoved Start from root and reach all levels of queue hierarchy to confirm whether corresponding queue tracker
// object can be removed from ugm or not. Based on running applications, resource usage, child queue trackers, max running apps, max resources etc
// it decides the removal. It returns false the moment it sees any unexpected values for any queue in any levels.
// Note: Lock free call. The RLock of the linked tracker (UserTracker and GroupTracker) should be held before calling this function.
func (qt *QueueTracker) canBeRemoved() bool {
	for _, childQT := range qt.childQueueTrackers {
		// quick check to avoid further traversal
		if childQT.canBeRemovedInternal() {
			if !childQT.canBeRemoved() {
				return false
			}
		} else {
			return false
		}
	}
	// reached leaf queues, no more to traverse
	return qt.canBeRemovedInternal()
}

func (qt *QueueTracker) canBeRemovedInternal() bool {
	if len(qt.runningApplications) == 0 && resources.IsZero(qt.resourceUsage) && len(qt.childQueueTrackers) == 0 &&
		qt.maxRunningApps == 0 && resources.IsZero(qt.maxResources) {
		return true
	}
	return false
}
