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

package objects

import (
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
)

type ApplicationMoveStat struct {
	Moved                  bool
	FromQueue              string
	NumApps                int
	Reservations           int
	RunningApps            uint64
	ReservedApps           int
	AllocatingAcceptedApps int
	PendingResource        *resources.Resource
	AllocatedResource      *resources.Resource
	UgmResult              ugm.TrackerMoveResult
}

func (sq *Queue) PreserveRunningAppsInHierarchy() ([]ApplicationMoveStat, error) {
	stats := make([]ApplicationMoveStat, 0)
	childQueues := sq.GetCopyOfChildren()
	var err error
	for _, queue := range childQueues {
		if queue.IsLeafQueue() {
			var leafStat ApplicationMoveStat
			if leafStat, err = queue.preserveRunningApps(); err != nil {
				return nil, err
			}
			stats = append(stats, leafStat)
			sq.removeChildQueue(queue.Name)
			continue
		}

		var childStats []ApplicationMoveStat
		if childStats, err = queue.PreserveRunningAppsInHierarchy(); err != nil {
			return nil, err
		}
		stats = append(stats, childStats...)
		sq.removeChildQueue(queue.Name)
	}

	return stats, nil
}

func (sq *Queue) preserveRunningApps() (ApplicationMoveStat, error) {
	if !sq.isLeaf {
		return ApplicationMoveStat{Moved: false}, nil
	}
	numApps := len(sq.applications)

	if numApps > 0 {
		log.Logger().Info("Moving application from queue", zap.String("queue", sq.QueuePath))
		preservedQueue, err := sq.getPreservedQueueOrCreate()
		if err != nil {
			return ApplicationMoveStat{Moved: false}, err
		}
		queuesToUpdate := sq.collectQueuesToUpdate()

		sq.moveAppsToQueue(preservedQueue)
		pendingResource := sq.updatePendingResourceInHierarchy(preservedQueue, queuesToUpdate)
		reservedApps := len(sq.reservedApps)
		reservations := sq.updateReservations(preservedQueue)
		totalAllocatingAccepted := sq.updateAllocatingAcceptedAppsInHierarchy(preservedQueue, queuesToUpdate)
		allocatedResource := sq.updateAllocatedResourceInHierarchy(preservedQueue, queuesToUpdate)
		runningApps := sq.updateRunningAppsInHierarchy(preservedQueue, queuesToUpdate)

		sq.updatePriorities()
		preservedQueue.updateAllocatedAndPendingResourceMetrics()
		preservedQueue.updateGuaranteedResourceMetrics()
		preservedQueue.updateMaxResourceMetrics()
		preservedQueue.updatePriorities()

		var ugmMoveResult ugm.TrackerMoveResult
		manager := ugm.GetUserManager()
		ugmMoveResult, err = manager.PreserveRunningApplications(sq.QueuePath)
		if err != nil {
			log.Logger().Error("User/group tracking update failed", zap.Error(err))
		}

		stats := ApplicationMoveStat{
			Moved:                  true,
			FromQueue:              sq.QueuePath,
			NumApps:                numApps,
			ReservedApps:           reservedApps,
			RunningApps:            runningApps,
			Reservations:           reservations,
			AllocatingAcceptedApps: totalAllocatingAccepted,
			PendingResource:        pendingResource,
			AllocatedResource:      allocatedResource,
			UgmResult:              ugmMoveResult,
		}

		return stats, nil
	}

	return ApplicationMoveStat{Moved: false, FromQueue: sq.QueuePath}, nil
}

func (sq *Queue) getPreservedQueueOrCreate() (*Queue, error) {
	preserved := sq.root.getPreservedQueue()
	if preserved == nil {
		log.Logger().Info("Creating queue for applications already running",
			zap.String("queue path", configs.PreservedQueuePath))
		p, err := NewPreservedQueue(sq.root)
		if err != nil {
			return nil, err
		}
		preserved = p
	}

	return preserved, nil
}

func (sq *Queue) moveAppsToQueue(target *Queue) {
	for _, app := range sq.applications {
		log.Logger().Info("Moving application",
			zap.String("appID", app.ApplicationID),
			zap.String("state", app.CurrentState()),
			zap.Int("no of all allocations", len(app.GetAllAllocations())),
			zap.Int("no of asks", len(app.GetAllRequests())))
		target.AddApplication(app)
		app.SetQueue(target)
		app.SetQueuePath(target.GetQueuePath())
	}
	sq.applications = make(map[string]*Application)
}

func (sq *Queue) updatePendingResourceInHierarchy(target *Queue, queuesToUpdate []*Queue) *resources.Resource {
	pending := sq.pending
	target.incPendingResourceDirect(pending)
	for _, queue := range queuesToUpdate {
		queue.decPendingResourceDirect(pending)
	}
	sq.pending = resources.NewResource()

	return pending
}

func (sq *Queue) updateReservations(target *Queue) int {
	totalNumRes := 0
	for appID, numRes := range sq.reservedApps {
		target.setReservations(appID, numRes)
		totalNumRes += numRes
	}
	sq.reservedApps = make(map[string]int)

	return totalNumRes
}

func (sq *Queue) updateAllocatingAcceptedAppsInHierarchy(target *Queue, queuesToUpdate []*Queue) int {
	totalAllocatingAccepted := 0
	appList := make([]string, 0)
	for appID := range sq.allocatingAcceptedApps {
		target.setAllocatingAccepted(appID)
		totalAllocatingAccepted++
		appList = append(appList, appID)
	}
	for _, queue := range queuesToUpdate {
		queue.removeAllocatingAcceptedApps(appList)
	}
	sq.allocatingAcceptedApps = make(map[string]bool)

	return totalAllocatingAccepted
}

func (sq *Queue) updateAllocatedResourceInHierarchy(target *Queue, queuesToUpdate []*Queue) *resources.Resource {
	allocatedResource := sq.allocatedResource
	target.incAllocatedResourceDirect(allocatedResource)
	for _, queue := range queuesToUpdate {
		queue.decAllocatedResourceDirect(allocatedResource)
	}
	sq.allocatedResource = resources.NewResource()
	return allocatedResource
}

func (sq *Queue) updateRunningAppsInHierarchy(target *Queue, queuesToUpdate []*Queue) uint64 {
	runningApps := sq.runningApps
	target.incRunningAppsDirect(runningApps)
	for _, queue := range queuesToUpdate {
		queue.decRunningAppsDirect(runningApps)
	}
	sq.runningApps = 0

	return runningApps
}

func (sq *Queue) updatePriorities() {
	priority := sq.recalculatePriority()

	sq.parent.UpdateQueuePriority(sq.Name, priority)
}

func (sq *Queue) incPendingResourceDirect(delta *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	sq.pending = resources.Add(sq.pending, delta)
}

func (sq *Queue) decPendingResourceDirect(delta *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	var err error
	sq.pending, err = resources.SubErrorNegative(sq.pending, delta)
	if err != nil {
		log.Logger().Warn("Pending resources went negative",
			zap.String("queueName", sq.QueuePath),
			zap.Error(err))
	}
}

func (sq *Queue) incRunningAppsDirect(num uint64) {
	sq.Lock()
	defer sq.Unlock()
	sq.runningApps += num
}

func (sq *Queue) decRunningAppsDirect(num uint64) {
	sq.Lock()
	defer sq.Unlock()
	sq.runningApps -= num
}

func (sq *Queue) decAllocatedResourceDirect(alloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	newAllocated := resources.Sub(sq.allocatedResource, alloc)
	sq.allocatedResource = newAllocated
}

func (sq *Queue) incAllocatedResourceDirect(alloc *resources.Resource) {
	sq.Lock()
	defer sq.Unlock()
	newAllocated := resources.Add(sq.allocatedResource, alloc)
	sq.allocatedResource = newAllocated
}

func (sq *Queue) removeAllocatingAcceptedApps(appIDs []string) {
	sq.Lock()
	defer sq.Unlock()
	for _, appID := range appIDs {
		delete(sq.allocatingAcceptedApps, appID)
	}
}

func (sq *Queue) setPreservedQueue(preserved *Queue) {
	sq.Lock()
	defer sq.Unlock()
	sq.preserved = preserved
}
func (sq *Queue) getPreservedQueue() *Queue {
	sq.Lock()
	defer sq.Unlock()
	return sq.preserved
}

func (sq *Queue) collectQueuesToUpdate() []*Queue {
	toUpdate := make([]*Queue, 0)

	for current := sq.parent; current != sq.root && current != nil; {
		toUpdate = append(toUpdate, current)
		current = current.parent
	}

	return toUpdate
}

func LogMoveStat(stat ApplicationMoveStat) {
	log.Logger().Info("Application move statistics",
		zap.String("from queue", stat.FromQueue),
		zap.Int("total applications moved", stat.NumApps),
		zap.Int("total number of reservations", stat.Reservations),
		zap.Uint64("number of running apps", stat.RunningApps),
		zap.Int("number of reserved apps", stat.ReservedApps),
		zap.Int("number of allocating accepted apps", stat.AllocatingAcceptedApps),
		zap.Any("pending resource in queue", stat.PendingResource),
		zap.Any("total allocated resource", stat.AllocatedResource))

	if stat.UgmResult.Error != nil {
		return
	}

	log.Logger().Info("User/group tracking move statistics",
		zap.Any("tracked user resources affected by move", stat.UgmResult.TotalUserTrackedResource),
		zap.Any("tracked group resources affected by move", stat.UgmResult.TotalGroupTrackedResource),
		zap.Strings("affected groups", stat.UgmResult.Groups),
		zap.Strings("affected users", stat.UgmResult.Users),
		zap.Int("number of apps tracked for users", stat.UgmResult.NumAppsForUser),
		zap.Int("number of apps tracked for groups", stat.UgmResult.NumAppsForGroups))
}
