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

package scheduler

import (
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
)

const (
	DefaultCleanRootInterval        = 10000 * time.Millisecond // sleep between queue removal checks
	DefaultCleanExpiredAppsInterval = 24 * time.Hour           // sleep between apps removal checks
)

type partitionManager struct {
	pc                       *PartitionContext
	cc                       *ClusterContext
	stopCleanRoot            chan struct{}
	stopCleanExpiredApps     chan struct{}
	cleanRootInterval        time.Duration
	cleanExpiredAppsInterval time.Duration
}

func newPartitionManager(pc *PartitionContext, cc *ClusterContext) *partitionManager {
	return &partitionManager{
		pc:                       pc,
		cc:                       cc,
		stopCleanRoot:            make(chan struct{}),
		stopCleanExpiredApps:     make(chan struct{}),
		cleanRootInterval:        DefaultCleanRootInterval,
		cleanExpiredAppsInterval: DefaultCleanExpiredAppsInterval,
	}
}

// Run the manager for the partition.
// The manager has four tasks:
// - clean up the managed queues that are empty and removed from the configuration
// - remove empty unmanaged queues
// - remove completed applications from the partition
// - remove rejected applications from the partition
// When the manager exits the partition is removed from the system and must be cleaned up
func (manager *partitionManager) Run() {
	log.Log(log.SchedPartition).Info("starting partition manager",
		zap.String("partition", manager.pc.Name),
		zap.Stringer("cleanRootInterval", manager.cleanRootInterval))
	go manager.cleanExpiredApps()
	go manager.cleanRoot()
}

func (manager *partitionManager) cleanRoot() {
	log.Log(log.SchedPartition).Info("Starting partition queue cleaner")
	// exit only when the partition this manager belongs to exits
	for {
		cleanRootInterval := manager.cleanRootInterval
		if cleanRootInterval <= 0 {
			cleanRootInterval = DefaultCleanRootInterval
		}
		select {
		case <-manager.stopCleanRoot:
			return
		case <-time.After(cleanRootInterval):
			runStart := time.Now()
			manager.cleanQueues(manager.pc.root)
			log.Log(log.SchedPartition).Debug("time consumed for queue cleaner",
				zap.Stringer("duration", time.Since(runStart)))
		}
	}
}

// Set the flag that the will allow the manager to exit.
// No locking needed as there is just one place where this is called which is already locked.
func (manager *partitionManager) Stop() {
	log.Log(log.SchedPartition).Info("Stopping partition manager",
		zap.String("partition", manager.pc.Name))
	close(manager.stopCleanExpiredApps)
	close(manager.stopCleanRoot)
	manager.remove()
}

// Remove drained managed and empty unmanaged queues. Perform the action recursively.
// Only called internally and recursive, no locking
func (manager *partitionManager) cleanQueues(queue *objects.Queue) {
	if queue == nil {
		return
	}
	// check the children first: call recursive
	if children := queue.GetCopyOfChildren(); len(children) != 0 {
		for _, child := range children {
			manager.cleanQueues(child)
		}
	}
	// when we have done the children (or have none) this queue might be removable
	if queue.IsDraining() || !queue.IsManaged() {
		log.Log(log.SchedPartition).Debug("removing queue",
			zap.String("queueName", queue.QueuePath),
			zap.String("partitionName", manager.pc.Name))
		// make sure the queue is empty
		if queue.IsEmpty() {
			// all OK update the queue hierarchy and partition
			if !queue.RemoveQueue() {
				log.Log(log.SchedPartition).Debug("unexpected failure removing the queue",
					zap.String("partitionName", manager.pc.Name),
					zap.String("queue", queue.QueuePath))
			}
		} else {
			// TODO time out waiting for draining and removal
			log.Log(log.SchedPartition).Debug("skip removing the queue",
				zap.String("reason", "there are existing assigned apps or leaf queues"),
				zap.String("queue", queue.QueuePath),
				zap.String("partitionName", manager.pc.Name))
		}
	}
}

// The partition has been removed from the configuration and must be removed.
// Clean up all linked objects:
// - queues
// - applications
// - nodes
// last action is to remove the cluster links
//
//nolint:errcheck
func (manager *partitionManager) remove() {
	log.Log(log.SchedPartition).Info("marking all queues for removal",
		zap.String("partitionName", manager.pc.Name))
	// mark all queues for removal
	manager.pc.root.MarkQueueForRemoval()
	// remove applications: we do not care about return values or issues
	apps := manager.pc.GetApplications()
	log.Log(log.SchedPartition).Info("removing all applications from partition",
		zap.Int("numOfApps", len(apps)),
		zap.String("partitionName", manager.pc.Name))
	for i := range apps {
		_ = apps[i].FailApplication("PartitionRemoved")
		appID := apps[i].ApplicationID
		_ = manager.pc.removeApplication(appID)
	}
	// remove the nodes
	nodes := manager.pc.GetNodes()
	log.Log(log.SchedPartition).Info("removing all nodes from partition",
		zap.Int("numOfNodes", len(nodes)),
		zap.String("partitionName", manager.pc.Name))
	for i := range nodes {
		_, _ = manager.pc.removeNode(nodes[i].NodeID)
	}
	log.Log(log.SchedPartition).Info("removing partition",
		zap.String("partitionName", manager.pc.Name))
	// remove the scheduler object
	manager.cc.removePartition(manager.pc.Name)
}

func (manager *partitionManager) cleanExpiredApps() {
	log.Log(log.SchedPartition).Info("Starting partition expired apps cleaner")
	for {
		cleanExpiredAppsInterval := manager.cleanExpiredAppsInterval
		if cleanExpiredAppsInterval <= 0 {
			cleanExpiredAppsInterval = DefaultCleanExpiredAppsInterval
		}
		select {
		case <-manager.stopCleanExpiredApps:
			return
		case <-time.After(cleanExpiredAppsInterval):
			manager.pc.cleanupExpiredApps()
		}
	}
}
