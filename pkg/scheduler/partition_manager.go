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

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
)

const (
	cleanerInterval = 10000 // sleep between queue removal checks
)

var appRemovalInterval = 24 * time.Hour

type partitionManager struct {
	pc       *PartitionContext
	cc       *ClusterContext
	stop     bool
	interval time.Duration
}

// Run the manager for the partition.
// The manager has three tasks:
// - clean up the managed queues that are empty and removed from the configuration
// - remove empty unmanaged queues
// - remove completed applications from the partition
// When the manager exits the partition is removed from the system and must be cleaned up
func (manager partitionManager) Run() {
	if manager.interval == 0 {
		manager.interval = cleanerInterval * time.Millisecond
	}

	log.Logger().Info("starting partition manager",
		zap.String("partition", manager.pc.Name),
		zap.String("interval", manager.interval.String()))
	go manager.cleanupCompletedApps()
	// exit only when the partition this manager belongs to exits
	for {
		time.Sleep(manager.interval)
		runStart := time.Now()
		manager.cleanQueues(manager.pc.root)
		if manager.stop {
			break
		}
		log.Logger().Debug("time consumed for queue cleaner",
			zap.String("duration", time.Since(runStart).String()))
	}
	manager.remove()
}

// Set the flag that the will allow the manager to exit.
// No locking needed as there is just one place where this is called which is already locked.
func (manager partitionManager) Stop() {
	manager.stop = true
}

// Remove drained managed and empty unmanaged queues. Perform the action recursively.
// Only called internally and recursive, no locking
func (manager partitionManager) cleanQueues(queue *objects.Queue) {
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
		log.Logger().Debug("removing queue",
			zap.String("queueName", queue.QueuePath),
			zap.String("partitionName", manager.pc.Name))
		// make sure the queue is empty
		if queue.IsEmpty() {
			// all OK update the queue hierarchy and partition
			if !queue.RemoveQueue() {
				log.Logger().Debug("unexpected failure removing the queue",
					zap.String("partitionName", manager.pc.Name),
					zap.String("queue", queue.QueuePath))
			}
		} else {
			// TODO time out waiting for draining and removal
			log.Logger().Debug("skip removing the queue",
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
//nolint:errcheck
func (manager partitionManager) remove() {
	log.Logger().Info("marking all queues for removal",
		zap.String("partitionName", manager.pc.Name))
	// mark all queues for removal
	manager.pc.root.MarkQueueForRemoval()
	// remove applications: we do not care about return values or issues
	apps := manager.pc.GetApplications()
	log.Logger().Info("removing all applications from partition",
		zap.Int("numOfApps", len(apps)),
		zap.String("partitionName", manager.pc.Name))
	for i := range apps {
		_ = apps[i].HandleApplicationEvent(objects.KillApplication)
		appID := apps[i].ApplicationID
		_ = manager.pc.removeApplication(appID)
	}
	// remove the nodes
	nodes := manager.pc.GetNodes()
	log.Logger().Info("removing all nodes from partition",
		zap.Int("numOfNodes", len(nodes)),
		zap.String("partitionName", manager.pc.Name))
	for i := range nodes {
		_ = manager.pc.removeNode(nodes[i].NodeID)
	}
	log.Logger().Info("removing partition",
		zap.String("partitionName", manager.pc.Name))
	// remove the scheduler object
	manager.cc.removePartition(manager.pc.Name)
}

func (manager partitionManager) cleanupCompletedApps() {
	for {
		if manager.stop {
			break
		}
		manager.pc.cleanupExpiredApps()
		time.Sleep(appRemovalInterval)
	}
}
