/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
    "github.com/golang/glog"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/cache"
    "time"
)

const (
    cleanerInterval = 10000 // sleep between queue removal checks
)

type PartitionManager struct {
    psc      *PartitionSchedulingContext
    csc      *ClusterSchedulingContext
    stop     bool
    interval time.Duration
}

// Run the manager for the partition.
// The manager has two tasks:
// - clean up the managed queues that are empty and removed from the configuration
// - remove empty unmanaged queues
// When the manager exits the partition is removed from the system and must be cleaned up
func (manager PartitionManager) Run() {
    if manager.interval == 0 {
        manager.interval = cleanerInterval * time.Millisecond
    }
    glog.V(0).Infof("Starting manager for the partition %s, sleep interval: %v", manager.psc.Name, manager.interval)
    // exit only when the partition this manager belongs to exits
    for {
        glog.V(4).Infof("Queue cleaner sleeping partition %s", manager.psc.Name)
        time.Sleep(manager.interval)
        runStart := time.Now()
        manager.cleanQueues(manager.psc.Root)
        if manager.stop {
            break
        }
        glog.V(4).Infof("Processing time for queue cleaner: %v", time.Since(runStart))
    }
    glog.V(0).Infof("Exiting queue cleaner for the partition %s", manager.psc.Name)
    manager.remove()
    glog.V(0).Infof("Exiting manager for the partition %s", manager.psc.Name)
}

// Set the flag that the will allow the manager to exit.
// No locking needed as there is just one place where this is called which is already locked.
func (manager PartitionManager) Stop() {
    manager.stop = true
}

// Remove empty managed or unmanaged queue. The logic is mostly hidden in the cached object(s).
// Perform the action recursively.
// Only called internally and recursive, no locking
func (manager PartitionManager) cleanQueues(schedulingQueue *SchedulingQueue) {
    if schedulingQueue == nil {
        return
    }
    // check the children first: call recursive
    if children := schedulingQueue.GetCopyOfChildren(); len(children) != 0 {
        for _, child := range children {
            manager.cleanQueues(child)
        }
    }
    // when we have done the children (or have none) this schedulingQueue might be removable
    if schedulingQueue.isDraining() || !schedulingQueue.isManaged() {
        glog.V(4).Infof("Queue cleaner trying to remove scheduling schedulingQueue %s from partition %s", schedulingQueue.Name, manager.psc.Name)
        // make sure the queue is empty
        if len(schedulingQueue.applications) == 0 {
            // remove the cached queue, if not empty there is a problem since we have no applications left.
            if schedulingQueue.CachedQueueInfo.RemoveQueue() {
                // all OK update the queue hierarchy and partition
                schedulingQueue.RemoveQueue()
                manager.psc.RemoveQueue(schedulingQueue.Name)
            } else {
                glog.V(0).Infof("Queue cleaner failed to remove scheduling queue (%s)%s due to existing allocations (%v) and 0 applications assigned",
                    manager.psc.Name, schedulingQueue.Name, schedulingQueue.CachedQueueInfo.GetAllocatedResource())
            }
        } else {
            // TODO time out waiting for draining and removal
            glog.V(0).Infof("Queue cleaner failed to remove scheduling schedulingQueue (%s)%s due to existing assigned applications",
                manager.psc.Name, schedulingQueue.Name)
        }
    }
}

// The partition has been removed from the configuration and must be removed.
// Clean up all linked objects:
// - queues
// - applications
// - nodes
// last action is to remove the cluster links
func (manager PartitionManager) remove() {
    glog.V(4).Infof("Marking all queues for removal on partition %s", manager.psc.Name)
    pi := manager.psc.partition
    // mark all queues for removal
    pi.Root.MarkQueueForRemoval()
    // remove applications: we do not care about return values or issues
    apps := pi.GetApplications()
    glog.V(4).Infof("Removing all applications (%d total) from partition %s", len(apps), manager.psc.Name)
    for i := range apps {
        _ = apps[i].HandleApplicationEvent(cache.KillApplication)
        appId := apps[i].ApplicationId
        _, _ = pi.RemoveApplication(appId)
        _, _ = manager.psc.RemoveSchedulingApplication(appId)
    }
    // remove the nodes
    nodes := pi.CopyNodeInfos()
    glog.V(4).Infof("Removing all nodes (%d total) from partition %s", len(nodes), manager.psc.Name)
    for i := range nodes {
        pi.RemoveNode(nodes[i].NodeId)
    }
    glog.V(4).Infof("Removing partition from cluster %s", manager.psc.Name)
    // remove the cache object
    pi.Remove()
    // remove the scheduler object
    manager.csc.removeSchedulingPartition(manager.psc.Name)
}
