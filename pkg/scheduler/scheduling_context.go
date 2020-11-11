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
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/schedulerevent"
)

var actualSchedulerOptions Options

type Options struct {
	reservationDisabled bool
}

type ClusterSchedulingContext struct {
	partitions map[string]*partitionSchedulingContext

	needPreemption bool

	lock sync.RWMutex
}

func NewClusterSchedulingContext() *ClusterSchedulingContext {
	actualSchedulerOptions = Options{
		reservationDisabled: common.GetBoolEnvVar(DisableReservationVarName, false),
	}
	return &ClusterSchedulingContext{
		partitions: make(map[string]*partitionSchedulingContext),
	}
}

func (csc *ClusterSchedulingContext) getPartitionMapClone() map[string]*partitionSchedulingContext {
	csc.lock.RLock()
	defer csc.lock.RUnlock()

	newMap := make(map[string]*partitionSchedulingContext)
	for k, v := range csc.partitions {
		newMap[k] = v
	}
	return newMap
}

func (csc *ClusterSchedulingContext) getPartition(partitionName string) *partitionSchedulingContext {
	csc.lock.RLock()
	defer csc.lock.RUnlock()

	return csc.partitions[partitionName]
}

// Get the scheduling application based on the ID from the partition.
// Returns nil if the partition or app cannot be found.
// Visible for tests
func (csc *ClusterSchedulingContext) GetSchedulingApplication(appID, partitionName string) *SchedulingApplication {
	csc.lock.RLock()
	defer csc.lock.RUnlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		return partition.getApplication(appID)
	}

	return nil
}

// Get the scheduling queue based on the queue path name from the partition.
// Returns nil if the partition or queue cannot be found.
// Visible for tests
func (csc *ClusterSchedulingContext) GetSchedulingQueue(queueName string, partitionName string) *SchedulingQueue {
	csc.lock.RLock()
	defer csc.lock.RUnlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		return partition.GetQueue(queueName)
	}

	return nil
}

// Return the list of reservations for the partition.
// Returns nil if the partition cannot be found or an empty map if there are no reservations
// Visible for tests
func (csc *ClusterSchedulingContext) GetPartitionReservations(partitionName string) map[string]int {
	csc.lock.RLock()
	defer csc.lock.RUnlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		return partition.getReservations()
	}

	return nil
}

func (csc *ClusterSchedulingContext) addSchedulingApplication(schedulingApp *SchedulingApplication) error {
	partitionName := schedulingApp.ApplicationInfo.Partition
	appID := schedulingApp.ApplicationInfo.ApplicationID

	csc.lock.Lock()
	defer csc.lock.Unlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		if err := partition.addSchedulingApplication(schedulingApp); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find partition=%s while adding app=%s", partitionName, appID)
	}

	return nil
}

func (csc *ClusterSchedulingContext) removeSchedulingApplication(appID string, partitionName string) (*SchedulingApplication, error) {
	csc.lock.Lock()
	defer csc.lock.Unlock()

	if partition := csc.partitions[partitionName]; partition != nil {
		schedulingApp, err := partition.removeSchedulingApplication(appID)
		if err != nil {
			return nil, err
		}
		return schedulingApp, nil
	}
	return nil, fmt.Errorf("failed to find partition=%s while remove app=%s", partitionName, appID)
}

// Update the scheduler's partition list based on the processed config
// - updates existing partitions and the queues linked
// - add new partitions including queues
// updates and add internally are processed differently outside of this method they are the same.
func (csc *ClusterSchedulingContext) updateSchedulingPartitions(partitions []*cache.PartitionInfo) error {
	csc.lock.Lock()
	defer csc.lock.Unlock()
	log.Logger().Info("updating scheduler context",
		zap.Int("numOfPartitionsUpdated", len(partitions)))

	// Walk over the updated partitions
	for _, updatedPartition := range partitions {
		csc.needPreemption = csc.needPreemption || updatedPartition.NeedPreemption()

		partition := csc.partitions[updatedPartition.Name]
		if partition != nil {
			log.Logger().Info("updating scheduling partition",
				zap.String("partitionName", updatedPartition.Name))
			// the partition details don't need updating just the queues
			partition.updatePartitionSchedulingContext(updatedPartition)
		} else {
			log.Logger().Info("creating scheduling partition",
				zap.String("partitionName", updatedPartition.Name))
			// create a new partition and add the queues
			root := newSchedulingQueueInfo(updatedPartition.Root, nil)
			newPartition := newPartitionSchedulingContext(updatedPartition, root)
			newPartition.partitionManager = &partitionManager{
				psc: newPartition,
				csc: csc,
			}
			go newPartition.partitionManager.Run()

			csc.partitions[updatedPartition.Name] = newPartition
		}
	}
	return nil
}

func (csc *ClusterSchedulingContext) RemoveSchedulingPartitionsByRMId(rmID string) {
	csc.lock.Lock()
	defer csc.lock.Unlock()
	partitionToRemove := make(map[string]bool)

	// Just remove corresponding partitions
	for k, partition := range csc.partitions {
		if partition.RmID == rmID {
			partition.partitionManager.stop = true
			partitionToRemove[k] = true
		}
	}

	for partitionName := range partitionToRemove {
		delete(csc.partitions, partitionName)
	}
}

// Remove the partition from the scheduler based on a configuration change
// No resources can be used and the underlying partition should not be running
func (csc *ClusterSchedulingContext) deleteSchedulingPartitions(partitions []*cache.PartitionInfo) error {
	csc.lock.Lock()
	defer csc.lock.Unlock()

	var err error
	// Walk over the deleted partitions
	for _, deletedPartition := range partitions {
		partition := csc.partitions[deletedPartition.Name]
		if partition != nil {
			log.Logger().Info("marking scheduling partition for deletion",
				zap.String("partitionName", deletedPartition.Name))
			partition.partitionManager.Stop()
		} else {
			// collect all errors and keep processing
			if err == nil {
				err = fmt.Errorf("failed to find partition that should have been deleted: %s", deletedPartition.Name)
			} else {
				err = fmt.Errorf("%v, %s", err, deletedPartition.Name)
			}
		}
	}
	return err
}

func (csc *ClusterSchedulingContext) NeedPreemption() bool {
	csc.lock.RLock()
	defer csc.lock.RUnlock()

	return csc.needPreemption
}

// Callback from the partition manager to finalise the removal of the partition
func (csc *ClusterSchedulingContext) removeSchedulingPartition(partitionName string) {
	csc.lock.RLock()
	defer csc.lock.RUnlock()

	delete(csc.partitions, partitionName)
}

// Add a scheduling node based on the cache node that is already added.
// This should not fail as the cache node exists and has been checked.
func (csc *ClusterSchedulingContext) addSchedulingNode(info *cache.NodeInfo) {
	csc.lock.Lock()
	defer csc.lock.Unlock()

	partition := csc.partitions[info.Partition]
	if partition == nil {
		log.Logger().Info("partition not found for new scheduling node",
			zap.String("nodeID", info.NodeID),
			zap.String("partitionName", info.Partition))
		return
	}
	partition.addSchedulingNode(info)
}

// Add a scheduling node based on the cache node that is already added.
// This should not fail as the cache node exists and has been checked.
func (csc *ClusterSchedulingContext) removeSchedulingNode(info *cache.NodeInfo) {
	csc.lock.Lock()
	defer csc.lock.Unlock()

	partition := csc.partitions[info.Partition]
	if partition == nil {
		log.Logger().Info("partition not found for removed scheduling node",
			zap.String("nodeID", info.NodeID),
			zap.String("partitionName", info.Partition))
		return
	}
	partition.removeSchedulingNode(info.NodeID)
}

func (csc *ClusterSchedulingContext) updateSchedulingNode(newInfo *cache.NodeInfo) {
	csc.lock.Lock()
	defer csc.lock.Unlock()

	partition := csc.partitions[newInfo.Partition]
	if partition == nil {
		log.Logger().Info("partition not found for updated scheduling node",
			zap.String("nodeID", newInfo.NodeID),
			zap.String("partitionName", newInfo.Partition))
		return
	}
	partition.updateSchedulingNode(newInfo)
}

// Get a scheduling node based on its name from the partition.
// Returns nil if the partition or node cannot be found.
// Visible for tests
func (csc *ClusterSchedulingContext) GetSchedulingNode(nodeID, partitionName string) *SchedulingNode {
	csc.lock.Lock()
	defer csc.lock.Unlock()

	partition := csc.partitions[partitionName]
	if partition == nil {
		log.Logger().Info("partition not found for scheduling node",
			zap.String("nodeID", nodeID),
			zap.String("partitionName", partitionName))
		return nil
	}
	return partition.getSchedulingNode(nodeID)
}

// Release preempted resources after the cache has been updated.
// This is a lock free call: locks are taken while retrieving the node and when updating the node
func (csc *ClusterSchedulingContext) releasePreemptedResources(resources []schedulerevent.PreemptedNodeResource) {
	// no resources to release just return
	if len(resources) == 0 {
		return
	}
	// walk over the list of preempted resources
	for _, nodeRes := range resources {
		node := csc.GetSchedulingNode(nodeRes.NodeID, nodeRes.Partition)
		if node == nil {
			log.Logger().Info("scheduling node not found trying to release preempted resources",
				zap.String("nodeID", nodeRes.NodeID),
				zap.String("partitionName", nodeRes.Partition),
				zap.Any("resource", nodeRes.PreemptedRes))
			continue
		}
		node.decPreemptingResource(nodeRes.PreemptedRes)
	}
}
