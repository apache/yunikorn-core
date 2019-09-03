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

package cache

import (
    "fmt"
    "github.com/cloudera/yunikorn-core/pkg/common"
    "github.com/cloudera/yunikorn-core/pkg/common/configs"
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "go.uber.org/zap"
)

// Create partition info objects from the configuration to set in the cluster.
// - The cluster must not have any partitions set (checked in the caller)
// - A validated config must be passed in.
func createPartitionInfos(clusterInfo *ClusterInfo, conf *configs.SchedulerConfig, rmId string) ([]*PartitionInfo, error) {
    // cluster info has versions,
    // this is determined by the checksum of the configuration file
    updatedPartitions := make([]*PartitionInfo, 0)
    for _, p := range conf.Partitions {
        partitionName := common.GetNormalizedPartitionName(p.Name, rmId)
        p.Name = partitionName
        partition, err := newPartitionInfo(p, rmId, clusterInfo)
        if err != nil {
            return []*PartitionInfo{}, err
        }

        clusterInfo.addPartition(partitionName, partition)
        updatedPartitions = append(updatedPartitions, partition)
        log.Logger().Info("added partition", zap.String("partition", partitionName))
    }

    return updatedPartitions, nil
}

// Create the mew partition configuration and ass all of them to the cluster.
// This function may only be called by the scheduler when a RM registers.
// It creates a new PartitionInfo from scratch and does not merge the configurations.
func SetClusterInfoFromConfigFile(clusterInfo *ClusterInfo, rmId string, policyGroup string) ([]*PartitionInfo, error) {
    // we should not have any partitions set at this point
    if len(clusterInfo.partitions) > 0 {
        return []*PartitionInfo{}, fmt.Errorf("RM %s has been registerd before, active partitions %d", rmId, len(clusterInfo.partitions))
    }
    // load the config this returns a validated configuration
    conf, err := configs.SchedulerConfigLoader(policyGroup)
    if err != nil {
        return []*PartitionInfo{}, err
    }

    // update global scheduler configs
    configs.ConfigContext.Set(policyGroup, conf)

    updatedPartitions, err := createPartitionInfos(clusterInfo, conf, rmId)

    if err != nil {
        return []*PartitionInfo{}, err
    }

    return updatedPartitions, nil
}

// Update the existing cluster info:
// - add new partitions
// - update existing partitions
// - remove deleted partitions
// updates and add internally are processed differently outside of this method they are the same.
func UpdateClusterInfoFromConfigFile(clusterInfo *ClusterInfo, rmId string) ([]*PartitionInfo, []*PartitionInfo, error) {
    // we must have partitions set at this point
    if len(clusterInfo.partitions) == 0 {
        return []*PartitionInfo{}, []*PartitionInfo{}, fmt.Errorf("RM %s has no active partitions, make sure it is registered", rmId)
    }
    // load the config this returns a validated configuration
    conf, err := configs.SchedulerConfigLoader(clusterInfo.policyGroup)
    if err != nil {
        return []*PartitionInfo{}, []*PartitionInfo{}, err
    }

    // update global scheduler configs
    configs.ConfigContext.Set(clusterInfo.policyGroup, conf)

    // Start updating the config is OK and should pass setting on the cluster
    log.Logger().Info("updating partitions", zap.String("rmId", rmId))
    // keep track of the deleted and updated partitions
    updatedPartitions := make([]*PartitionInfo, 0)
    visited := map[string]bool{}
    // walk over the partitions in the config: update existing ones
    for _, p := range conf.Partitions {
        partitionName := common.GetNormalizedPartitionName(p.Name, rmId)
        p.Name = partitionName
        part, ok := clusterInfo.partitions[p.Name]
        if ok {
            // make sure the new info passes all checks
            _, err = newPartitionInfo(p, rmId, nil)
            if err != nil {
                return []*PartitionInfo{}, []*PartitionInfo{}, err
            }
            // checks passed perform the real update
            log.Logger().Info("updating partitions", zap.String("partitionName", partitionName))
            err = part.updatePartitionDetails(p)
            if err != nil {
                return []*PartitionInfo{}, []*PartitionInfo{}, err
            }
        } else {
            // not found: new partition, no checks needed
            log.Logger().Info("added partitions", zap.String("partitionName", partitionName))

            part, err = newPartitionInfo(p, rmId, clusterInfo)
            clusterInfo.addPartition(partitionName, part)
            if err != nil {
                return []*PartitionInfo{}, []*PartitionInfo{}, err
            }
        }
        // add it to the partitions to update
        updatedPartitions = append(updatedPartitions, part)
        visited[p.Name] = true
    }

    // get the removed partitions, mark them as deleted
    deletedPartitions := make([]*PartitionInfo, 0)
    for _, part := range clusterInfo.partitions {
        if !visited[part.Name] {
            part.MarkPartitionForRemoval()
            deletedPartitions = append(deletedPartitions, part)
            log.Logger().Info("marked partition for removal",
                zap.String("partitionName", part.Name))
        }
    }

    return updatedPartitions, deletedPartitions, nil
}

// Create a new checked PartitionInfo
// convenience method that wraps creation and checking the settings.
func newPartitionInfo(part configs.PartitionConfig, rmId string, info *ClusterInfo) (*PartitionInfo, error) {

    partition, err := NewPartitionInfo(part, rmId, info)
    if err != nil {
        return nil, err
    }
    // sanity check the partition and the queues below it now that we have a full setup
    err = checkResourceConfigurationsForQueue(partition.Root, nil)
    if err != nil {
        return nil, err
    }
    return partition, nil
}

// Check the queue resource configuration settings.
// - child or children cannot have higher maximum or guaranteed limits than parents
// - children (added together) cannot have a higher guaranteed setting than a parent
// TODO add maximum number of running applications
func checkResourceConfigurationsForQueue(cur *QueueInfo, parent *QueueInfo) error {
    // If cur has children, make sure sum of children's guaranteed <= cur.guaranteed
    if len(cur.children) > 0 {
        // Check children
        for _, child := range cur.children {
            if err := checkResourceConfigurationsForQueue(child, cur); err != nil {
                return err
            }
        }

        sum := resources.NewResource()
        for _, child := range cur.children {
            sum = resources.Add(sum, child.GuaranteedResource)
        }

        if cur.GuaranteedResource != nil {
            if !resources.FitIn(cur.GuaranteedResource, sum) {
                return fmt.Errorf("queue %s has guaranteed-resources (%v) smaller than sum of children guaranteed resources (%v)", cur.Name, cur.GuaranteedResource, sum)
            }
        } else {
            cur.GuaranteedResource = sum
        }
    } else {
        // When the queue doesn't have children, set guaranteed to zero if absent.
        if cur.GuaranteedResource == nil {
            cur.GuaranteedResource = resources.NewResource()
        }
    }

    // If Max resource not set, use parent's max resource
    if cur.MaxResource == nil && parent != nil && parent.MaxResource != nil {
        cur.MaxResource = parent.MaxResource
    }

    // If max resource exist, check guaranteed fits in max, cur.max fit in parent.max
    if cur.MaxResource != nil {
        if parent != nil && parent.MaxResource != nil {
            if !resources.FitIn(parent.MaxResource, cur.MaxResource) {
                return fmt.Errorf("queue %s has max resources (%v) set larger than parent's max resources (%v)", cur.Name, cur.MaxResource, parent.MaxResource)
            }
        }

        if !resources.FitIn(cur.MaxResource, cur.GuaranteedResource) {
            return fmt.Errorf("queue %s has max resources (%v) set smaller than guaranteed resources (%v)", cur.Name, cur.MaxResource, cur.GuaranteedResource)
        }
    }

    return nil
}
