/*
Copyright 2019 The Unity Scheduler Authors

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
    "errors"
    "fmt"
    "github.com/golang/glog"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/configs"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/resources"
    "strconv"
)

// Return updated partition names
func updateClusterInfoFromConfig(clusterInfo *ClusterInfo, conf *configs.SchedulerConfig, rmId string) ([]*PartitionInfo, error) {
    updatedPartitions := make([]*PartitionInfo, 0)
    for _, p := range conf.Partitions {
        partitionName := common.GetNormalizedPartitionName(p.Name, rmId)

        if clusterInfo.GetPartition(partitionName) != nil {
            return []*PartitionInfo{}, errors.New(fmt.Sprintf("Partition=%s is already defined in scheduler config", p.Name))
        }

        partition := newPartitionInfo(partitionName)
        if err := updatePartitionInfoFromConfig(partition, &p); err != nil {
            return []*PartitionInfo{}, err
        }

        clusterInfo.addPartition(partitionName, partition)
        updatedPartitions = append(updatedPartitions, partition)

        glog.V(0).Infof("Added partition=%s to cluster", partitionName)
    }

    return updatedPartitions, nil
}

func updatePartitionInfoFromConfig(partitionInfo *PartitionInfo, conf *configs.PartitionConfig) (error) {
    // first make a map of queue name to config
    queueConfigMap := make(map[string]configs.QueueConfig)

    for _, v := range conf.Queues {
        if _, ok := queueConfigMap[v.Name]; ok {
            return errors.New(fmt.Sprintf("Queue=%s, is duplicated in config.", v.Name))
        }
        queueConfigMap[v.Name] = v
    }

    // Then initialize queues on partitions
    queuesMap := partitionInfo.queues
    for queueName, queueConfig := range queueConfigMap {
        queueInfo := NewQueueInfo(queueName, nil)
        if err := updateQueueInfoFromConfig(queueInfo, &queueConfig); err != nil {
            return err
        }

        queuesMap[queueName] = queueInfo
    }

    // Make connections of queue
    for queueName, queueConfig := range queueConfigMap {
        queue := queuesMap[queueName]

        for _, childName := range queueConfig.Children {
            var childQueue *QueueInfo
            if childQueue = queuesMap[childName]; childQueue == nil {
                return errors.New(fmt.Sprintf("Defined child queue=%s under parent=%s, but child queue=%s not defined", childName, queueConfig.Name, childName))
            }

            if queue.children[childName] != nil {
                return errors.New(fmt.Sprintf("Defined duplicated child queue=%s, under parent=%s", childName, queueName))
            }

            queue.children[childName] = childQueue
            queue.IsLeafQueue = false

            if childQueue.Parent != nil {
                return errors.New(fmt.Sprintf("Multiple queue defined queue=%s as children, please double check", childName))
            }
            childQueue.Parent = queue
        }
    }

    // get root queue
    // Root queue must be existed
    partitionInfo.Root = queuesMap["root"]
    if partitionInfo.Root == nil {
        return errors.New("failed to find root queue, it must be defined")
    }

    // Check loop in the queue
    visited := make(map[string]bool)
    if err := recursiveCheckQueue(partitionInfo.Root, visited); err != nil {
        return err
    }

    // From root queue it can reach all queues
    if len(visited) != len(queuesMap) {
        return errors.New("size of visited queue from root is not same as defined queue, " +
            "it could be caused by some queue is not be able to reach from root")
    }

    // Check resource configurations
    if err := checkResourceConfigurationsForQueue(partitionInfo.Root, nil); err != nil {
        return err
    }
    return nil
}

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
                return errors.New(fmt.Sprintf("Queue=%s, guaranteed-resource=%v < sum of children guarantees=%v", cur.Name, cur.GuaranteedResource, sum))
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
                return errors.New(fmt.Sprintf("Queue=%s, max-resource=%v, not fitin parent's max=%v", cur.Name, cur.MaxResource, parent.MaxResource))
            }
        }

        if !resources.FitIn(cur.MaxResource, cur.GuaranteedResource) {
            return errors.New(fmt.Sprintf("Queue=%s, max-resource=%v < guaranteed-resource=%v", cur.Name, cur.MaxResource, cur.GuaranteedResource))
        }
    }

    return nil
}

// There should not loop in the queues
func recursiveCheckQueue(root *QueueInfo, visited map[string]bool) (error) {
    if visited[root.Name] {
        return errors.New("detected loop in queue hierachy definition from config, please double check")
    }

    visited[root.Name] = true

    for _, child := range root.children {
        if err := recursiveCheckQueue(child, visited); err != nil {
            return err
        }
    }

    return nil
}

func getResourceFromMap(configMap map[string]string) (*resources.Resource, error) {
    resMap := make(map[string]resources.Quantity)
    for k, v := range configMap {
        intValue, err := strconv.ParseInt(v, 10, 64)
        if err != nil {
            return nil, err
        }
        resMap[k] = resources.Quantity(intValue)
    }
    return resources.NewResourceFromMap(resMap), nil
}

func updateQueueInfoFromConfig(queueInfo *QueueInfo, conf *configs.QueueConfig) error {
    guaranteed, err := getResourceFromMap(conf.Resources.Guaranteed)
    if err != nil {
        return err
    }
    if 0 != len(guaranteed.Resources) {
        queueInfo.GuaranteedResource = guaranteed
    }

    max, err := getResourceFromMap(conf.Resources.Max)
    if err != nil {
        return err
    }
    if 0 != len(max.Resources) {
        queueInfo.MaxResource = max
    }

    // Update Properties
    queueInfo.Properties = conf.Properties

    return nil
}

// This function should be called by scheduler to invoke
func UpdateClusterInfoFromConfigFile(clusterInfo *ClusterInfo, rmId string, policyGroup string) ([]*PartitionInfo, error) {
    conf, err := configs.SchedulerConfigLoader(policyGroup)
    if err != nil {
        return []*PartitionInfo{}, err
    }

    updatedPartitions, err := updateClusterInfoFromConfig(clusterInfo, conf, rmId)

    if err != nil {
        return []*PartitionInfo{}, err
    }

    return updatedPartitions, nil
}
