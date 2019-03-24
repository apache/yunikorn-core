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
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "regexp"
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

func updatePartitionInfoFromConfig(partitionInfo *PartitionInfo, conf *configs.PartitionConfig) error {
    for _, q := range conf.Queues {
        queueInfo := NewQueueInfo(q.Name, nil)
        if err := updateQueueInfo(queueInfo, &q); err != nil {
            return err
        }
        partitionInfo.queues[queueInfo.Name] = queueInfo
        if queueInfo.Parent == nil {
            partitionInfo.Root = queueInfo
        }
    }

    // populate flat full-qualified-queue-name to queue mapping
    partitionInfo.initQueues(partitionInfo.Root)

    // get root queue
    // Root queue must be existed
    if partitionInfo.Root == nil {
        return errors.New("failed to find root queue, it must be defined")
    }

    // ensure there is only one root
    if len(partitionInfo.queues) > 1 {
        return errors.New("each partition can have and only have 1 root queue defined")
    }

    // Check loop in the queue
    visited := make(map[string]bool)
    if err := recursiveCheckQueue(partitionInfo.Root, visited); err != nil {
        return err
    }

    // Check resource configurations
    if err := checkResourceConfigurationsForQueue(partitionInfo.Root, nil); err != nil {
        return err
    }
    return nil
}

func updateQueueInfo(queueInfo *QueueInfo, conf *configs.QueueConfig) error {
    reg := regexp.MustCompile("^[a-zA-Z0-9_-]{1,16}$")
    // check queue name
    if !reg.MatchString(conf.Name) {
        return errors.New(fmt.Sprintf("invalid queue name %s, name must only have alphabets," +
            " numbers, - or _, and no longer than 16 chars", conf.Name))
    }

    // update queue configs
    if err := updateQueueInfoFromConfig(queueInfo, conf); err != nil {
        return err
    }

    // recursively update child queue info
    children := make(map[string]*QueueInfo)
    queueInfo.children = children
    for _, child := range conf.Queues {
        // under same parent, dup queue names are disallowed
        if qn, ok := children[child.Name]; ok {
            return errors.New(fmt.Sprintf(
                "duplicate queue name is found under same parent, queue name: %s",
                qn.FullQualifiedPath))
        }
        childQueue := NewQueueInfo(child.Name, queueInfo)
        children[child.Name] = childQueue
        if err := updateQueueInfo(childQueue, &child); err != nil {
            return err
        }
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
func recursiveCheckQueue(root *QueueInfo, visited map[string]bool) error {
    if visited[root.Name] {
        return errors.New("detected loop in queue hierarchy definition from config, please double check")
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
    if queueInfo.Parent != nil && queueInfo.Parent.Properties != nil {
        queueInfo.Properties = mergeProperties(queueInfo.Parent.Properties, conf.Properties)
    }

    return nil
}

func mergeProperties(parent map[string]string, child map[string]string) map[string]string {
    merged := make(map[string]string)
    if parent != nil && len(parent) > 0 {
        for key, value := range parent {
            merged[key] = value
        }
    }
    if child != nil && len(child) > 0 {
        for key, value := range child {
            merged[key] = value
        }
    }
    return merged
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