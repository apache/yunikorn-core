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
    "github.com/cloudera/yunikorn-core/pkg/common/resources"
    "github.com/cloudera/yunikorn-core/pkg/log"
    "github.com/cloudera/yunikorn-core/pkg/metrics"
    "github.com/google/btree"
    "go.uber.org/zap"
    "sort"
    "strings"
    "time"
)

// Sort type for queues, apps, nodes etc.
type SortType int32

const (
    FairSortPolicy        = 0
    FifoSortPolicy        = 1
    MaxAvailableResources = 2 // node sorting, descending on available resources
    MinAvailableResources = 3 // node sorting, ascending on available resources
)

func SortQueue(queues []*SchedulingQueue, sortType SortType) {
    // TODO add latency metric
    switch sortType {
    case FairSortPolicy:
        sort.SliceStable(queues, func(i, j int) bool {
            l := queues[i]
            r := queues[j]
            comp := resources.CompUsageRatioSeparately(l.ProposingResource, l.CachedQueueInfo.GuaranteedResource,
                r.ProposingResource, r.CachedQueueInfo.GuaranteedResource)
            return comp < 0
        })
    }
}

func SortApplications(apps []*SchedulingApplication, sortType SortType, globalResource *resources.Resource) {
    // TODO add latency metric
    switch sortType {
    case FairSortPolicy:
        // Sort by usage
        sort.SliceStable(apps, func(i, j int) bool {
            l := apps[i]
            r := apps[j]
            return resources.CompUsageRatio(l.MayAllocatedResource, r.MayAllocatedResource, globalResource) < 0
        })
    case FifoSortPolicy:
        // Sort by submission time oldest first
        sort.SliceStable(apps, func(i, j int) bool {
            l := apps[i]
            r := apps[j]
            return l.ApplicationInfo.SubmissionTime < r.ApplicationInfo.SubmissionTime
        })
    }
}

func SortNodes(nodes []*SchedulingNode, sortType SortType) {
    sortingStart := time.Now()
    switch sortType {
    case MaxAvailableResources:
        // Sort by available resource, descending order
        sort.SliceStable(nodes, func(i, j int) bool {
            l := nodes[i]
            r := nodes[j]
            return resources.CompUsageShares(l.getAvailableResource(), r.getAvailableResource()) > 0
        })
    case MinAvailableResources:
        // Sort by available resource, ascending order
        sort.SliceStable(nodes, func(i, j int) bool {
            l := nodes[i]
            r := nodes[j]
            return resources.CompUsageShares(r.getAvailableResource(), l.getAvailableResource()) > 0
        })
    }
    metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)
}

// NodeSorter sorts a list of scheduling nodes based on the defined policy
type NodeSorter interface {
    // initialize node sorter with passing in specified sorting policy and a list of scheduling nodes
    Init(schedulingNodes []*SchedulingNode, sortType SortType)
    // returns sorted scheduling nodes based on specified sorting policy
    GetSortedSchedulingNodes() []*SchedulingNode
    // Update node snapshot and adjust its placement in tree
    UpdateNode(nodeId string) bool
}

// Node sorter based on slice
type SliceBasedNodeSorter struct {
    NodeSorter
    schedulingNodes []*SchedulingNode
    sortType SortType
}

func (sbns *SliceBasedNodeSorter) Init(schedulingNodes []*SchedulingNode, sortType SortType) {
    sbns.schedulingNodes = schedulingNodes
    sbns.sortType = sortType
}

func (sbns *SliceBasedNodeSorter) GetSortedSchedulingNodes() []*SchedulingNode {
    SortNodes(sbns.schedulingNodes, sbns.sortType)
    return sbns.schedulingNodes
}

func (sbns *SliceBasedNodeSorter) UpdateNode(nodeId string) bool {
    // do nothing for non-incremental sort policy
    return true
}

// Node sorter based on btree
type BtreeBasedNodeSorter struct {
    NodeSorter
    sortType SortType
    nodeTree *btree.BTree
    nodes    map[string]*SchedulingNodeSnapshot
}

// Snapshot of scheduling node used for sorting nodes based on btree,
// cachedAvailableResource is involved in the comparison between btree items,
// which should be unchangeable so that sorter can keep and manage a stable btree.
type SchedulingNodeSnapshot struct {
    schedulingNode          *SchedulingNode
    cachedAvailableResource *resources.Resource
}

// Comparing cachedAvailableResource and nodeId (to make sure all nodes are independent items in btree)
func (sns *SchedulingNodeSnapshot) Less(than btree.Item) bool {
    availSharesComp := resources.CompUsageShares(sns.cachedAvailableResource, than.(*SchedulingNodeSnapshot).cachedAvailableResource)
    if availSharesComp != 0 {
        return availSharesComp < 0
    }
    return strings.Compare(sns.schedulingNode.NodeId, than.(*SchedulingNodeSnapshot).schedulingNode.NodeId) > 0
}

func (bbns *BtreeBasedNodeSorter) Init(schedulingNodes []*SchedulingNode, sortType SortType) {
    bbns.sortType = sortType
    bbns.nodeTree = btree.New(32)
    bbns.nodes = make(map[string]*SchedulingNodeSnapshot)
    // build btree and snapshots for input scheduling nodes
    sortingStart := time.Now()
    for _, schedulingNode := range schedulingNodes {
        bbns.addSchedulingNode(schedulingNode)
    }
    metrics.GetSchedulerMetrics().ObserveNodeSortingLatency(sortingStart)
}

func (bbns *BtreeBasedNodeSorter) addSchedulingNode(schedulingNode *SchedulingNode) {
    nodeSnapshot := &SchedulingNodeSnapshot{
        schedulingNode:          schedulingNode,
        cachedAvailableResource: schedulingNode.getAvailableResource().Clone(),
    }
    bbns.nodeTree.ReplaceOrInsert(nodeSnapshot)
    bbns.nodes[schedulingNode.NodeId] = nodeSnapshot
}

func (bbns *BtreeBasedNodeSorter) UpdateNode(nodeId string) bool {
    if nodeSnapshot, ok := bbns.nodes[nodeId]; ok {
        deletedItem := bbns.nodeTree.Delete(nodeSnapshot)
        if deletedItem == nil {
            log.Logger().Warn("node not found in snapshot tree of node sorter",
                zap.String("nodeId", nodeId))
        } else {
            schedulingNode := deletedItem.(*SchedulingNodeSnapshot).schedulingNode
            bbns.addSchedulingNode(schedulingNode)
            return true
        }
    } else {
        log.Logger().Warn("node not found in snapshot cache of node sorter",
            zap.String("nodeId", nodeId))
    }
    return false
}

func (bbns *BtreeBasedNodeSorter) GetSortedSchedulingNodes() []*SchedulingNode {
    sortedSchedulingNodes := make([]*SchedulingNode, bbns.nodeTree.Len())
    var i = 0
    switch bbns.sortType {
    case MinAvailableResources:
        bbns.nodeTree.Ascend(func(item btree.Item) bool {
            sortedSchedulingNodes[i] = item.(*SchedulingNodeSnapshot).schedulingNode
            i++
            return true
        })
    case MaxAvailableResources:
        bbns.nodeTree.Descend(func(item btree.Item) bool {
            sortedSchedulingNodes[i] = item.(*SchedulingNodeSnapshot).schedulingNode
            i++
            return true
        })
    }
    return sortedSchedulingNodes
}
