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

package scheduler

import (
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/resources"
    "math"
)

// Temp object for better readability.
type queuePreemptCalcResourceByType struct {
    guaranteed          resources.Quantity
    normalizedGuarantee float64
    used                resources.Quantity
    pending             resources.Quantity
    max                 resources.Quantity
    ideal               resources.Quantity
    // Point to queue's resource to be updated.
    resources *queuePreemptCalcResource
}

func calculateIdealResources(scheduler *Scheduler) {
    // for each partition
    for partitionName, preemptionPartitionContext := range scheduler.preemptionContext.partitions {
        calculateIdealResourcesForPartition(scheduler, partitionName, preemptionPartitionContext)
    }
}

func calculateIdealResourcesForPartition(scheduler *Scheduler, partitionName string, preemptionPartitionCtx *preemptionPartitionContext) {
    // Sum of all node's total resource under the partition
    partitionTotal := scheduler.clusterInfo.GetPartition(partitionName).GetTotalPartitionResource()
    preemptionPartitionCtx.partitionTotalResource = partitionTotal

    // Init queue resources from scheduler
    root := preemptionPartitionCtx.root
    recursiveInitResources(root)

    // Init root queue's ideal allocation
    root.resources.ideal = partitionTotal

    // For each resource type, calculate ideal resource
    for resourceType := range partitionTotal.Resources {
        // Set children's ideal resources
        setIdealResourceForDirectChildren(resourceType, partitionTotal.Resources[resourceType], root.children)
    }

    // Set preemptable resource for each queue recursively.
    recursiveUpdatePreemptableResources(partitionTotal, root)
}

func recursiveInitResources(ctx *preemptionQueueContext) {
    ctx.resources = newQueuePreemptCalcResource()
    ctx.resources.initFromSchedulingQueue(ctx.schedulingQueue)

    for _, preemptQueue := range ctx.children {
        recursiveInitResources(preemptQueue)
    }
}

func recursiveUpdatePreemptableResources(partitionResource *resources.Resource, queue *preemptionQueueContext) {
    // There's a deadzone, when a queue used more than 10% of its guaranteed resource, preemption will started.
    // TODO: Make the ratio configurable
    if resources.FairnessRatio(queue.resources.used, partitionResource, queue.resources.guaranteed, partitionResource) > 1.1 {
        // Preemptable resource = used - guarantee of each queue
        queue.resources.preemptable = resources.ComponentWiseMax(resources.Sub(queue.resources.used, queue.resources.guaranteed), resources.Zero)
    }

    for _, preemptQueue := range queue.children {
        recursiveUpdatePreemptableResources(partitionResource, preemptQueue)
    }
}

func newQueuePreemptCalcResourceByType(resourceType string, queueResource *queuePreemptCalcResource) *queuePreemptCalcResourceByType {
    ret := &queuePreemptCalcResourceByType{}

    ret.guaranteed = queueResource.guaranteed.Resources[resourceType]
    ret.used = queueResource.used.Resources[resourceType]
    ret.pending = queueResource.pending.Resources[resourceType]
    ret.max = queueResource.max.Resources[resourceType]

    ret.resources = queueResource

    return ret
}

func setIdealResourceForDirectChildren(resourceType string, parentTotal resources.Quantity, childrenPreemptionContext map[string]*preemptionQueueContext) {
    // Two iterates, first assign resources to non-empty guaranteed queues.
    totalGuaranteed := resources.Quantity(0)

    nonZeroGuaranteedQueues := make(map[string]*queuePreemptCalcResourceByType)
    zeroGuaranteedQueues := make(map[string]*queuePreemptCalcResourceByType)

    for queueName, queue := range childrenPreemptionContext {
        queueCalcRes := newQueuePreemptCalcResourceByType(resourceType, queue.resources)
        totalGuaranteed += queueCalcRes.guaranteed

        if queueCalcRes.guaranteed > 0 {
            nonZeroGuaranteedQueues[queueName] = queueCalcRes
        } else {
            zeroGuaranteedQueues[queueName] = queueCalcRes
        }
    }

    // calculate normalized guaranteed resources
    // For queue which guaranteed resource > 0, it is proportion to its guaranteed
    for _, queue := range nonZeroGuaranteedQueues {
        queue.normalizedGuarantee = float64(queue.guaranteed) / float64(totalGuaranteed)
    }
    // For queue which guaranteed resource == 0, it is same as other non-zero guaranteed queues
    for _, queue := range zeroGuaranteedQueues {
        queue.normalizedGuarantee = 1 / float64(len(zeroGuaranteedQueues))
    }

    // Then calculate ideal allocation
    totalAvailable := parentTotal
    if totalAvailable > 0 {
        totalAvailable -= calculateIdealAllocation(resourceType, totalAvailable, nonZeroGuaranteedQueues)
    }
    if totalAvailable > 0 {
        calculateIdealAllocation(resourceType, totalAvailable, zeroGuaranteedQueues)
    }

    // For each children, set ideal for its children
    for _, child := range childrenPreemptionContext {
        // Set ideal allocation of the given resourceType
        setIdealResourceForDirectChildren(resourceType, child.resources.ideal.Resources[resourceType], child.children)
    }
}

// Returns allocated resources
func calculateIdealAllocation(resourceType string, totalAvailable resources.Quantity, queueResources map[string]*queuePreemptCalcResourceByType) resources.Quantity {
    satisfiedQueues := make(map[string]bool)
    totalAllocated := resources.Quantity(0)

    // Reset ideal allocation to zero
    for _, queueCalc := range queueResources {
        queueCalc.ideal = 0
        queueCalc.resources.ideal.Resources[resourceType] = 0
    }

    for len(queueResources) > 0 && totalAvailable > 0 {
        for queue, queueCalc := range queueResources {
            // Ignore satisfied queues
            if satisfiedQueues[queue] {
                continue
            }

            // How much resource we can give to this queue this round.
            totalAvailableForQueue := resources.Quantity(math.Ceil(float64(totalAvailable) * queueCalc.normalizedGuarantee))

            // How much queue will accept
            // It equals min(total-available, min(pending + used, max) - ideal)
            accepted := resources.MinQuantity(totalAvailableForQueue, resources.MinQuantity(queueCalc.pending+queueCalc.used, queueCalc.max)-queueCalc.ideal)

            if accepted > 0 {
                queueCalc.ideal += accepted
                queueCalc.resources.ideal.Resources[resourceType] += accepted
                totalAvailable -= accepted
                totalAllocated += accepted
            } else {
                satisfiedQueues[queue] = true
            }
        }

        // Recompute normalized guarantees
        totalGuarantees := 0.0
        for queue, queueCalc := range queueResources {
            if satisfiedQueues[queue] {
                continue
            }
            totalGuarantees += queueCalc.normalizedGuarantee
        }
        for queue, queueCalc := range queueResources {
            if satisfiedQueues[queue] {
                continue
            }
            queueCalc.normalizedGuarantee = queueCalc.normalizedGuarantee / totalGuarantees
        }
    }

    // Calculate ideal resource for given resource type
    for _, queueCalc := range queueResources {
        queueCalc.resources.ideal.Resources[resourceType] = queueCalc.ideal
    }

    return totalAllocated
}
