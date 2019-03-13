package tests

import (
    "github.com/universal-scheduler/scheduler-spec/lib/go/si"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/cache"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/resources"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/common/strings"
    "github.com/universal-scheduler/yunikorn-scheduler/pkg/unityscheduler/scheduler"
    "testing"
    "time"
)

type MockRMCallbackHandler struct {
    t *testing.T

    acceptedJobs  map[string]bool
    acceptedNodes map[string]bool
    Allocations   map[string]*si.Allocation
}

func NewMockRMCallbackHandler(t *testing.T) *MockRMCallbackHandler {
    return &MockRMCallbackHandler{
        t:             t,
        acceptedJobs:  make(map[string]bool),
        acceptedNodes: make(map[string]bool),
        Allocations:   make(map[string]*si.Allocation),
    }
}

func (m *MockRMCallbackHandler) RecvUpdateResponse(response *si.UpdateResponse) error {
    m.t.Logf("---- Received Update=%s", strings.PrettyPrintStruct(response))

    for _, job := range response.AcceptedJobs {
        m.acceptedJobs[job.JobId] = true
    }

    for _, node := range response.AcceptedNodes {
        m.acceptedNodes[node.NodeId] = true
    }

    for _, alloc := range response.NewAllocations {
        m.Allocations[alloc.Uuid] = alloc
    }

    for _, alloc := range response.ReleasedAllocations {
        delete(m.Allocations, alloc.AllocationUUID)
    }

    return nil
}

func waitForAcceptedJobs(m *MockRMCallbackHandler, jobId string, timeoutMs int) {
    var i = 0
    for {
        i++
        if !m.acceptedJobs[jobId] {
            time.Sleep(time.Duration(100 * time.Millisecond))
        } else {
            return
        }
        if i*100 >= timeoutMs {
            m.t.Fatalf("Failed to wait AcceptedJobs.")
            return
        }
    }
}

func waitForAcceptedNodes(m *MockRMCallbackHandler, nodeId string, timeoutMs int) {
    var i = 0
    for {
        i++
        if !m.acceptedNodes[nodeId] {
            time.Sleep(time.Duration(100 * time.Millisecond))
        } else {
            return
        }
        if i*100 >= timeoutMs {
            m.t.Fatalf("Failed to wait AcceptedNode.")
            return
        }
    }
}

func waitForPendingResource(t *testing.T, queue *scheduler.SchedulingQueue, memory resources.Quantity, timeoutMs int) {
    var i = 0
    for {
        i++
        if queue.PendingResource.Resources[resources.MEMORY] != memory {
            time.Sleep(time.Duration(100 * time.Millisecond))
        } else {
            return
        }
        if i*100 >= timeoutMs {
            t.Fatalf("Failed to wait pending resource, actual = %v, expected = %v", queue.PendingResource.Resources[resources.MEMORY], memory)
            return
        }
    }
}

func waitForPendingResourceForJob(t *testing.T, job *scheduler.SchedulingJob, memory resources.Quantity, timeoutMs int) {
    var i = 0
    for {
        i++
        if job.Requests.TotalPendingResource.Resources[resources.MEMORY] != memory {
            time.Sleep(time.Duration(100 * time.Millisecond))
        } else {
            return
        }
        if i*100 >= timeoutMs {
            t.Fatalf("Failed to wait pending resource, expected=%v, actual=%v", memory, job.Requests.TotalPendingResource.Resources[resources.MEMORY])
            return
        }
    }
}

func waitForAllocations(m *MockRMCallbackHandler, nAlloc int, timeoutMs int) {
    var i = 0
    for {
        i++
        if len(m.Allocations) != nAlloc {
            time.Sleep(time.Duration(100 * time.Millisecond))
        } else {
            return
        }
        if i*100 >= timeoutMs {
            m.t.Fatalf("Failed to wait Allocations")
            return
        }
    }
}

func waitForNodesAllocatedResource(t *testing.T, cache *cache.ClusterInfo, partitionName string, nodeIds []string, allocatdMemory resources.Quantity, timeoutMs int) {
    var i = 0
    for {
        i++

        var totalNodeResource resources.Quantity = 0
        for _, nodeId := range nodeIds {
            totalNodeResource += cache.GetPartition(partitionName).GetNode(nodeId).AllocatedResource.Resources[resources.MEMORY]
        }

        if totalNodeResource != allocatdMemory {
            time.Sleep(time.Duration(100 * time.Millisecond))
        } else {
            return
        }
        if i*100 >= timeoutMs {
            t.Fatalf("Failed to wait Allocations")
            return
        }
    }
}
