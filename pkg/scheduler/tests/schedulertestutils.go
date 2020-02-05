/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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

package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type MockRMCallbackHandler struct {
	t testing.TB

	acceptedApplications map[string]bool
	rejectedApplications map[string]bool
	acceptedNodes        map[string]bool
	rejectedNodes        map[string]bool
	nodeAllocations      map[string][]*si.Allocation
	Allocations          map[string]*si.Allocation

	lock sync.RWMutex
}

func NewMockRMCallbackHandler(t testing.TB) *MockRMCallbackHandler {
	return &MockRMCallbackHandler{
		t:                    t,
		acceptedApplications: make(map[string]bool),
		rejectedApplications: make(map[string]bool),
		acceptedNodes:        make(map[string]bool),
		rejectedNodes:        make(map[string]bool),
		nodeAllocations:      make(map[string][]*si.Allocation),
		Allocations:          make(map[string]*si.Allocation),
	}
}

func (m *MockRMCallbackHandler) EvalPredicates(name string, node string) error {
	return nil
}

func (m *MockRMCallbackHandler) RecvUpdateResponse(response *si.UpdateResponse) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// m.t.Logf("---- Received Update=%s", strings.PrettyPrintStruct(response))

	for _, app := range response.AcceptedApplications {
		m.acceptedApplications[app.ApplicationID] = true
	}

	for _, app := range response.RejectedApplications {
		m.rejectedApplications[app.ApplicationID] = true
	}

	for _, node := range response.AcceptedNodes {
		m.acceptedNodes[node.NodeID] = true
	}

	for _, node := range response.RejectedNodes {
		m.rejectedNodes[node.NodeID] = true
	}

	for _, alloc := range response.NewAllocations {
		m.Allocations[alloc.UUID] = alloc
		if val, ok := m.nodeAllocations[alloc.NodeID]; ok {
			val = append(val, alloc)
			m.nodeAllocations[alloc.NodeID] = val
		} else {
			nodeAllocations := make([]*si.Allocation, 0)
			nodeAllocations = append(nodeAllocations, alloc)
			m.nodeAllocations[alloc.NodeID] = nodeAllocations
		}
	}

	for _, alloc := range response.ReleasedAllocations {
		delete(m.Allocations, alloc.UUID)
	}

	return nil
}

func (m *MockRMCallbackHandler) getAllocations() map[string]*si.Allocation {
	m.lock.RLock()
	defer m.lock.RUnlock()

	allocations := make(map[string]*si.Allocation)
	for key, value := range m.Allocations {
		allocations[key] = value
	}
	return allocations
}

func waitForAcceptedApplications(m *MockRMCallbackHandler, appID string, timeoutMs int) {
	var i = 0
	for {
		i++

		m.lock.RLock()
		accepted := m.acceptedApplications[appID]
		m.lock.RUnlock()

		if !accepted {
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
		if i*100 >= timeoutMs {
			m.t.Fatalf("Failed to wait AcceptedApplications: %s", appID)
			return
		}
	}
}

func waitForRejectedApplications(m *MockRMCallbackHandler, appID string, timeoutMs int) {
	var i = 0
	for {
		i++

		m.lock.RLock()
		wait := !m.rejectedApplications[appID] || m.acceptedApplications[appID]
		m.lock.RUnlock()

		if wait {
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
		if i*100 >= timeoutMs {
			m.t.Fatalf("Failed to wait RejectedApplications: %s", appID)
			return
		}
	}
}

func waitForAcceptedNodes(m *MockRMCallbackHandler, nodeID string, timeoutMs int) {
	var i = 0
	for {
		i++

		m.lock.RLock()
		accepted := m.acceptedNodes[nodeID]
		m.lock.RUnlock()

		if !accepted {
			time.Sleep(time.Duration(1 * time.Millisecond))
		} else {
			return
		}
		if i >= timeoutMs {
			m.t.Fatalf("Failed to wait AcceptedNode: %s", nodeID)
			return
		}
	}
}

func waitForMinNumberOfAcceptedNodes(m *MockRMCallbackHandler, minNumNode int, timeoutMs int) {
	var i = 0
	for {
		i++

		m.lock.RLock()
		accepted := len(m.acceptedNodes)
		m.lock.RUnlock()

		if accepted < minNumNode {
			time.Sleep(time.Duration(1 * time.Millisecond))
		} else {
			return
		}
		if i >= timeoutMs {
			m.t.Fatalf("Failed to wait #AcceptedNode=%d", minNumNode)
			return
		}
	}
}

func waitForRejectedNodes(m *MockRMCallbackHandler, nodeID string, timeoutMs int) {
	var i = 0
	for {
		i++

		m.lock.RLock()
		accepted := m.rejectedNodes[nodeID]
		m.lock.RUnlock()

		if !accepted {
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
		if i*100 >= timeoutMs {
			m.t.Fatalf("Failed to wait AcceptedNode: %s", nodeID)
			return
		}
	}
}

func waitForPendingResource(t *testing.T, queue *scheduler.SchedulingQueue, memory resources.Quantity, timeoutMs int) {
	var i = 0
	for {
		i++
		if queue.GetPendingResource().Resources[resources.MEMORY] != memory {
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
		if i*100 >= timeoutMs {
			log.Logger().Info("queue detail",
				zap.Any("queue", queue))
			t.Fatalf("Failed to wait pending resource on queue %s, actual = %v, expected = %v", queue.Name, queue.GetPendingResource().Resources[resources.MEMORY], memory)
			return
		}
	}
}

func waitForPendingResourceForApplication(t *testing.T, app *scheduler.SchedulingApplication, memory resources.Quantity, timeoutMs int) {
	var i = 0
	for {
		i++
		if app.Requests.GetPendingResource().Resources[resources.MEMORY] != memory {
			time.Sleep(time.Duration(100 * time.Millisecond))
		} else {
			return
		}
		if i*100 >= timeoutMs {
			t.Fatalf("Failed to wait pending resource, expected=%v, actual=%v", memory, app.Requests.GetPendingResource().Resources[resources.MEMORY])
			return
		}
	}
}

func waitForAllocations(m *MockRMCallbackHandler, nAlloc int, timeoutMs int) {
	var i = 0
	for {
		i++
		m.lock.RLock()
		allocLen := len(m.Allocations)
		m.lock.RUnlock()

		if allocLen != nAlloc {
			time.Sleep(100 * time.Millisecond)
		} else {
			return
		}
		if i*100 >= timeoutMs {
			m.t.Fatalf("Failed to wait Allocations expected %d, got %d", nAlloc, allocLen)
			return
		}
	}
}

func waitForMinAllocations(m *MockRMCallbackHandler, nAlloc int, timeoutMs int) {
	var i = 0
	for {
		i++
		m.lock.RLock()
		allocLen := len(m.Allocations)
		m.lock.RUnlock()

		if allocLen < nAlloc {
			time.Sleep(time.Millisecond)
		} else {
			return
		}
		if i >= timeoutMs {
			m.t.Fatalf("Failed to wait Allocations expected %d, got %d", nAlloc, allocLen)
			return
		}
	}
}

func waitForNodesAllocatedResource(t *testing.T, cache *cache.ClusterInfo, partitionName string, nodeIDs []string, allocatdMemory resources.Quantity, timeoutMs int) {
	var i = 0
	for {
		i++

		var totalNodeResource resources.Quantity = 0
		for _, nodeID := range nodeIDs {
			totalNodeResource += cache.GetPartition(partitionName).GetNode(nodeID).GetAllocatedResource().Resources[resources.MEMORY]
		}

		if totalNodeResource != allocatdMemory {
			time.Sleep(100 * time.Millisecond)
		} else {
			return
		}
		if i*100 >= timeoutMs {
			t.Fatalf("Failed to wait Allocations on partition %s and node %v", partitionName, nodeIDs)
			return
		}
	}
}

func waitForNewSchedulerNode(t *testing.T, context *scheduler.ClusterSchedulingContext, nodeID string, partitionName string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		node := context.GetSchedulingNode(nodeID, partitionName)
		return node != nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for new scheduling node on partition %s, node %v", partitionName, nodeID)
	}
}

func waitForRemovedSchedulerNode(t *testing.T, context *scheduler.ClusterSchedulingContext, nodeID string, partitionName string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		node := context.GetSchedulingNode(nodeID, partitionName)
		return node == nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for removal of scheduling node on partition %s, node %v", partitionName, nodeID)
	}
}

func getApplicationInfoFromPartition(partitionInfo *cache.PartitionInfo, appID string) (*cache.ApplicationInfo, error) {
	for _, appInfo := range partitionInfo.GetApplications() {
		if appInfo.ApplicationID == appID {
			return appInfo, nil
		}
	}
	return nil, fmt.Errorf("cannot find app %s from cache", appID)
}

func newAddAppRequest(apps map[string]string) []*si.AddApplicationRequest {
	var requests []*si.AddApplicationRequest
	for app, queue := range apps {
		request := si.AddApplicationRequest{
			ApplicationID: app,
			QueueName:     queue,
			PartitionName: "",
			Ugi: &si.UserGroupInformation{
				User: "testuser",
			},
		}
		requests = append(requests, &request)
	}
	return requests
}
