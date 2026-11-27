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

package tests

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/api"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

//nolint:funlen
func benchmarkScheduling(b *testing.B, numNodes, numPods int) {
	log.UpdateLoggingConfig(map[string]string{"log.level": "WARN"})
	defer log.UpdateLoggingConfig(nil)

	// Start all tests
	serviceContext := entrypoint.StartAllServices()
	defer serviceContext.StopAll()
	proxy := serviceContext.RMProxy

	// Register RM
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
            resources:
              guaranteed:
                memory: 100000
                vcore: 10000
          - name: b
            resources:
              guaranteed:
                memory: 1000000
                vcore: 10000
`
	mockRM := newMockRMCallbackHandler()

	BuildInfoMap := make(map[string]string)
	BuildInfoMap["k"] = "v"

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
			BuildInfo:   BuildInfoMap,
			Config:      configData,
			ExtraConfig: map[string]string{
				"log.level": "WARN",
			},
		}, mockRM)

	assert.NilError(b, err, "RegisterResourceManager failed")

	// Add two apps and wait for them to be accepted
	err = proxy.UpdateApplication(&si.ApplicationRequest{
		New:  newAddAppRequest(map[string]string{appID1: "root.a", appID2: "root.b"}),
		RmID: "rm:123",
	})
	assert.NilError(b, err, "UpdateRequest application failed")
	mockRM.waitForAcceptedApplication(b, appID1, 1000)
	mockRM.waitForAcceptedApplication(b, appID2, 1000)

	// Calculate node resources to make sure all required pods can be allocated
	requestMem := 10
	requestVcore := 1
	numPodsPerNode := numPods/numNodes + 1
	nodeMem := requestMem * numPodsPerNode
	nodeVcore := requestVcore * numPodsPerNode

	// Register nodes
	var newNodes []*si.NodeInfo
	for i := range numNodes {
		nodeName := "node-" + strconv.Itoa(i)
		node := &si.NodeInfo{
			NodeID:     nodeName + ":1234",
			Attributes: map[string]string{},
			SchedulableResource: &si.Resource{
				Resources: map[string]*si.Quantity{
					"memory": {Value: int64(nodeMem)},
					"vcore":  {Value: int64(nodeVcore)},
				},
			},
			Action: si.NodeInfo_CREATE,
		}
		newNodes = append(newNodes, node)
	}
	err = proxy.UpdateNode(&si.NodeRequest{
		RmID:  "rm:123",
		Nodes: newNodes,
	})
	assert.NilError(b, err, "NodeRequest nodes failed")

	// Wait for all nodes to be accepted
	startTime := time.Now()
	mockRM.waitForMinAcceptedNodes(b, numNodes, 5000)
	duration1 := time.Since(startTime)
	b.Logf("Total time to add %d nodes in %s, %f nodes per second", numNodes, duration1, float64(numNodes)/duration1.Seconds())
	// Request pods
	app1NumPods := numPods / 2
	app1Asks := make([]*si.Allocation, app1NumPods)
	for i := range app1NumPods {
		app1Asks[i] = &si.Allocation{
			AllocationKey: fmt.Sprintf("alloc-1-%d", i),
			ResourcePerAlloc: &si.Resource{
				Resources: map[string]*si.Quantity{
					"memory": {Value: int64(requestMem)},
					"vcore":  {Value: int64(requestVcore)},
				},
			},
			ApplicationID: appID1,
		}
	}

	app2NumPods := numPods - app1NumPods
	app2Asks := make([]*si.Allocation, app2NumPods)
	for i := range app2NumPods {
		app2Asks[i] = &si.Allocation{
			AllocationKey: fmt.Sprintf("alloc-2-%d", i),
			ResourcePerAlloc: &si.Resource{
				Resources: map[string]*si.Quantity{
					"memory": {Value: int64(requestMem)},
					"vcore":  {Value: int64(requestVcore)},
				},
			},
			ApplicationID: appID2,
		}
	}

	b.ResetTimer()
	startTime = time.Now()
	err = proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: app1Asks,
		RmID:        "rm:123",
	})
	if err != nil {
		b.Error(err.Error())
	}
	err = proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: app2Asks,
		RmID:        "rm:123",
	})
	if err != nil {
		b.Error(err.Error())
	}
	// Wait for all pods to be allocated
	mockRM.waitForMinAllocations(b, numPods, 300000)
	// Stop timer and calculate duration
	b.StopTimer()
	duration2 := time.Since(startTime)
	b.Logf("Total time to add %d pods in %s, %f pods per second", numPods, duration2, float64(numPods)/duration2.Seconds())
	b.ReportAllocs()
	// ReportMetric must run after ResetTimer; ResetTimer clears any custom metrics reported before it.
	b.ReportMetric(float64(numNodes)/duration1.Seconds(), "nodes/s")
	b.ReportMetric(float64(numPods)/duration2.Seconds(), "pods/s")
}

func BenchmarkScheduling(b *testing.B) {
	tests := []struct{ numNodes, numPods int }{
		{numNodes: 500, numPods: 10000},
		{numNodes: 1000, numPods: 10000},
		{numNodes: 2000, numPods: 10000},
		{numNodes: 5000, numPods: 10000},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%vNodes/%vPods", test.numNodes, test.numPods)
		b.Run(name, func(b *testing.B) {
			if b.N > 1 {
				b.Skip("Single-run benchmark")
			}
			benchmarkScheduling(b, test.numNodes, test.numPods)
		})
	}
}

const (
	manyAppsNodes        = 1000
	manyAppsWorkPods     = 5000
	manyAppsBatch        = 1000
	manyAppsRequestMem   = 10
	manyAppsRequestVcore = 1
	// Node capacity is four times the pods of the largest grid point, so it never limits the rate.
	manyAppsPodsPerNode = 100

	shapeOneApp    = "oneApp"
	shapeAppPerPod = "appPerPod"
)

func manyAppsAsk(allocationKey, appID string) *si.Allocation {
	return &si.Allocation{
		AllocationKey: allocationKey,
		ResourcePerAlloc: &si.Resource{
			Resources: map[string]*si.Quantity{
				"memory": {Value: int64(manyAppsRequestMem)},
				"vcore":  {Value: int64(manyAppsRequestVcore)},
			},
		},
		ApplicationID: appID,
	}
}

// addManyAppsApplications waits for each application to be accepted: the core rejects asks for unknown applications.
func addManyAppsApplications(b *testing.B, proxy api.SchedulerAPI, mockRM *mockRMCallback, prefix string, count int) {
	for start := 0; start < count; start += manyAppsBatch {
		end := min(start+manyAppsBatch, count)
		apps := make(map[string]string, end-start)
		for i := start; i < end; i++ {
			apps[fmt.Sprintf("%s-%d", prefix, i)] = "root.a"
		}
		err := proxy.UpdateApplication(&si.ApplicationRequest{
			New:  newAddAppRequest(apps),
			RmID: "rm:123",
		})
		assert.NilError(b, err, "UpdateApplication failed")
		for i := start; i < end; i++ {
			mockRM.waitForAcceptedApplication(b, fmt.Sprintf("%s-%d", prefix, i), 60000)
		}
	}
}

// verifyIdleApps fails unless every idle application is in root.a, Running and with nothing pending.
func verifyIdleApps(b *testing.B, serviceContext *entrypoint.ServiceContext, numIdleApps, numWorkApps int) {
	queue := serviceContext.Scheduler.GetClusterContext().GetPartition(partition).GetQueue("root.a")
	if queue == nil {
		b.Fatal("queue root.a not found")
	}
	apps := queue.GetCopyOfApps()
	if len(apps) != numIdleApps+numWorkApps {
		b.Fatalf("queue root.a holds %d applications, expected %d", len(apps), numIdleApps+numWorkApps)
	}
	for i := range numIdleApps {
		appID := fmt.Sprintf("idle-%d", i)
		app := apps[appID]
		if app == nil {
			b.Fatalf("application %s is not in queue root.a", appID)
		}
		if app.CurrentState() != objects.Running.String() {
			b.Fatalf("application %s is in state %s, expected %s", appID, app.CurrentState(), objects.Running.String())
		}
		if !resources.IsZero(app.GetPendingResource()) {
			b.Fatalf("application %s has pending resource %s, expected zero", appID, app.GetPendingResource())
		}
	}
}

//nolint:funlen
func benchmarkSchedulingManyApps(b *testing.B, numIdleApps int, shape string) {
	log.UpdateLoggingConfig(map[string]string{"log.level": "WARN"})
	defer log.UpdateLoggingConfig(nil)

	serviceContext := entrypoint.StartAllServices()
	defer serviceContext.StopAll()
	proxy := serviceContext.RMProxy

	// Idle and work applications share one unlimited leaf queue: the per-cycle walk over its applications is the measured cost.
	configData := `
partitions:
  -
    name: default
    queues:
      - name: root
        submitacl: "*"
        queues:
          - name: a
`
	mockRM := newMockRMCallbackHandler()

	BuildInfoMap := make(map[string]string)
	BuildInfoMap["k"] = "v"

	_, err := proxy.RegisterResourceManager(
		&si.RegisterResourceManagerRequest{
			RmID:        "rm:123",
			PolicyGroup: "policygroup",
			Version:     "0.0.2",
			BuildInfo:   BuildInfoMap,
			Config:      configData,
			ExtraConfig: map[string]string{
				"log.level": "WARN",
			},
		}, mockRM)
	assert.NilError(b, err, "RegisterResourceManager failed")

	nodeMem := manyAppsRequestMem * manyAppsPodsPerNode
	nodeVcore := manyAppsRequestVcore * manyAppsPodsPerNode
	newNodes := make([]*si.NodeInfo, manyAppsNodes)
	for i := range manyAppsNodes {
		newNodes[i] = &si.NodeInfo{
			NodeID:     "node-" + strconv.Itoa(i) + ":1234",
			Attributes: map[string]string{},
			SchedulableResource: &si.Resource{
				Resources: map[string]*si.Quantity{
					"memory": {Value: int64(nodeMem)},
					"vcore":  {Value: int64(nodeVcore)},
				},
			},
			Action: si.NodeInfo_CREATE,
		}
	}
	err = proxy.UpdateNode(&si.NodeRequest{
		RmID:  "rm:123",
		Nodes: newNodes,
	})
	assert.NilError(b, err, "NodeRequest nodes failed")
	mockRM.waitForMinAcceptedNodes(b, manyAppsNodes, 60000)

	// Idle allocations are never released, which keeps those applications Running with nothing pending.
	addManyAppsApplications(b, proxy, mockRM, "idle", numIdleApps)
	for start := 0; start < numIdleApps; start += manyAppsBatch {
		end := min(start+manyAppsBatch, numIdleApps)
		asks := make([]*si.Allocation, 0, end-start)
		for i := start; i < end; i++ {
			asks = append(asks, manyAppsAsk(fmt.Sprintf("idle-alloc-%d", i), fmt.Sprintf("idle-%d", i)))
		}
		err = proxy.UpdateAllocation(&si.AllocationRequest{
			Allocations: asks,
			RmID:        "rm:123",
		})
		assert.NilError(b, err, "AllocationRequest idle asks failed")
	}
	mockRM.waitForMinAllocations(b, numIdleApps, 600000)

	numWorkApps := 1
	if shape == shapeAppPerPod {
		numWorkApps = manyAppsWorkPods
	}
	addManyAppsApplications(b, proxy, mockRM, "work", numWorkApps)

	workAsks := make([]*si.Allocation, manyAppsWorkPods)
	for i := range manyAppsWorkPods {
		appID := "work-0"
		if shape == shapeAppPerPod {
			appID = fmt.Sprintf("work-%d", i)
		}
		workAsks[i] = manyAppsAsk(fmt.Sprintf("work-alloc-%d", i), appID)
	}

	verifyIdleApps(b, serviceContext, numIdleApps, numWorkApps)

	b.ResetTimer()
	startTime := time.Now()
	err = proxy.UpdateAllocation(&si.AllocationRequest{
		Allocations: workAsks,
		RmID:        "rm:123",
	})
	if err != nil {
		b.Error(err.Error())
	}
	mockRM.waitForMinAllocations(b, numIdleApps+manyAppsWorkPods, 900000)
	b.StopTimer()

	duration := time.Since(startTime)
	rate := float64(manyAppsWorkPods) / duration.Seconds()
	b.Logf("idle=%d shape=%s pods=%d duration=%s pods/s=%f", numIdleApps, shape, manyAppsWorkPods, duration, rate)
	b.ReportAllocs()
	// ReportMetric must run after ResetTimer; ResetTimer clears any custom metrics reported before it.
	b.ReportMetric(rate, "pods/s")
}

func BenchmarkSchedulingManyApps(b *testing.B) {
	for _, numIdleApps := range []int{0, 1000, 5000, 20000} {
		for _, shape := range []string{shapeOneApp, shapeAppPerPod} {
			name := fmt.Sprintf("idle=%d/shape=%s", numIdleApps, shape)
			b.Run(name, func(b *testing.B) {
				if b.N > 1 {
					b.Skip("Single-run benchmark")
				}
				benchmarkSchedulingManyApps(b, numIdleApps, shape)
			})
		}
	}
}
