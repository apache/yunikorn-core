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

	"go.uber.org/zap"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/entrypoint"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func benchmarkScheduling(b *testing.B, numNodes, numPods int) {
	log.InitAndSetLevel(zap.InfoLevel)
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
	for i := 0; i < numNodes; i++ {
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
	duration := time.Since(startTime)
	b.Logf("Total time to add %d node in %s, %f per second", numNodes, duration, float64(numNodes)/duration.Seconds())

	// Request pods
	app1NumPods := numPods / 2
	err = proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: int64(requestMem)},
						"vcore":  {Value: int64(requestVcore)},
					},
				},
				MaxAllocations: int32(app1NumPods),
				ApplicationID:  appID1,
			},
		},
		RmID: "rm:123",
	})
	if err != nil {
		b.Error(err.Error())
	}

	err = proxy.UpdateAllocation(&si.AllocationRequest{
		Asks: []*si.AllocationAsk{
			{
				AllocationKey: "alloc-1",
				ResourceAsk: &si.Resource{
					Resources: map[string]*si.Quantity{
						"memory": {Value: int64(requestMem)},
						"vcore":  {Value: int64(requestVcore)},
					},
				},
				MaxAllocations: int32(numPods - app1NumPods),
				ApplicationID:  appID2,
			},
		},
		RmID: "rm:123",
	})
	if err != nil {
		b.Error(err.Error())
	}

	// Reset  timer for this benchmark
	startTime = time.Now()
	b.ResetTimer()

	// Wait for all pods to be allocated
	mockRM.waitForMinAllocations(b, numPods, 300000)

	// Stop timer and calculate duration
	b.StopTimer()
	duration = time.Since(startTime)

	b.Logf("Total time to allocate %d containers in %s, %f per second", numPods, duration, float64(numPods)/duration.Seconds())
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
			benchmarkScheduling(b, test.numNodes, test.numPods)
		})
	}
}
