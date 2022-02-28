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
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	siCommon "github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const pName = "default"

type mockEventHandler struct {
	eventHandled  bool
	rejectedNodes []*si.RejectedNode
	acceptedNodes []*si.AcceptedNode
}

func newMockEventHandler() *mockEventHandler {
	return &mockEventHandler{
		eventHandled:  false,
		rejectedNodes: make([]*si.RejectedNode, 0),
		acceptedNodes: make([]*si.AcceptedNode, 0),
	}
}

func (m *mockEventHandler) reset() {
	m.eventHandled = false
	m.rejectedNodes = make([]*si.RejectedNode, 0)
	m.acceptedNodes = make([]*si.AcceptedNode, 0)
}

func (m *mockEventHandler) HandleEvent(ev interface{}) {
	if nodeEvent, ok := ev.(*rmevent.RMNodeUpdateEvent); ok {
		m.eventHandled = true
		m.rejectedNodes = append(m.rejectedNodes, nodeEvent.RejectedNodes...)
		m.acceptedNodes = append(m.acceptedNodes, nodeEvent.AcceptedNodes...)
	}
}

func createTestContext(t *testing.T, partitionName string) *ClusterContext {
	handler := newMockEventHandler()
	context := &ClusterContext{
		partitions:     map[string]*PartitionContext{},
		rmEventHandler: handler,
	}
	conf := configs.PartitionConfig{
		Name: partitionName,
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
	}
	partition, err := newPartitionContext(conf, "test", context)
	assert.NilError(t, err, "partition create should not have failed with error")
	context.partitions[partition.Name] = partition
	return context
}

func TestAddUnlimitedNode(t *testing.T) {
	context := createTestContext(t, pName)

	unlimitedNode := &si.NodeInfo{
		NodeID:     "unlimited",
		Attributes: map[string]string{"yunikorn.apache.org/nodeType": "unlimited", "si/node-partition": pName},
		Action:     si.NodeInfo_CREATE,
	}
	var newNodes []*si.NodeInfo
	newNodes = append(newNodes, unlimitedNode)

	request := &si.NodeRequest{
		Nodes: newNodes,
	}

	handler, ok := context.rmEventHandler.(*mockEventHandler)
	assert.Assert(t, ok, "incorrect handler set in context")

	// 1. add the first unlimited node, but keep reservation enabled
	context.processNodes(request)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 0, "There should be no accepted nodes")
	assert.Assert(t, len(handler.rejectedNodes) == 1, "The node should be rejected")
	assert.Equal(t, handler.rejectedNodes[0].NodeID, unlimitedNode.NodeID, "The rejected node is not the unlimited one")

	// 2. add the first unlimited node, but disable reservation
	context.reservationDisabled = true
	handler.reset()
	context.processNodes(request)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 1, "The node should be accepted")
	assert.Assert(t, len(handler.rejectedNodes) == 0, "There should be no rejected nodes")
	assert.Equal(t, handler.acceptedNodes[0].NodeID, unlimitedNode.NodeID, "The accepted node is not the unlimited one")

	// 3. there is already an unlimited node registered
	unlimitedNode2 := &si.NodeInfo{
		NodeID:     "unlimited2",
		Attributes: map[string]string{"yunikorn.apache.org/nodeType": "unlimited", "si/node-partition": pName},
		Action:     si.NodeInfo_CREATE,
	}
	var newNodes2 []*si.NodeInfo
	newNodes2 = append(newNodes2, unlimitedNode2)
	request2 := &si.NodeRequest{
		Nodes: newNodes2,
	}
	handler.reset()
	context.processNodes(request2)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 0, "There should be no accepted nodes")
	assert.Assert(t, len(handler.rejectedNodes) == 1, "The node should be rejected")
	assert.Equal(t, handler.rejectedNodes[0].NodeID, unlimitedNode2.NodeID, "The rejected node is not the unlimited one")

	// 4. there are other nodes in the list as well
	regularNode := &si.NodeInfo{
		NodeID:     "regularNode",
		Attributes: map[string]string{"si/node-partition": pName},
		Action:     si.NodeInfo_CREATE,
	}
	newNodes2 = append(newNodes2, unlimitedNode, regularNode)
	request2.Nodes = newNodes2
	handler.reset()
	partition := context.GetPartition(pName)
	if partition == nil {
		t.Fatalf("partition should have been found")
	}
	_, _ = partition.removeNode(unlimitedNode.NodeID)
	context.processNodes(request2)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 1, "There should be only one accepted node")
	assert.Assert(t, len(handler.rejectedNodes) == 2, "There should be 2 rejected nodes")
	assert.Equal(t, handler.acceptedNodes[0].NodeID, regularNode.NodeID, "The accepted node is not the regular one")

	// 5. there is a regular node registered
	handler.reset()
	context.processNodes(request)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 0, "There should be no accepted node")
	assert.Assert(t, len(handler.rejectedNodes) == 1, "The node should be rejected")
	assert.Equal(t, handler.rejectedNodes[0].NodeID, unlimitedNode.NodeID, "The rejected node is not the unlimited one")
}

func TestContext_UpdateNode(t *testing.T) {
	context := createTestContext(t, pName)
	n := &si.NodeInfo{
		NodeID:              "test-1",
		Action:              si.NodeInfo_UNKNOWN_ACTION_FROM_RM,
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 10}}},
	}
	full := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 5})
	partition := context.GetPartition(pName)
	if partition == nil {
		t.Fatalf("partition should have been found")
	}
	// no partition set
	context.updateNode(n)
	assert.Equal(t, 0, len(partition.GetNodes()), "unexpected node found on partition (no partition set)")
	n.Attributes = map[string]string{"si/node-partition": "unknown"}
	context.updateNode(n)
	assert.Equal(t, 0, len(partition.GetNodes()), "unexpected node found on partition (unknown partition set)")
	n.Attributes = map[string]string{"si/node-partition": pName}
	context.updateNode(n)
	assert.Equal(t, 0, len(partition.GetNodes()), "unexpected node found on partition (correct partition set)")
	// add the node to the context
	err := context.addNode(n, 1)
	assert.NilError(t, err, "unexpected error returned from addNode")
	assert.Equal(t, 1, len(partition.GetNodes()), "expected node not found on partition")
	assert.Assert(t, resources.Equals(full, partition.GetTotalPartitionResource()), "partition resource should be updated")
	// try to update: fail due to unknown action
	n.SchedulableResource = &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 5}}}
	n.OccupiedResource = &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 5}}}
	context.updateNode(n)
	node := partition.GetNode("test-1")
	assert.Assert(t, resources.Equals(full, node.GetAvailableResource()), "node available resource should not be updated")
	n.Action = si.NodeInfo_UPDATE
	context.updateNode(n)
	assert.Assert(t, resources.Equals(half, partition.GetTotalPartitionResource()), "partition resource should be updated")
	assert.Assert(t, resources.IsZero(node.GetAvailableResource()), "node available should have been updated to zero")
	assert.Assert(t, resources.Equals(half, node.GetOccupiedResource()), "node occupied should have been updated")

	// other actions
	n = &si.NodeInfo{
		NodeID:     "test-1",
		Action:     si.NodeInfo_DRAIN_NODE,
		Attributes: map[string]string{"si/node-partition": pName},
	}
	context.updateNode(n)
	assert.Assert(t, !node.IsSchedulable(), "node should be marked as unschedulable")
	n.Action = si.NodeInfo_DRAIN_TO_SCHEDULABLE
	context.updateNode(n)
	assert.Assert(t, node.IsSchedulable(), "node should be marked as schedulable")
	n.Action = si.NodeInfo_DECOMISSION
	context.updateNode(n)
	assert.Equal(t, 0, len(partition.GetNodes()), "unexpected node found on partition node should have been removed")
	assert.Assert(t, resources.IsZero(partition.GetTotalPartitionResource()), "partition resource should be zero")
	assert.Assert(t, !node.IsSchedulable(), "node should be marked as unschedulable")
}

func TestContext_AddNode(t *testing.T) {
	context := createTestContext(t, pName)

	n := &si.NodeInfo{
		NodeID:              "test-1",
		Action:              si.NodeInfo_CREATE,
		Attributes:          map[string]string{"si/node-partition": "unknown"},
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 10}}},
	}
	err := context.addNode(n, 1)
	if err == nil {
		t.Fatalf("unknown node partition should have failed the node add")
	}
	n.Attributes = map[string]string{"si/node-partition": pName}
	err = context.addNode(n, 1)
	assert.NilError(t, err, "unexpected error returned adding node")
	partition := context.GetPartition(pName)
	if partition == nil {
		t.Fatalf("partition should have been found")
	}
	assert.Equal(t, 1, len(partition.GetNodes()), "expected node not found on partition")
	// add same node again should show an error
	err = context.addNode(n, 1)
	if err == nil {
		t.Fatalf("existing node addition should have failed")
	}
}

func TestContext_AddRMBuildInformation(t *testing.T) {
	context := createTestContext(t, pName)

	rmID1 := "myCluster1"
	buildInfoMap1 := make(map[string]string)
	buildInfoMap1["buildDate"] = "2006-01-02T15:04:05-0700"
	buildInfoMap1["buildVersion"] = "latest"
	buildInfoMap1["isPluginVersion"] = "false"
	rm1 := &si.RegisterResourceManagerRequest{
		RmID:        rmID1,
		PolicyGroup: "policygroup",
		Version:     "0.0.2",
		BuildInfo:   buildInfoMap1,
	}

	rmID2 := "myCluster2"
	buildInfoMap2 := make(map[string]string)
	buildInfoMap2["buildDate"] = "2022-01-02T15:04:05-0700"
	buildInfoMap2["buildVersion"] = "latest"
	buildInfoMap2["isPluginVersion"] = "true"
	rm2 := &si.RegisterResourceManagerRequest{
		RmID:        rmID2,
		PolicyGroup: "policygroup",
		Version:     "0.0.2",
		BuildInfo:   buildInfoMap2,
	}

	context.SetRMInfos(rmID1, rm1.BuildInfo)
	assert.Equal(t, 1, len(context.rmInfos))
	assert.DeepEqual(t, context.rmInfos[rmID1].RMBuildInformation, rm1.BuildInfo)
	context.rmInfos[rmID1].RMBuildInformation["rmId"] = rmID1
	assert.Equal(t, context.rmInfos[rmID1].RMBuildInformation["rmId"], rm1.RmID)

	context.SetRMInfos(rmID2, rm2.BuildInfo)
	assert.Equal(t, 2, len(context.rmInfos))
	assert.DeepEqual(t, context.rmInfos[rmID2].RMBuildInformation, rm2.BuildInfo)
	context.rmInfos[rmID2].RMBuildInformation["rmId"] = rmID2
	assert.Equal(t, context.rmInfos[rmID2].RMBuildInformation["rmId"], rm2.RmID)

	//update a value,make sure it is replaced as expected
	buildInfoMap2["buildVersion"] = "1.0.0"
	context.SetRMInfos(rmID2, buildInfoMap2)
	assert.Equal(t, context.rmInfos[rmID2].RMBuildInformation["buildVersion"], buildInfoMap2["buildVersion"])
}

func TestContext_ProcessNode(t *testing.T) {
	// make sure we're nil safe IDE will complain about the non nil check
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil node info in processNodes test")
		}
	}()
	context := createTestContext(t, pName)

	request := &si.NodeRequest{
		Nodes: []*si.NodeInfo{
			nil,
			{NodeID: "test-1", Action: si.NodeInfo_CREATE, Attributes: map[string]string{"si/node-partition": pName}},
		},
	}
	context.processNodes(request)
	node := context.GetNode("test-1", pName)
	if node == nil {
		t.Fatalf("expected node to be added skipping nil node in list")
	}
}

func TestContextUpdateNodeMetrics(t *testing.T) {
	metrics.GetSchedulerMetrics().Reset()
	context := createTestContext(t, pName)

	n := getNodeInfoForAddingNode(true)

	err := context.addNode(n, 1)
	assert.NilError(t, err, "unexpected error returned from addNode")
	verifyMetrics(t, 1, "active")

	// Update: node became unhealthy
	n = getNodeInfoForUpdatingNode(si.NodeInfo_UPDATE, false)
	context.updateNode(n)
	verifyMetrics(t, 1, "unhealthy")

	// Update: node became healthy
	n = getNodeInfoForUpdatingNode(si.NodeInfo_UPDATE, true)
	context.updateNode(n)
	verifyMetrics(t, 0, "unhealthy")
}

func TestContextAddUnhealthyNodeMetrics(t *testing.T) {
	metrics.GetSchedulerMetrics().Reset()
	context := createTestContext(t, pName)

	n := getNodeInfoForAddingNode(false)

	err := context.addNode(n, 1)
	assert.NilError(t, err, "unexpected error returned from addNode")
	verifyMetrics(t, 1, "active")
	verifyMetrics(t, 1, "unhealthy")
}

func TestContextDrainingNodeMetrics(t *testing.T) {
	metrics.GetSchedulerMetrics().Reset()
	context := createTestContext(t, pName)

	n := getNodeInfoForAddingNode(true)
	err := context.addNode(n, 1)
	assert.NilError(t, err, "unexpected error returned from addNode")

	n = getNodeInfoForUpdatingNode(si.NodeInfo_DRAIN_NODE, true)
	context.updateNode(n)
	verifyMetrics(t, 1, "draining")
}

func TestContextDrainingNodeBackToSchedulableMetrics(t *testing.T) {
	metrics.GetSchedulerMetrics().Reset()
	context := createTestContext(t, pName)

	n := getNodeInfoForAddingNode(true)
	err := context.addNode(n, 1)
	assert.NilError(t, err, "unexpected error returned from addNode")

	n = getNodeInfoForUpdatingNode(si.NodeInfo_DRAIN_NODE, true)
	context.updateNode(n)

	n = getNodeInfoForUpdatingNode(si.NodeInfo_DRAIN_TO_SCHEDULABLE, true)
	context.updateNode(n)
	verifyMetrics(t, 0, "draining")
}

func getNodeInfoForAddingNode(ready bool) *si.NodeInfo {
	n := &si.NodeInfo{
		NodeID:              "test-1",
		Action:              si.NodeInfo_UNKNOWN_ACTION_FROM_RM,
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 10}}},
		Attributes: map[string]string{
			siCommon.NodePartition: pName,
			objects.ReadyFlag:      strconv.FormatBool(ready),
		},
	}

	return n
}

func getNodeInfoForUpdatingNode(action si.NodeInfo_ActionFromRM, ready bool) *si.NodeInfo {
	n := &si.NodeInfo{
		NodeID: "test-1",
		Action: action,
		Attributes: map[string]string{
			siCommon.NodePartition: pName,
			objects.ReadyFlag:      strconv.FormatBool(ready),
		},
	}

	return n
}

func verifyMetrics(t *testing.T, expectedCounter float64, expectedState string) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	assert.NilError(t, err)

	var checked bool
outer:
	for _, metric := range mfs {
		if strings.Contains(metric.GetName(), "yunikorn_scheduler_node") {
			assert.Equal(t, dto.MetricType_GAUGE, metric.GetType())
			for _, m := range metric.Metric {
				if len(m.GetLabel()) == 1 &&
					*m.GetLabel()[0].Name == "state" &&
					*m.GetLabel()[0].Value == expectedState {
					assert.Assert(t, m.Gauge != nil)
					assert.Equal(t, expectedCounter, *m.Gauge.Value)
					checked = true
					break outer
				}
			}
		}
	}

	assert.Assert(t, checked, "Failed to find metric")
}
