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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-core/pkg/rmproxy/rmevent"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
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

func TestContext_UpdateNode(t *testing.T) {
	context := createTestContext(t, pName)
	n := &si.NodeInfo{
		NodeID:              "test-1",
		Action:              si.NodeInfo_UNKNOWN_ACTION_FROM_RM,
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 10}}},
	}
	full := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
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
	err := context.addNode(n, true)
	assert.NilError(t, err, "unexpected error returned from addNode")
	assert.Equal(t, 1, len(partition.GetNodes()), "expected node not found on partition")
	assert.Assert(t, resources.Equals(full, partition.GetTotalPartitionResource()), "partition resource should be updated")
	// try to update: fail due to unknown action
	n.SchedulableResource = &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 5}}}
	context.updateNode(n)
	node := partition.GetNode("test-1")
	assert.Assert(t, resources.Equals(full, node.GetAvailableResource()), "node available resource should not be updated")

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
	err := context.addNode(n, true)
	if err == nil {
		t.Fatalf("unknown node partition should have failed the node add")
	}
	n.Attributes = map[string]string{"si/node-partition": pName}
	err = context.addNode(n, true)
	assert.NilError(t, err, "unexpected error returned adding node")
	partition := context.GetPartition(pName)
	if partition == nil {
		t.Fatalf("partition should have been found")
	}
	assert.Equal(t, 1, len(partition.GetNodes()), "expected node not found on partition")
	// add same node again should show an error
	err = context.addNode(n, true)
	if err == nil {
		t.Fatalf("existing node addition should have failed")
	}
}

func TestContext_AddNodeDrained(t *testing.T) {
	context := createTestContext(t, pName)

	draining, err := metrics.GetSchedulerMetrics().GetDrainingNodes()
	assert.NilError(t, err, "failed to get draining node count")

	n := &si.NodeInfo{
		NodeID:              "test-1",
		Action:              si.NodeInfo_CREATE_DRAIN,
		Attributes:          map[string]string{"si/node-partition": "unknown"},
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 10}}},
	}
	err = context.addNode(n, false)
	if err == nil {
		t.Fatalf("unknown node partition should have failed the node add")
	}
	n.Attributes = map[string]string{"si/node-partition": pName}
	err = context.addNode(n, false)
	assert.NilError(t, err, "unexpected error returned adding node")
	partition := context.GetPartition(pName)
	if partition == nil {
		t.Fatalf("partition should have been found")
	}
	assert.Equal(t, 1, len(partition.GetNodes()), "expected node not found on partition")
	// add same node again should show an error
	err = context.addNode(n, false)
	if err == nil {
		t.Fatalf("existing node addition should have failed")
	}
	// verify that node is draining
	node := partition.GetNode(n.NodeID)
	assert.Assert(t, node != nil, "node not found in partition")
	assert.Equal(t, false, node.IsSchedulable(), "node was schedulable and should not be")
	expectedDraining := draining + 1
	draining, err = metrics.GetSchedulerMetrics().GetDrainingNodes()
	assert.NilError(t, err, "failed to get draining node count")
	assert.Equal(t, expectedDraining, draining, "wrong draining node count")
}

func TestContext_AddRMBuildInformation(t *testing.T) {
	context := createTestContext(t, pName)

	rmID1 := "myCluster1"
	buildInfoMap1 := make(map[string]string)
	buildInfoMap1["buildDate"] = "2006-01-02T15:04:05-0700"
	buildInfoMap1["buildVersion"] = "latest"
	buildInfoMap1["isPluginVersion"] = "false"

	rmID2 := "myCluster2"
	buildInfoMap2 := make(map[string]string)
	buildInfoMap2["buildDate"] = "2022-01-02T15:04:05-0700"
	buildInfoMap2["buildVersion"] = "latest"
	buildInfoMap2["isPluginVersion"] = "true"

	context.SetRMInfo(rmID1, buildInfoMap1)
	assert.Equal(t, 1, len(context.rmInfo))
	buildInfoMap1["rmId"] = rmID1
	assert.DeepEqual(t, context.rmInfo[rmID1].RMBuildInformation, buildInfoMap1)
	assert.Equal(t, context.rmInfo[rmID1].RMBuildInformation["rmId"], rmID1)

	context.SetRMInfo(rmID2, buildInfoMap2)
	assert.Equal(t, 2, len(context.rmInfo))
	buildInfoMap2["rmId"] = rmID2
	assert.DeepEqual(t, context.rmInfo[rmID2].RMBuildInformation, buildInfoMap2)
	assert.Equal(t, context.rmInfo[rmID2].RMBuildInformation["rmId"], rmID2)

	// update a value,make sure it is replaced as expected
	// since we store the pointer and not a copy we need to create a new object to replace the old one
	buildInfoMap3 := make(map[string]string)
	buildInfoMap3["buildDate"] = "2022-01-02T19:04:05-0700"
	buildInfoMap3["isPluginVersion"] = "true"
	buildInfoMap3["buildVersion"] = "1.0.0"
	context.SetRMInfo(rmID2, buildInfoMap3)
	assert.Equal(t, 2, len(context.rmInfo))
	buildInfoMap3["rmId"] = rmID2
	assert.DeepEqual(t, context.rmInfo[rmID2].RMBuildInformation, buildInfoMap3)
	assert.Equal(t, context.rmInfo[rmID2].RMBuildInformation["buildVersion"], buildInfoMap3["buildVersion"])
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

func TestContextDrainingNodeMetrics(t *testing.T) {
	metrics.GetSchedulerMetrics().Reset()
	context := createTestContext(t, pName)

	n := getNodeInfoForAddingNode()
	err := context.addNode(n, true)
	assert.NilError(t, err, "unexpected error returned from addNode")

	n = getNodeInfoForUpdatingNode(si.NodeInfo_DRAIN_NODE)
	context.updateNode(n)
	verifyMetrics(t, 1, "draining")
}

func TestContextDrainingNodeBackToSchedulableMetrics(t *testing.T) {
	metrics.GetSchedulerMetrics().Reset()
	context := createTestContext(t, pName)

	n := getNodeInfoForAddingNode()
	err := context.addNode(n, true)
	assert.NilError(t, err, "unexpected error returned from addNode")

	n = getNodeInfoForUpdatingNode(si.NodeInfo_DRAIN_NODE)
	context.updateNode(n)

	n = getNodeInfoForUpdatingNode(si.NodeInfo_DRAIN_TO_SCHEDULABLE)
	context.updateNode(n)
	verifyMetrics(t, 0, "draining")
}

func getNodeInfoForAddingNode() *si.NodeInfo {
	n := &si.NodeInfo{
		NodeID:              "test-1",
		Action:              si.NodeInfo_UNKNOWN_ACTION_FROM_RM,
		SchedulableResource: &si.Resource{Resources: map[string]*si.Quantity{"first": {Value: 10}}},
		Attributes: map[string]string{
			siCommon.NodePartition: pName,
		},
	}

	return n
}

func getNodeInfoForUpdatingNode(action si.NodeInfo_ActionFromRM) *si.NodeInfo {
	n := &si.NodeInfo{
		NodeID: "test-1",
		Action: action,
		Attributes: map[string]string{
			siCommon.NodePartition: pName,
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
