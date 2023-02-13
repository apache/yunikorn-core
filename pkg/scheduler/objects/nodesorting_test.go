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

package objects

import (
	"testing"

	"github.com/google/uuid"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/scheduler/policies"
)

func TestNewNodeSortingPolicy(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want policies.SortingPolicy
	}{
		{"EmptyString", "", policies.FairnessPolicy},
		{"FairString", "fair", policies.FairnessPolicy},
		{"BinString", "binpacking", policies.BinPackingPolicy},
		{"UnknownString", "unknown", policies.FairnessPolicy},
	}
	for _, tt := range tests {
		got := NewNodeSortingPolicy(tt.arg, nil)
		if got == nil || got.PolicyType() != tt.want {
			t.Errorf("%s unexpected policy returned, expected = '%s', got '%v'", tt.name, tt.want, got)
		}
	}
}

func TestEmptyResourceWeightsFromConfig(t *testing.T) {
	emptyConf := `
partitions:
  - name: default
    nodesortpolicy:
      type: fair
    queues:
      - name: root
        submitacl: '*'
`
	conf, err := configs.LoadSchedulerConfigFromByteArray([]byte(emptyConf))
	assert.NilError(t, err, "No error is expected")
	if conf == nil {
		t.Fatal("Returned conf was nil")
	}
	if conf.Partitions == nil {
		t.Fatal("Returned partitions was nil")
	}
	assert.Equal(t, 1, len(conf.Partitions), "Wrong partition count")

	policyType := conf.Partitions[0].NodeSortPolicy.Type
	weights := conf.Partitions[0].NodeSortPolicy.ResourceWeights
	assert.Equal(t, 0, len(weights), "Expected empty weights")

	policy, ok := NewNodeSortingPolicy(policyType, weights).(fairnessNodeSortingPolicy)
	if !ok {
		t.Fatal("Didn't get expected policy")
	}

	assert.Equal(t, 2, len(policy.resourceWeights), "Wrong size of resourceWeights")
	assert.Equal(t, policy.resourceWeights["vcore"], 1.0, "Wrong weight for vcore")
	assert.Equal(t, policy.resourceWeights["memory"], 1.0, "Wrong weight for memory")
}

func TestInvalidResourceWeightsFromConfig(t *testing.T) {
	badConf := `
partitions:
  - name: default
    nodesortpolicy:
      type: fair
      resourceweights:
        vcore: -1.0
        memory: 0.5
        gpu: 5.0
    queues:
      - name: root
        submitacl: '*'
`
	_, err := configs.LoadSchedulerConfigFromByteArray([]byte(badConf))
	assert.ErrorContains(t, err, "negative", "Expected error")
}

func TestSpecifiedResourceWeightsFromConfig(t *testing.T) {
	explicitConf := `
partitions:
  - name: default
    nodesortpolicy:
      type: fair
      resourceweights:
        vcore: 2.0
        memory: 0.5
        gpu: 5.0
    queues:
      - name: root
        submitacl: '*'
`
	conf, err := configs.LoadSchedulerConfigFromByteArray([]byte(explicitConf))
	assert.NilError(t, err, "No error is expected")
	if conf == nil {
		t.Fatal("Returned conf was nil")
	}
	if conf.Partitions == nil {
		t.Fatal("Returned partitions was nil")
	}
	assert.Equal(t, 1, len(conf.Partitions), "Wrong partition count")

	policyType := conf.Partitions[0].NodeSortPolicy.Type
	weights := conf.Partitions[0].NodeSortPolicy.ResourceWeights
	assert.Equal(t, 3, len(weights), "Expected populated weights")

	policy, ok := NewNodeSortingPolicy(policyType, weights).(fairnessNodeSortingPolicy)
	if !ok {
		t.Fatal("Didn't get expected policy")
	}

	assert.Equal(t, 3, len(policy.resourceWeights), "Wrong size of resourceWeights")
	assert.Equal(t, policy.resourceWeights["vcore"], 2.0, "Wrong weight for vcore")
	assert.Equal(t, policy.resourceWeights["memory"], 0.5, "Wrong weight for memory")
	assert.Equal(t, policy.resourceWeights["gpu"], 5.0, "Wrong weight for gpu")
}

func TestSortPolicyWeighting(t *testing.T) {
	nc := NewNodeCollection("test")
	weights := map[string]float64{
		"vcore":  4.0,
		"memory": 1.0,
	}
	fair, ok := NewNodeSortingPolicy("fair", weights).(fairnessNodeSortingPolicy)
	if !ok {
		t.Fatal("Didn't get fair policy")
	}
	bin, ok := NewNodeSortingPolicy("binpacking", weights).(binPackingNodeSortingPolicy)
	if !ok {
		t.Fatal("Didn't get binpacking policy")
	}

	nc.SetNodeSortingPolicy(fair)
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 2000, "memory": 16000})

	if nc.GetNodeIterator() != nil {
		t.Fatal("Shouldn't have any nodes when there are not any nodes in nodeCollection.")
	}

	proto1 := newProto("test1", totalRes, nil, map[string]string{})
	node1 := NewNode(proto1)
	if err := nc.AddNode(node1); err != nil {
		t.Fatal("Failed to add node1")
	}

	proto2 := newProto("test2", totalRes, nil, map[string]string{})
	node2 := NewNode(proto2)
	if err := nc.AddNode(node2); err != nil {
		t.Fatal("Failed to add node2")
	}

	// add allocations
	res1 := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 500, "memory": 12000})
	alloc1 := newAllocation("test-app-1", uuid.NewString(), "test1", "root.default", res1)
	node1.AddAllocation(alloc1)

	res2 := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1500, "memory": 4000})
	alloc2 := newAllocation("test-app-1", uuid.NewString(), "test2", "root.default", res2)
	node2.AddAllocation(alloc2)

	// node1 w/ fair: 25% vcore, 75% memory => ((.25 * 4) + (.75 * 1)) / 5 = 0.35
	assert.Equal(t, 0.35, fair.ScoreNode(node1), "Wrong fair score for node1")

	// node1 w/ binpacking: same but 1 - fair => 0.65
	assert.Equal(t, 0.65, bin.ScoreNode(node1), "Wrong binpacking score for node1")

	// node2 w/ fair: 75% vcore, 25% memory => ((.75 * 4) + (.25 * 1)) / 5 = 0.65
	assert.Equal(t, 0.65, fair.ScoreNode(node2), "Wrong fair score for node2")

	// node2 w/ binpacking: same but 1 - fair = 0.35
	assert.Equal(t, 0.35, bin.ScoreNode(node2), "Wrong binpacking score for node2")

	// node1 should be first as it is the least-loaded
	nodes := make([]*Node, 0)
	for it := nc.GetNodeIterator(); it.HasNext(); {
		nodes = append(nodes, it.Next())
	}

	assert.Equal(t, 2, len(nodes), "node length != 2")
	assert.Equal(t, node1.NodeID, nodes[0].NodeID, "wrong initial node (fair)")
	assert.Equal(t, node2.NodeID, nodes[1].NodeID, "wrong second node (fair)")

	// switch to binpacking
	nc.SetNodeSortingPolicy(bin)

	// node2 should now be first as it is most-loaded
	nodes = make([]*Node, 0)
	for it := nc.GetNodeIterator(); it.HasNext(); {
		nodes = append(nodes, it.Next())
	}

	assert.Equal(t, 2, len(nodes), "node length != 2")
	assert.Equal(t, node2.NodeID, nodes[0].NodeID, "wrong initial node (binpacking)")
	assert.Equal(t, node1.NodeID, nodes[1].NodeID, "wrong second node (binpacking)")
}

func TestSortPolicy(t *testing.T) {
	nc := NewNodeCollection("test")
	bp := NewNodeSortingPolicy("binpacking", nil)
	fair := NewNodeSortingPolicy("fair", nil)
	nc.SetNodeSortingPolicy(bp)
	totalRes := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 2000, "memory": 4000})

	proto1 := newProto("test1", totalRes, nil, map[string]string{})
	node1 := NewNode(proto1)
	if err := nc.AddNode(node1); err != nil {
		t.Fatal("Failed to add node1")
	}

	proto2 := newProto("test2", totalRes, nil, map[string]string{})
	node2 := NewNode(proto2)
	if err := nc.AddNode(node2); err != nil {
		t.Fatal("Failed to add node2")
	}

	// nodes should be in ascending order before any allocations
	nodes := make([]*Node, 0)
	for it := nc.GetNodeIterator(); it.HasNext(); {
		nodes = append(nodes, it.Next())
	}
	assert.Equal(t, 2, len(nodes), "node length != 2")
	assert.Equal(t, node1.NodeID, nodes[0].NodeID, "wrong initial node (binpacking, empty allocation)")
	assert.Equal(t, node2.NodeID, nodes[1].NodeID, "wrong second node (binpacking, empty allocation)")

	// change policy to fair, and we should see the same order
	nc.SetNodeSortingPolicy(fair)

	nodes = make([]*Node, 0)
	for it := nc.GetNodeIterator(); it.HasNext(); {
		nodes = append(nodes, it.Next())
	}
	assert.Equal(t, 2, len(nodes), "node length != 2")
	assert.Equal(t, node1.NodeID, nodes[0].NodeID, "wrong initial node (fair, empty allocation)")
	assert.Equal(t, node2.NodeID, nodes[1].NodeID, "wrong second node (fair, empty allocation)")

	// reset back to binpacking
	nc.SetNodeSortingPolicy(bp)

	// add allocation to second node
	half := resources.NewResourceFromMap(map[string]resources.Quantity{"vcore": 1000, "memory": 2000})
	alloc := newAllocation("test-app-1", uuid.NewString(), "test2", "root.default", half)
	node2.AddAllocation(alloc)

	// node2 should now be first as it is the highest-loaded
	nodes = make([]*Node, 0)
	for it := nc.GetNodeIterator(); it.HasNext(); {
		nodes = append(nodes, it.Next())
	}

	assert.Equal(t, 2, len(nodes), "node length != 2")
	assert.Equal(t, node2.NodeID, nodes[0].NodeID, "wrong initial node (binpacking, node2 half-filled)")
	assert.Equal(t, node1.NodeID, nodes[1].NodeID, "wrong second node (binpacking, node2 half-filled")

	// change policy to fair and try again
	nc.SetNodeSortingPolicy(fair)

	// node1 should again be first, as it is least-loaded
	nodes = make([]*Node, 0)
	for it := nc.GetNodeIterator(); it.HasNext(); {
		nodes = append(nodes, it.Next())
	}
	assert.Equal(t, 2, len(nodes), "node length != 2")
	assert.Equal(t, node1.NodeID, nodes[0].NodeID, "wrong initial node (fair, node2 half-filled)")
	assert.Equal(t, node2.NodeID, nodes[1].NodeID, "wrong second node (binpacking, node2 half-filled")
}
