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
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/rmproxy/rmevent"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

type mockEventHandler struct {
	eventHandled  bool
	rejectedNodes []*si.RejectedNode
	acceptedNodes []*si.AcceptedNode
}

func NewMockEventHandler() *mockEventHandler {
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
	m.eventHandled = true
	nodeEvent := ev.(*rmevent.RMNodeUpdateEvent)
	m.rejectedNodes = append(m.rejectedNodes, nodeEvent.RejectedNodes...)
	m.acceptedNodes = append(m.acceptedNodes, nodeEvent.AcceptedNodes...)
}

func TestAddUnlimitedNode(t *testing.T) {
	handler := NewMockEventHandler()
	context := &ClusterContext{
		partitions:     map[string]*PartitionContext{},
		rmEventHandler: handler,
	}
	conf := configs.PartitionConfig{
		Name: "default",
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
	context.partitions[partition.Name] = partition
	assert.NilError(t, err, "partition create should not have failed with error")
	unlimitedNode := &si.NewNodeInfo{
		NodeID:     "unlimited",
		Attributes: map[string]string{"yunikorn.apache.org/nodeType": "unlimited", "si/node-partition": "default"},
	}
	var newNodes []*si.NewNodeInfo
	newNodes = append(newNodes, unlimitedNode)

	request := &si.UpdateRequest{
		NewSchedulableNodes: newNodes,
	}

	// 1. add the first unlimited node, but keep reservation enabled
	context.addNodes(request)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 0, "There should be no accepted nodes")
	assert.Assert(t, len(handler.rejectedNodes) == 1, "The node should be rejected")
	assert.Equal(t, handler.rejectedNodes[0].NodeID, unlimitedNode.NodeID, "The rejected node is not the unlimited one")

	// 2. add the first unlimited node, but disable reservation
	context.reservationDisabled = true
	handler.reset()
	context.addNodes(request)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 1, "The node should be accepted")
	assert.Assert(t, len(handler.rejectedNodes) == 0, "There should be no rejected nodes")
	assert.Equal(t, handler.acceptedNodes[0].NodeID, unlimitedNode.NodeID, "The accepted node is not the unlimited one")

	// 3. there is already an unlimited node registeredvar newNodes []*si.NewNodeInfo
	unlimitedNode2 := &si.NewNodeInfo{
		NodeID:     "unlimited2",
		Attributes: map[string]string{"yunikorn.apache.org/nodeType": "unlimited", "si/node-partition": "default"},
	}
	var newNodes2 []*si.NewNodeInfo
	newNodes2 = append(newNodes2, unlimitedNode2)
	request2 := &si.UpdateRequest{
		NewSchedulableNodes: newNodes2,
	}
	handler.reset()
	context.addNodes(request2)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 0, "There should be no accepted nodes")
	assert.Assert(t, len(handler.rejectedNodes) == 1, "The node should be rejected")
	assert.Equal(t, handler.rejectedNodes[0].NodeID, unlimitedNode2.NodeID, "The rejected node is not the unlimited one")

	// 4. there are other nodes in the list as well
	regularNode := &si.NewNodeInfo{
		NodeID:     "regularNode",
		Attributes: map[string]string{"si/node-partition": "default"},
	}
	newNodes2 = append(newNodes2, unlimitedNode, regularNode)
	request2.NewSchedulableNodes = newNodes2
	handler.reset()
	partition.removeNode(unlimitedNode.NodeID)
	context.addNodes(request2)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 1, "There should be only one accepted node")
	assert.Assert(t, len(handler.rejectedNodes) == 2, "There should be 2 rejected nodes")
	assert.Equal(t, handler.acceptedNodes[0].NodeID, regularNode.NodeID, "The accepted node is not the regular one")

	// 5. there is a regular node registered
	handler.reset()
	context.addNodes(request)
	assert.Assert(t, handler.eventHandled, "Event should have been handled")
	assert.Assert(t, len(handler.acceptedNodes) == 0, "There should be no accepted node")
	assert.Assert(t, len(handler.rejectedNodes) == 1, "The node should be rejected")
	assert.Equal(t, handler.rejectedNodes[0].NodeID, unlimitedNode.NodeID, "The rejected node is not the unlimited one")
}
