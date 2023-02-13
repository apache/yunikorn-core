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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	appID1    = "app-1"
	appID2    = "app-2"
	appID3    = "app-3"
	nodeID1   = "node-1"
	nodeID2   = "node-2"
	defQueue  = "root.default"
	rmID      = "testRM"
	taskGroup = "tg-1"
	phID      = "ph-1"
	allocID   = "alloc-1"
	allocID2  = "alloc-2"
)

func newBasePartition() (*PartitionContext, error) {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "default",
						Parent: false,
						Queues: nil,
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
		Preemption:     configs.PartitionPreemptionConfig{},
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}

	return newPartitionContext(conf, rmID, nil)
}

func newConfiguredPartition() (*PartitionContext, error) {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "leaf",
						Parent: false,
						Queues: nil,
					}, {
						Name:   "parent",
						Parent: true,
						Queues: []configs.QueueConfig{
							{
								Name:   "sub-leaf",
								Parent: false,
								Queues: nil,
							},
						},
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
		Preemption:     configs.PartitionPreemptionConfig{},
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}
	return newPartitionContext(conf, rmID, nil)
}

func newLimitedPartition(resLimit map[string]string) (*PartitionContext, error) {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "limited",
						Parent: false,
						Queues: nil,
						Resources: configs.Resources{
							Max: resLimit,
						},
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
		Preemption:     configs.PartitionPreemptionConfig{},
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}

	return newPartitionContext(conf, rmID, nil)
}

func newPlacementPartition() (*PartitionContext, error) {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
		PlacementRules: []configs.PlacementRule{
			{
				Name:   "tag",
				Create: true,
				Parent: nil,
				Value:  "taskqueue",
			},
		},
		Limits:         nil,
		Preemption:     configs.PartitionPreemptionConfig{},
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}

	return newPartitionContext(conf, rmID, nil)
}

func newApplication(appID, partition, queueName string) *objects.Application {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{"testgroup"},
	}
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partition,
	}
	return objects.NewApplication(siApp, user, nil, rmID)
}

func newApplicationWithHandler(appID, partition, queueName string) (*objects.Application, *rmproxy.MockedRMProxy) {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{"testgroup"},
	}
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partition,
	}
	mockEventHandler := rmproxy.NewMockedRMProxy()
	return objects.NewApplication(siApp, user, mockEventHandler, rmID), mockEventHandler
}

func newApplicationTG(appID, partition, queueName string, task *resources.Resource) *objects.Application {
	return newApplicationTGTags(appID, partition, queueName, task, nil)
}

func newApplicationTGTags(appID, partition, queueName string, task *resources.Resource, tags map[string]string) *objects.Application {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{"testgroup"},
	}
	siApp := &si.AddApplicationRequest{
		ApplicationID:  appID,
		QueueName:      queueName,
		PartitionName:  partition,
		PlaceholderAsk: task.ToProto(),
		Tags:           tags,
	}
	return objects.NewApplication(siApp, user, nil, rmID)
}

func newAllocationAskTG(allocKey, appID, taskGroup string, res *resources.Resource, placeHolder bool) *objects.AllocationAsk {
	return newAllocationAskAll(allocKey, appID, taskGroup, res, 1, 1, placeHolder)
}

func newAllocationAsk(allocKey, appID string, res *resources.Resource) *objects.AllocationAsk {
	return newAllocationAskRepeat(allocKey, appID, res, 1)
}

func newAllocationAskRepeat(allocKey, appID string, res *resources.Resource, repeat int32) *objects.AllocationAsk {
	return newAllocationAskPriority(allocKey, appID, res, repeat, 1)
}

func newAllocationAskPriority(allocKey, appID string, res *resources.Resource, repeat int32, prio int32) *objects.AllocationAsk {
	return newAllocationAskAll(allocKey, appID, "", res, repeat, prio, false)
}

func newAllocationAskAll(allocKey, appID, taskGroup string, res *resources.Resource, repeat int32, prio int32, placeHolder bool) *objects.AllocationAsk {
	return objects.NewAllocationAskFromSI(&si.AllocationAsk{
		AllocationKey:  allocKey,
		ApplicationID:  appID,
		PartitionName:  "test",
		ResourceAsk:    res.ToProto(),
		MaxAllocations: repeat,
		Priority:       prio,
		TaskGroupName:  taskGroup,
		Placeholder:    placeHolder,
	})
}

func newNodeWithResources(nodeID string, max, occupied *resources.Resource) *objects.Node {
	proto := &si.NodeInfo{
		NodeID: nodeID,
		Attributes: map[string]string{
			common.NodeReadyAttribute: "true",
		},
		SchedulableResource: max.ToProto(),
		OccupiedResource:    occupied.ToProto(),
	}
	return objects.NewNode(proto)
}

func newNodeMaxResource(nodeID string, max *resources.Resource) *objects.Node {
	return newNodeWithResources(nodeID, max, nil)
}

// partition with an expected basic queue hierarchy
// root -> parent -> leaf1
//      -> leaf2
// and 2 nodes: node-1 & node-2
func createQueuesNodes(t *testing.T) *PartitionContext {
	partition, err := newConfiguredPartition()
	assert.NilError(t, err, "test partition create failed with error")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	err = partition.AddNode(newNodeMaxResource("node-1", res), nil)
	assert.NilError(t, err, "test node1 add failed unexpected")
	err = partition.AddNode(newNodeMaxResource("node-2", res), nil)
	assert.NilError(t, err, "test node2 add failed unexpected")
	return partition
}

func getTestUserGroup() security.UserGroup {
	return security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}
}

func assertUserGroupResource(t *testing.T, userGroup security.UserGroup, expected *resources.Resource) {
	ugm := ugm.GetUserManager()
	userResource := ugm.GetUserResources(userGroup)
	groupResource := ugm.GetGroupResources(userGroup.Groups[0])
	assert.Equal(t, resources.Equals(userResource, expected), true)
	assert.Equal(t, resources.Equals(groupResource, expected), true)
}
