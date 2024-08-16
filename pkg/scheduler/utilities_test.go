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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	appID1          = "app-1"
	appID2          = "app-2"
	appID3          = "app-3"
	nodeID1         = "node-1"
	instType1       = "itype-1"
	nodeID2         = "node-2"
	instType2       = "itype-2"
	defQueue        = "root.default"
	rmID            = "testRM"
	taskGroup       = "tg-1"
	phID            = "ph-1"
	phID2           = "ph-2"
	allocKey        = "alloc-1"
	allocKey2       = "alloc-2"
	allocKey3       = "alloc-3"
	maxresources    = "maxresources"
	maxapplications = "maxapplications"
	foreignAlloc1   = "foreign-alloc-1"
)

func newBasePartitionNoRootDefault() (*PartitionContext, error) {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "custom",
						Parent: false,
						Queues: nil,
						Limits: []configs.Limit{
							{
								Limit: "custom queue limit",
								Users: []string{
									"testuser",
								},
								Groups: []string{
									"testgroup",
								},
								MaxResources: map[string]string{
									"memory": "5",
									"vcores": "5",
								},
								MaxApplications: 8,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"testuser",
						},
						Groups: []string{
							"testgroup",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 10,
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}

	return newPartitionContext(conf, rmID, nil)
}

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
						Limits: []configs.Limit{
							{
								Limit: "default queue limit",
								Users: []string{
									"testuser",
								},
								Groups: []string{
									"testgroup",
								},
								MaxResources: map[string]string{
									"memory": "5",
									"vcores": "5",
								},
								MaxApplications: 8,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"testuser",
						},
						Groups: []string{
							"testgroup",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 10,
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
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
						Limits: []configs.Limit{
							{
								Limit: "leaf queue limit",
								Users: []string{
									"testuser",
								},
								Groups: []string{
									"testgroup",
								},
								MaxResources: map[string]string{
									"memory": "5",
									"vcores": "5",
								},
								MaxApplications: 1,
							},
						},
					}, {
						Name:   "parent",
						Parent: true,
						Queues: []configs.QueueConfig{
							{
								Name:   "sub-leaf",
								Parent: false,
								Queues: nil,
								Limits: []configs.Limit{
									{
										Limit: "sub-leaf queue limit",
										Users: []string{
											"testuser",
										},
										Groups: []string{
											"testgroup",
										},
										MaxResources: map[string]string{
											"memory": "3",
											"vcores": "3",
										},
										MaxApplications: 2,
									},
								},
							},
						},
						Limits: []configs.Limit{
							{
								Limit: "parent queue limit",
								Users: []string{
									"testuser",
								},
								Groups: []string{
									"testgroup",
								},
								MaxResources: map[string]string{
									"memory": "5",
									"vcores": "5",
								},
								MaxApplications: 8,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"testuser",
						},
						Groups: []string{
							"testgroup",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 10,
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}
	return newPartitionContext(conf, rmID, nil)
}

func newPreemptionConfiguredPartition(parentLimit map[string]string, leafGuarantees map[string]string) (*PartitionContext, error) {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "parent",
						Parent: true,
						Resources: configs.Resources{
							Max: parentLimit,
						},
						Queues: []configs.QueueConfig{
							{
								Name:   "leaf1",
								Parent: false,
								Queues: nil,
								Resources: configs.Resources{
									Guaranteed: leafGuarantees,
								},
								Properties: map[string]string{configs.PreemptionDelay: "1ms"},
								Limits: []configs.Limit{
									{
										Limit: "leaf1 queue limit",
										Users: []string{
											"testuser",
										},
										Groups: []string{
											"testgroup",
										},
										MaxResources: map[string]string{
											"memory": "5",
											"vcores": "5",
										},
										MaxApplications: 8,
									},
								},
							},
							{
								Name:   "leaf2",
								Parent: false,
								Queues: nil,
								Resources: configs.Resources{
									Guaranteed: leafGuarantees,
								},
								Properties: map[string]string{configs.PreemptionDelay: "1ms"},
								Limits: []configs.Limit{
									{
										Limit: "leaf2 queue limit",
										Users: []string{
											"testuser",
										},
										Groups: []string{
											"testgroup",
										},
										MaxResources: map[string]string{
											"memory": "5",
											"vcores": "5",
										},
										MaxApplications: 6,
									},
								},
							},
						},
						Limits: []configs.Limit{
							{
								Limit: "parent queue limit",
								Users: []string{
									"testuser",
								},
								Groups: []string{
									"testgroup",
								},
								MaxResources: map[string]string{
									"memory": "5",
									"vcores": "5",
								},
								MaxApplications: 8,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"testuser",
						},
						Groups: []string{
							"testgroup",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 10,
					},
				},
			},
		},
		PlacementRules: nil,
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
						MaxApplications: 2,
						Limits: []configs.Limit{
							{
								Limit: "limited queue limit",
								Users: []string{
									"testuser",
								},
								Groups: []string{
									"testgroup",
								},
								MaxResources: map[string]string{
									"memory": "5",
									"vcores": "5",
								},
								MaxApplications: 2,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"testuser",
						},
						Groups: []string{
							"testgroup",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 2,
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
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
				Limits: []configs.Limit{
					{
						Limit: "root queue limit",
						Users: []string{
							"testuser",
						},
						Groups: []string{
							"testgroup",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 2,
					},
				},
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
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}

	return newPartitionContext(conf, rmID, nil)
}

func newApplication(appID, partition, queueName string) *objects.Application {
	return newApplicationTags(appID, partition, queueName, nil)
}

func newApplicationTags(appID, partition, queueName string, tags map[string]string) *objects.Application {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{"testgroup"},
	}
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partition,
		Tags:          tags,
	}
	return objects.NewApplication(siApp, user, nil, rmID)
}

func newApplicationWithUser(appID, partition, queueName string, user security.UserGroup) *objects.Application {
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

func newApplicationTGTagsWithPhTimeout(appID, partition, queueName string, task *resources.Resource, tags map[string]string, phTimeout int64) *objects.Application {
	user := security.UserGroup{
		User:   "testuser",
		Groups: []string{"testgroup"},
	}
	siApp := &si.AddApplicationRequest{
		ApplicationID:                appID,
		QueueName:                    queueName,
		PartitionName:                partition,
		PlaceholderAsk:               task.ToProto(),
		Tags:                         tags,
		ExecutionTimeoutMilliSeconds: phTimeout,
	}
	return objects.NewApplication(siApp, user, nil, rmID)
}

func newAllocationAskTG(allocKey, appID, taskGroup string, res *resources.Resource, placeHolder bool) *objects.Allocation {
	return newAllocationAskAll(allocKey, appID, taskGroup, res, 1, placeHolder)
}

func newAllocationAsk(allocKey, appID string, res *resources.Resource) *objects.Allocation {
	return newAllocationAskAll(allocKey, appID, "", res, 1, false)
}

func newAllocationAskPriority(allocKey, appID string, res *resources.Resource, prio int32) *objects.Allocation {
	return newAllocationAskAll(allocKey, appID, "", res, prio, false)
}

func newAllocationAskAll(allocKey, appID, taskGroup string, res *resources.Resource, prio int32, placeHolder bool) *objects.Allocation {
	return objects.NewAllocationFromSI(&si.Allocation{
		AllocationKey:    allocKey,
		ApplicationID:    appID,
		PartitionName:    "test",
		ResourcePerAlloc: res.ToProto(),
		Priority:         prio,
		TaskGroupName:    taskGroup,
		Placeholder:      placeHolder,
	})
}

func newAllocationTG(allocKey, appID, nodeID, taskGroup string, res *resources.Resource, placeHolder bool) *objects.Allocation {
	return newAllocationAll(allocKey, appID, nodeID, taskGroup, res, 1, placeHolder)
}

func newAllocation(allocKey, appID, nodeID string, res *resources.Resource) *objects.Allocation {
	return newAllocationAll(allocKey, appID, nodeID, "", res, 1, false)
}

func newAllocationAll(allocKey, appID, nodeID, taskGroup string, res *resources.Resource, prio int32, placeHolder bool) *objects.Allocation {
	return objects.NewAllocationFromSI(&si.Allocation{
		AllocationKey:    allocKey,
		ApplicationID:    appID,
		PartitionName:    "test",
		NodeID:           nodeID,
		ResourcePerAlloc: res.ToProto(),
		Priority:         prio,
		TaskGroupName:    taskGroup,
		Placeholder:      placeHolder,
	})
}

func newForeignRequest(allocKey string) *objects.Allocation {
	return objects.NewAllocationFromSI(&si.Allocation{
		AllocationKey: allocKey,
		AllocationTags: map[string]string{
			"foreign": "true",
		},
	})
}

func newForeignAllocation(allocKey, nodeID string) *objects.Allocation {
	alloc := objects.NewAllocationFromSIAllocated(&si.Allocation{
		AllocationKey: allocKey,
		AllocationTags: map[string]string{
			"foreign": "true",
		},
		NodeID: nodeID,
	})
	return alloc
}

func newAllocationAskPreempt(allocKey, appID string, prio int32, res *resources.Resource) *objects.Allocation {
	return objects.NewAllocationFromSI(&si.Allocation{
		AllocationKey:    allocKey,
		ApplicationID:    appID,
		PartitionName:    "default",
		ResourcePerAlloc: res.ToProto(),
		Priority:         prio,
		TaskGroupName:    taskGroup,
		Placeholder:      false,
		PreemptionPolicy: &si.PreemptionPolicy{
			AllowPreemptSelf:  true,
			AllowPreemptOther: true,
		},
	})
}

func newNodeMaxResource(nodeID string, max *resources.Resource) *objects.Node {
	proto := &si.NodeInfo{
		NodeID:              nodeID,
		Attributes:          map[string]string{},
		SchedulableResource: max.ToProto(),
	}
	return objects.NewNode(proto)
}

// partition with an expected basic queue hierarchy
// root -> parent -> leaf1
//
//	-> leaf2
//
// and 2 nodes: node-1 & node-2
func createQueuesNodes(t *testing.T) *PartitionContext {
	partition, err := newConfiguredPartition()
	assert.NilError(t, err, "test partition create failed with error")
	var res *resources.Resource
	res, err = resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	err = partition.AddNode(newNodeMaxResource("node-1", res))
	assert.NilError(t, err, "test node1 add failed unexpected")
	err = partition.AddNode(newNodeMaxResource("node-2", res))
	assert.NilError(t, err, "test node2 add failed unexpected")
	return partition
}

// partition with a sibling relationship for testing preemption
// root -> parent -> {leaf1,leaf2}
//
//	parent max: 10 vcore, leaf guarantees: 5 vcore
//
// and 2 nodes: node-1 & node-2
func createPreemptionQueuesNodes(t *testing.T) *PartitionContext {
	parentLimit := map[string]string{"vcore": "10"}
	leafGuarantees := map[string]string{"vcore": "5"}
	partition, err := newPreemptionConfiguredPartition(parentLimit, leafGuarantees)
	assert.NilError(t, err, "test partition create failed with error")
	res, err := resources.NewResourceFromConf(map[string]string{"vcore": "10"})
	assert.NilError(t, err, "failed to create basic resource")
	err = partition.AddNode(newNodeMaxResource("node-1", res))
	assert.NilError(t, err, "test node1 add failed unexpected")
	err = partition.AddNode(newNodeMaxResource("node-2", res))
	assert.NilError(t, err, "test node2 add failed unexpected")
	return partition
}

func getTestUserGroup() security.UserGroup {
	return security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}
}

func assertLimits(t *testing.T, userGroup security.UserGroup, expected *resources.Resource) {
	expectedQueuesMaxLimits := make(map[string]map[string]interface{})
	expectedQueuesMaxLimits["root"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root.default"] = make(map[string]interface{})
	expectedQueuesMaxLimits["root"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 10, "vcores": 10})
	expectedQueuesMaxLimits["root.default"][maxresources] = resources.NewResourceFromMap(map[string]resources.Quantity{"memory": 5, "vcores": 5})
	expectedQueuesMaxLimits["root"][maxapplications] = uint64(10)
	expectedQueuesMaxLimits["root.default"][maxapplications] = uint64(8)
	assertUserGroupResourceMaxLimits(t, userGroup, expected, expectedQueuesMaxLimits)
}

func assertUserGroupResourceMaxLimits(t *testing.T, userGroup security.UserGroup, expected *resources.Resource, expectedQueuesMaxLimits map[string]map[string]interface{}) {
	manager := ugm.GetUserManager()
	userResource := manager.GetUserResources(userGroup)
	groupResource := manager.GetGroupResources(userGroup.Groups[0])
	ut := manager.GetUserTracker(userGroup.User)
	if ut != nil {
		maxResources := make(map[string]*resources.Resource)
		usage := ut.GetUserResourceUsageDAOInfo()
		getMaxResource(usage.Queues, maxResources)
		for q, qMaxLimits := range expectedQueuesMaxLimits {
			if qRes, ok := maxResources[q]; ok {
				assert.Equal(t, resources.Equals(qRes, qMaxLimits[maxresources].(*resources.Resource)), true)
			}
		}
		maxApplications := make(map[string]uint64)
		getMaxApplications(usage.Queues, maxApplications)
		for q, qMaxLimits := range expectedQueuesMaxLimits {
			if qApps, ok := maxApplications[q]; ok {
				assert.Equal(t, qApps, qMaxLimits[maxapplications].(uint64), "queue path is "+q+" actual: "+strconv.Itoa(int(qApps))+", expected: "+strconv.Itoa(int(qMaxLimits[maxapplications].(uint64))))
			}
		}
	}

	gt := manager.GetUserTracker(userGroup.User)
	if gt != nil {
		gMaxResources := make(map[string]*resources.Resource)
		gUsage := gt.GetUserResourceUsageDAOInfo()
		getMaxResource(gUsage.Queues, gMaxResources)
		for q, qMaxLimits := range expectedQueuesMaxLimits {
			if qRes, ok := gMaxResources[q]; ok {
				assert.Equal(t, resources.Equals(qRes, qMaxLimits[maxresources].(*resources.Resource)), true)
			}
		}
		gMaxApps := make(map[string]uint64)
		getMaxApplications(gUsage.Queues, gMaxApps)
		for q, qMaxLimits := range expectedQueuesMaxLimits {
			if qApps, ok := gMaxApps[q]; ok {
				assert.Equal(t, qApps, qMaxLimits[maxapplications].(uint64))
			}
		}
	}
	assert.Equal(t, resources.Equals(userResource, expected), true)
	assert.Equal(t, resources.Equals(groupResource, expected), true)
}

func getMaxResource(usage *dao.ResourceUsageDAOInfo, maxResources map[string]*resources.Resource) map[string]*resources.Resource {
	maxResources[usage.QueuePath] = usage.MaxResources
	if len(usage.Children) > 0 {
		for _, resourceUsage := range usage.Children {
			getMaxResource(resourceUsage, maxResources)
		}
	}
	return maxResources
}

func getMaxApplications(usage *dao.ResourceUsageDAOInfo, maxApplications map[string]uint64) map[string]uint64 {
	maxApplications[usage.QueuePath] = usage.MaxApplications
	if len(usage.Children) > 0 {
		for _, resourceUsage := range usage.Children {
			getMaxApplications(resourceUsage, maxApplications)
		}
	}
	return maxApplications
}
