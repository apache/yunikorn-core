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
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	appID0  = "app-0"
	appID1  = "app-1"
	appID2  = "app-2"
	aKey    = "alloc-1"
	nodeID1 = "node-1"
)

// Create the root queue, base for all testing
func createRootQueue(maxRes map[string]string) (*Queue, error) {
	return createManagedQueueWithProps(nil, "root", true, maxRes, nil)
}

// wrapper around the create call using the one syntax for all queue types
func createManagedQueue(parentSQ *Queue, name string, parent bool, maxRes map[string]string) (*Queue, error) {
	return createManagedQueueWithProps(parentSQ, name, parent, maxRes, nil)
}

// create managed queue with props set
func createManagedQueueWithProps(parentSQ *Queue, name string, parent bool, maxRes, props map[string]string) (*Queue, error) {
	return createManagedQueuePropsMaxApps(parentSQ, name, parent, maxRes, nil, props, uint64(0))
}

func createManagedQueueMaxApps(parentSQ *Queue, name string, parent bool, maxRes map[string]string, maxApps uint64) (*Queue, error) {
	return createManagedQueuePropsMaxApps(parentSQ, name, parent, maxRes, nil, nil, maxApps)
}

func createManagedQueueGuaranteed(parentSQ *Queue, name string, parent bool, maxRes, guarRes map[string]string) (*Queue, error) {
	return createManagedQueuePropsMaxApps(parentSQ, name, parent, maxRes, guarRes, nil, uint64(0))
}

func createManagedQueuePropsMaxApps(parentSQ *Queue, name string, parent bool, maxRes map[string]string, guarRes map[string]string, props map[string]string, maxApps uint64) (*Queue, error) {
	queueConfig := configs.QueueConfig{
		Name:            name,
		Parent:          parent,
		Queues:          nil,
		Properties:      props,
		MaxApplications: maxApps,
	}
	if maxRes != nil || guarRes != nil {
		queueConfig.Resources = configs.Resources{
			Max:        maxRes,
			Guaranteed: guarRes,
		}
	}
	queue, err := NewConfiguredQueue(queueConfig, parentSQ)
	if err != nil {
		return nil, err
	}
	// root queue can not set the max via the config
	if parentSQ == nil {
		var max *resources.Resource
		max, err = resources.NewResourceFromConf(maxRes)
		if err != nil {
			return nil, err
		}
		// only set if we have some limit
		if len(max.Resources) > 0 && !resources.IsZero(max) {
			queue.SetMaxResource(max)
		}
	}
	return queue, nil
}

// wrapper around the create call using the one syntax for all queue types
// NOTE: test code uses a flag for parent=true, dynamic queues use leaf flag
func createDynamicQueue(parentSQ *Queue, name string, parent bool) (*Queue, error) {
	return NewDynamicQueue(name, !parent, parentSQ)
}

// Create application with minimal info
func newApplication(appID, partition, queueName string) *Application {
	tags := make(map[string]string)
	return newApplicationWithTags(appID, partition, queueName, tags)
}

// Create application with minimal info
func newApplicationWithUserGroup(appID, partition, queueName string, userName string, groupList []string) *Application {
	tags := make(map[string]string)
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partition,
		Tags:          tags,
	}
	return NewApplication(siApp, getUserGroup(userName, groupList), nil, "")
}

// Create application with tags set
func newApplicationWithTags(appID, partition, queueName string, tags map[string]string) *Application {
	siApp := &si.AddApplicationRequest{
		ApplicationID: appID,
		QueueName:     queueName,
		PartitionName: partition,
		Tags:          tags,
	}
	return NewApplication(siApp, getTestUserGroup(), nil, "")
}

func newApplicationWithHandler(appID, partition, queueName string) (*Application, *rmproxy.MockedRMProxy) {
	return newApplicationWithPlaceholderTimeout(appID, partition, queueName, 0)
}

func newApplicationWithPlaceholderTimeout(appID, partition, queueName string, phTimeout int64) (*Application, *rmproxy.MockedRMProxy) {
	siApp := &si.AddApplicationRequest{
		ApplicationID:                appID,
		QueueName:                    queueName,
		PartitionName:                partition,
		ExecutionTimeoutMilliSeconds: phTimeout,
	}
	mockEventHandler := rmproxy.NewMockedRMProxy()
	return NewApplication(siApp, getTestUserGroup(), mockEventHandler, ""), mockEventHandler
}

// Create node with minimal info
func newNode(nodeID string, totalMap map[string]resources.Quantity) *Node {
	total := resources.NewResourceFromMap(totalMap)
	return newNodeInternal(nodeID, total, resources.Zero)
}

func newNodeRes(nodeID string, total *resources.Resource) *Node {
	return newNodeInternal(nodeID, total, resources.Zero)
}

func newNodeInternal(nodeID string, total, occupied *resources.Resource) *Node {
	return &Node{
		NodeID:            nodeID,
		Hostname:          "",
		Rackname:          "",
		Partition:         "",
		attributes:        nil,
		totalResource:     total,
		occupiedResource:  occupied,
		allocatedResource: resources.NewResource(),
		availableResource: resources.Sub(total, occupied),
		allocations:       make(map[string]*Allocation),
		schedulable:       true,
		reservations:      make(map[string]*reservation),
	}
}

func newProto(nodeID string, totalResource, occupiedResource *resources.Resource, attributes map[string]string) *si.NodeInfo {
	proto := si.NodeInfo{
		NodeID:     nodeID,
		Attributes: attributes,
	}

	if totalResource != nil {
		proto.SchedulableResource = &si.Resource{
			Resources: map[string]*si.Quantity{},
		}
		for name, value := range totalResource.Resources {
			quantity := si.Quantity{Value: int64(value)}
			proto.SchedulableResource.Resources[name] = &quantity
		}
	}

	if occupiedResource != nil {
		proto.OccupiedResource = &si.Resource{
			Resources: map[string]*si.Quantity{},
		}
		for name, value := range occupiedResource.Resources {
			quantity := si.Quantity{Value: int64(value)}
			proto.OccupiedResource.Resources[name] = &quantity
		}
	}
	return &proto
}

// Create a new Allocation with a random ask key
func newAllocation(appID, uuid, nodeID, queueName string, res *resources.Resource) *Allocation {
	askKey := strconv.FormatInt((time.Now()).UnixNano(), 10)
	ask := newAllocationAsk(askKey, appID, res)
	return NewAllocation(uuid, nodeID, ask)
}

// Create a new Allocation with a random ask key
func newPlaceholderAlloc(appID, uuid, nodeID, queueName string, res *resources.Resource) *Allocation {
	askKey := strconv.FormatInt((time.Now()).UnixNano(), 10)
	ask := newAllocationAsk(askKey, appID, res)
	ask.placeholder = true
	return NewAllocation(uuid, nodeID, ask)
}

func newAllocationAsk(allocKey, appID string, res *resources.Resource) *AllocationAsk {
	return newAllocationAskAll(allocKey, appID, "", res, 1, false, 0)
}

func newAllocationAskPriority(allocKey, appID string, res *resources.Resource, priority int32) *AllocationAsk {
	return newAllocationAskAll(allocKey, appID, "", res, 1, false, priority)
}

func newAllocationAskRepeat(allocKey, appID string, res *resources.Resource, repeat int) *AllocationAsk {
	return newAllocationAskAll(allocKey, appID, "", res, repeat, false, 0)
}

func newAllocationAskTG(allocKey, appID, taskGroup string, res *resources.Resource, repeat int) *AllocationAsk {
	return newAllocationAskAll(allocKey, appID, taskGroup, res, repeat, taskGroup != "", 0)
}

func newAllocationAskAll(allocKey, appID, taskGroup string, res *resources.Resource, repeat int, placeholder bool, priority int32) *AllocationAsk {
	ask := &si.AllocationAsk{
		AllocationKey:  allocKey,
		ApplicationID:  appID,
		PartitionName:  "default",
		ResourceAsk:    res.ToProto(),
		MaxAllocations: int32(repeat),
		TaskGroupName:  taskGroup,
		Placeholder:    placeholder,
		Priority:       priority,
	}
	return NewAllocationAskFromSI(ask)
}

func getTestUserGroup() security.UserGroup {
	return security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}
}

func getUserGroup(userName string, groupNameList []string) security.UserGroup {
	return security.UserGroup{User: userName, Groups: groupNameList}
}

func assertUserGroupResource(t *testing.T, userGroup security.UserGroup, expected *resources.Resource) {
	ugm := ugm.GetUserManager()
	userResource := ugm.GetUserResources(userGroup)
	groupResource := ugm.GetGroupResources(userGroup.Groups[0])
	assert.Equal(t, resources.Equals(userResource, expected), true)
	assert.Equal(t, resources.Equals(groupResource, expected), true)
}

func assertUserResourcesAndGroupResources(t *testing.T, userGroup security.UserGroup, expectedUserResources *resources.Resource, expectedGroupResources *resources.Resource, i int) {
	ugm := ugm.GetUserManager()
	userResource := ugm.GetUserResources(userGroup)
	groupResource := ugm.GetGroupResources(userGroup.Groups[i])
	assert.Equal(t, resources.Equals(userResource, expectedUserResources), true)
	assert.Equal(t, resources.Equals(groupResource, expectedGroupResources), true)
}
