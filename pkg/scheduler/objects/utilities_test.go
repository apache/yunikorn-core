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

	"github.com/google/btree"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-core/pkg/rmproxy"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	appID0        = "app-0"
	appID1        = "app-1"
	appID2        = "app-2"
	appID3        = "app-3"
	aKey          = "alloc-1"
	aKey2         = "alloc-2"
	aKey3         = "alloc-3"
	nodeID1       = "node-1"
	nodeID2       = "node-2"
	instType1     = "itype-1"
	testgroup     = "testgroup"
	foreignAlloc1 = "foreign-1"
	foreignAlloc2 = "foreign-2"
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
	sn := &Node{
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
		nodeEvents:        schedEvt.NewNodeEvents(events.GetEventSystem()),
	}
	return sn
}

func newProto(nodeID string, totalResource *resources.Resource, attributes map[string]string) *si.NodeInfo {
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

	return &proto
}

// Create a new Allocation with a random allocation key
func newAllocation(appID, nodeID string, res *resources.Resource) *Allocation {
	allocKey := strconv.FormatInt((time.Now()).UnixNano(), 10)
	return newAllocationAll(allocKey, appID, nodeID, "", res, false, 0)
}

// Create a new Allocation with a specified allocation key
func newAllocationWithKey(allocKey, appID, nodeID string, res *resources.Resource) *Allocation {
	return newAllocationAll(allocKey, appID, nodeID, "", res, false, 0)
}

// Create a new foreign Allocation with a specified allocation key
func newForeignAllocation(allocKey, nodeID string, res *resources.Resource) *Allocation {
	return NewAllocationFromSI(&si.Allocation{
		AllocationKey: allocKey,
		AllocationTags: map[string]string{
			siCommon.Foreign: siCommon.AllocTypeDefault,
		},
		ResourcePerAlloc: res.ToProto(),
		NodeID:           nodeID,
	})
}

// Create a new Allocation with a random ask key
func newPlaceholderAlloc(appID, nodeID string, res *resources.Resource, taskGroup string) *Allocation {
	allocKey := strconv.FormatInt((time.Now()).UnixNano(), 10)
	return newAllocationAll(allocKey, appID, nodeID, taskGroup, res, true, 0)
}

func newAllocationAsk(allocKey, appID string, res *resources.Resource) *Allocation {
	return newAllocationAskAll(allocKey, appID, "", res, false, 0)
}

func newAllocationAskPriority(allocKey, appID string, res *resources.Resource, priority int32) *Allocation {
	return newAllocationAskAll(allocKey, appID, "", res, false, priority)
}

func newAllocationAskTG(allocKey, appID, taskGroup string, res *resources.Resource) *Allocation {
	return newAllocationAskAll(allocKey, appID, taskGroup, res, taskGroup != "", 0)
}

func newAllocationAskAll(allocKey, appID, taskGroup string, res *resources.Resource, placeholder bool, priority int32) *Allocation {
	ask := &si.Allocation{
		AllocationKey:    allocKey,
		ApplicationID:    appID,
		PartitionName:    "default",
		ResourcePerAlloc: res.ToProto(),
		TaskGroupName:    taskGroup,
		Placeholder:      placeholder,
		Priority:         priority,
	}
	return NewAllocationFromSI(ask)
}

func newAllocationAll(allocKey, appID, nodeID, taskGroup string, res *resources.Resource, placeholder bool, priority int32) *Allocation {
	alloc := &si.Allocation{
		AllocationKey:    allocKey,
		ApplicationID:    appID,
		PartitionName:    "default",
		ResourcePerAlloc: res.ToProto(),
		TaskGroupName:    taskGroup,
		Placeholder:      placeholder,
		Priority:         priority,
		NodeID:           nodeID,
	}
	return NewAllocationFromSI(alloc)
}

func getTestUserGroup() security.UserGroup {
	return security.UserGroup{User: "testuser", Groups: []string{"testgroup"}}
}

func getUserGroup(userName string, groupNameList []string) security.UserGroup {
	return security.UserGroup{User: userName, Groups: groupNameList}
}

func assertUserGroupResource(t *testing.T, userGroup security.UserGroup, expected *resources.Resource) {
	assertUserResourcesAndGroupResources(t, userGroup, expected, nil, 0)
}

func assertUserResourcesAndGroupResources(t *testing.T, userGroup security.UserGroup, expectedUserResources *resources.Resource, expectedGroupResources *resources.Resource, i int) {
	m := ugm.GetUserManager()
	userResource := m.GetUserResources(userGroup.User)
	if expectedUserResources == nil {
		assert.Assert(t, userResource.IsEmpty(), "expected empty resource in user tracker")
	} else {
		assert.Assert(t, resources.Equals(userResource, expectedUserResources), "user value '%s' not equal to expected '%s'", userResource.String(), expectedUserResources.String())
	}
	groupResource := m.GetGroupResources(userGroup.Groups[i])
	if expectedGroupResources == nil {
		assert.Assert(t, groupResource.IsEmpty(), "expected empty resource in group tracker")
	} else {
		assert.Assert(t, resources.Equals(groupResource, expectedGroupResources), "group value '%s' not equal to expected '%s'", groupResource.String(), expectedGroupResources.String())
	}
}

func assertAllocationLog(t *testing.T, ask *Allocation) {
	log := ask.GetAllocationLog()
	preemptionPreconditionsFailed := false
	PreemptionDoesNotHelp := false
	for _, l := range log {
		if l.Message == common.PreemptionPreconditionsFailed {
			preemptionPreconditionsFailed = true
		} else if l.Message == common.PreemptionDoesNotHelp {
			PreemptionDoesNotHelp = true
		}
	}
	assert.Assert(t, preemptionPreconditionsFailed)
	assert.Assert(t, PreemptionDoesNotHelp)
}

func getNodeIteratorFn(nodes ...*Node) func() NodeIterator {
	tree := btree.New(7)
	for _, node := range nodes {
		tree.ReplaceOrInsert(nodeRef{
			node, 1,
		})
	}

	return func() NodeIterator {
		return NewTreeIterator(acceptAll, func() *btree.BTree {
			return tree
		})
	}
}
