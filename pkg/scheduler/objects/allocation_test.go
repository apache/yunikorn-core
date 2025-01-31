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
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/events/mock"
	schedEvt "github.com/apache/yunikorn-core/pkg/scheduler/objects/events"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const past = 1640995200 // 2022-1-1 00:00:00

func TestNewAsk(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	siAsk := &si.Allocation{
		AllocationKey:    "ask-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
	}
	ask := NewAllocationFromSI(siAsk)
	if ask == nil {
		t.Fatal("NewAllocationAskFromSI create failed while it should not")
	}
	askStr := ask.String()
	expected := "allocationKey ask-1, applicationID app-1, Resource map[first:10], Allocated false"
	assert.Equal(t, askStr, expected, "Strings should have been equal")
}

func TestAskAllocateDeallocate(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	assert.Assert(t, !ask.IsAllocated(), "pending ask should return false for IsAllocated()")
	assert.Assert(t, !ask.deallocate(), "attempt to deallocate pending ask should fail")
	assert.Assert(t, ask.allocate(), "attempt to allocate pending ask should not fail")
	assert.Assert(t, ask.IsAllocated(), "allocated ask should return true for IsAllocated()")
	assert.Assert(t, !ask.allocate(), "attempt to allocate previously allocated ask should fail")
	assert.Assert(t, ask.deallocate(), "deallocating previously allocated ask should succeed")
	assert.Assert(t, !ask.IsAllocated(), "deallocated ask should return false for IsAllocated()")
}

// the create time should not be manipulated but we need it for reservation testing
func TestGetCreateTime(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	ask := newAllocationAsk("alloc-1", "app-1", res)
	created := ask.GetCreateTime()
	// move time 10 seconds back
	ask.createTime = created.Add(time.Second * -10)
	createdNow := ask.GetCreateTime()
	if createdNow.Equal(created) {
		t.Fatal("create time stamp should have been modified")
	}
}

func TestPreemptionPolicy(t *testing.T) {
	ask1 := NewAllocationFromSI(&si.Allocation{
		AllocationKey:    "allow-self-deny-other",
		ApplicationID:    "allow-self-deny-other",
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: true, AllowPreemptOther: false}})
	assert.Check(t, ask1.IsAllowPreemptSelf(), "preempt self not allowed")
	assert.Check(t, !ask1.IsAllowPreemptOther(), "preempt other allowed")

	ask2 := NewAllocationFromSI(&si.Allocation{
		AllocationKey:    "deny-self-allow-other",
		ApplicationID:    "deny-self-allow-other",
		PreemptionPolicy: &si.PreemptionPolicy{AllowPreemptSelf: false, AllowPreemptOther: true}})
	assert.Check(t, !ask2.IsAllowPreemptSelf(), "preempt self allowed")
	assert.Check(t, ask2.IsAllowPreemptOther(), "preempt other not allowed")
}

func TestPreemptCheckTime(t *testing.T) {
	siAsk := &si.Allocation{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
	}
	ask := NewAllocationFromSI(siAsk)
	assert.Equal(t, ask.GetPreemptCheckTime(), time.Time{}, "preemptCheckTime was not default")

	now := time.Now().Add(-1 * time.Second)
	ask.UpdatePreemptCheckTime()
	assert.Check(t, now.Before(ask.GetPreemptCheckTime()), "preemptCheckTime was not current")
}

func TestPlaceHolder(t *testing.T) {
	siAsk := &si.Allocation{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
	}
	ask := NewAllocationFromSI(siAsk)
	assert.Assert(t, !ask.IsPlaceholder(), "standard ask should not be a placeholder")
	assert.Equal(t, ask.GetTaskGroup(), "", "standard ask should not have a TaskGroupName")

	siAsk = &si.Allocation{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
		TaskGroupName: "",
		Placeholder:   true,
	}
	ask = NewAllocationFromSI(siAsk)
	var nilAsk *Allocation
	assert.Equal(t, ask, nilAsk, "placeholder ask created without a TaskGroupName")

	siAsk.TaskGroupName = "TestPlaceHolder"
	ask = NewAllocationFromSI(siAsk)
	assert.Assert(t, ask != nilAsk, "placeholder ask creation failed unexpectedly")
	assert.Assert(t, ask.IsPlaceholder(), "ask should have been a placeholder")
	assert.Equal(t, ask.GetTaskGroup(), siAsk.TaskGroupName, "TaskGroupName not set as expected")
}

func TestGetRequiredNode(t *testing.T) {
	tag := make(map[string]string)
	// unset case
	siAsk := &si.Allocation{
		AllocationKey:  "ask1",
		ApplicationID:  "app1",
		PartitionName:  "default",
		AllocationTags: tag,
	}
	ask := NewAllocationFromSI(siAsk)
	assert.Equal(t, ask.GetRequiredNode(), "", "required node is empty as expected")
	// set case
	tag[siCommon.DomainYuniKorn+siCommon.KeyRequiredNode] = "NodeName"
	siAsk = &si.Allocation{
		AllocationKey:  "ask1",
		ApplicationID:  "app1",
		PartitionName:  "default",
		AllocationTags: tag,
	}
	ask = NewAllocationFromSI(siAsk)
	assert.Equal(t, ask.GetRequiredNode(), "NodeName", "required node should be NodeName")
}

func TestAllocationLog(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	siAsk := &si.Allocation{
		AllocationKey:    "ask-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
	}
	ask := NewAllocationFromSI(siAsk)

	// log a reservation event
	ask.LogAllocationFailure("reserve1", false)
	log := sortedLog(ask)
	assert.Equal(t, 0, len(log), "non-allocation events was logged")

	// log an allocation event
	ask.LogAllocationFailure("alloc1", true)
	log = sortedLog(ask)
	assert.Equal(t, 1, len(log), "allocation event should be logged")
	assert.Equal(t, "alloc1", log[0].Message, "wrong message for event 1")
	assert.Equal(t, 1, int(log[0].Count), "wrong count for event 1")

	// add a second allocation event
	ask.LogAllocationFailure("alloc2", true)
	log = sortedLog(ask)
	assert.Equal(t, 2, len(log), "allocation event 2 should be logged")
	assert.Equal(t, "alloc2", log[0].Message, "wrong message for event 1")
	assert.Equal(t, "alloc1", log[1].Message, "wrong message for event 2")
	assert.Equal(t, 1, int(log[0].Count), "wrong count for event 1")
	assert.Equal(t, 1, int(log[1].Count), "wrong count for event 2")

	// duplicate the first one
	ask.LogAllocationFailure("alloc1", true)
	log = sortedLog(ask)
	assert.Equal(t, 2, len(log), "allocation event alloc1 (#2) should not create a new event")
	assert.Equal(t, "alloc1", log[0].Message, "wrong message for event 1")
	assert.Equal(t, "alloc2", log[1].Message, "wrong message for event 2")
	assert.Equal(t, 2, int(log[0].Count), "wrong count for event 1")
	assert.Equal(t, 1, int(log[1].Count), "wrong count for event 2")
}

func TestSendPredicateFailed(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 10})
	siAsk := &si.Allocation{
		AllocationKey:    "ask-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
	}
	ask := NewAllocationFromSI(siAsk)
	eventSystem := mock.NewEventSystemDisabled()
	ask.askEvents = schedEvt.NewAskEvents(eventSystem)
	ask.SendPredicatesFailedEvent(map[string]int{})
	assert.Equal(t, 0, len(eventSystem.Events))

	eventSystem = mock.NewEventSystem()
	ask.askEvents = schedEvt.NewAskEvents(eventSystem)
	ask.SendPredicatesFailedEvent(map[string]int{
		"failure": 1,
	})
	assert.Equal(t, 1, len(eventSystem.Events))
	event := eventSystem.Events[0]
	assert.Equal(t, "Unschedulable request 'ask-1': failure (1x); ", event.Message)
}

func TestCreateTime(t *testing.T) {
	siAsk := &si.Allocation{
		AllocationKey: "ask1",
		ApplicationID: "app1",
		PartitionName: "default",
	}
	siAsk.AllocationTags = map[string]string{siCommon.CreationTime: "1622530800"}
	ask := NewAllocationFromSI(siAsk)
	assert.Equal(t, ask.GetTag(siCommon.CreationTime), "1622530800")
	assert.Equal(t, ask.GetTag("unknown"), "")
	assert.Equal(t, ask.GetCreateTime(), time.Unix(1622530800, 0))
}

func sortedLog(ask *Allocation) []*AllocationLogEntry {
	log := ask.GetAllocationLog()
	sort.SliceStable(log, func(i int, j int) bool {
		return log[i].LastOccurrence.After(log[j].LastOccurrence)
	})
	return log
}

func TestNewAlloc(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	alloc := newAllocationAsk("ask-1", "app-1", res)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")
	assert.Assert(t, !alloc.IsPlaceholder(), "ask should not have been a placeholder")
	assert.Equal(t, time.Now().Round(time.Second), alloc.GetCreateTime().Round(time.Second))
	assert.Equal(t, alloc.GetInstanceType(), "", "Default instance type should be empty")
	alloc.SetInstanceType(instType1)
	assert.Equal(t, alloc.GetInstanceType(), instType1, "Instance type not set as expected")
	allocStr := alloc.String()
	expected := "allocationKey ask-1, applicationID app-1, Resource map[first:1], Allocated false"
	assert.Equal(t, allocStr, expected, "Strings should have been equal")
	assert.Assert(t, !alloc.IsPlaceholderUsed(), fmt.Sprintf("Alloc should not be placeholder replacement by default: got %t, expected %t", alloc.IsPlaceholderUsed(), false))
}

func TestSetAllocatedResources(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	res2, err := resources.NewResourceFromConf(map[string]string{"first": "2"})
	assert.NilError(t, err, "Resource creation failed")
	alloc := newAllocationAsk("ask-1", "app-1", res)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")

	alloc.SetAllocatedResource(res2)
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res2), "Allocated resource not set correctly")
}

func TestNewAllocatedAllocationResult(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	alloc := newAllocation("ask-1", "app-1", res)
	result := newAllocatedAllocationResult("node-1", alloc)
	if result == nil {
		t.Fatal("NewAllocatedAllocationResult create failed while it should not")
	}
	assert.Equal(t, result.ResultType, Allocated, "NewAllocatedAllocationResult should have Allocated result type")
	assert.Equal(t, alloc, result.Request, "wrong allocation")
	assert.Equal(t, result.NodeID, "node-1", "wrong node id")
}

func TestNewReservedAllocationResult(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	result := newReservedAllocationResult("node-1", ask)
	if result == nil {
		t.Fatal("NewReservedAllocationResult create failed while it should not")
	}
	assert.Equal(t, result.ResultType, Reserved, "NewReservedAllocationResult should have Reserved result type")
	assert.Equal(t, ask, result.Request, "wrong ask")
	assert.Equal(t, result.NodeID, "node-1", "wrong node id")
}

func TestNewUnreservedAllocationResult(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	result := newUnreservedAllocationResult("node-1", ask)
	if result == nil {
		t.Fatal("NewReservedAllocationResult create failed while it should not")
	}
	assert.Equal(t, result.ResultType, Unreserved, "NewReservedAllocationResult should have Reserved result type")
	assert.Equal(t, ask, result.Request, "wrong ask")
	assert.Equal(t, result.NodeID, "node-1", "wrong node id")
}

func TestNewReplacedAllocationResult(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	alloc := newAllocationWithKey("ask-1", "app-1", "node-1", res)
	result := newReplacedAllocationResult("node-1", alloc)
	if result == nil {
		t.Fatal("NewReplacedllocationResult create failed while it should not")
	}
	assert.Equal(t, result.ResultType, Replaced, "NewReplacedAllocationResult should have Allocated result type")
	assert.Equal(t, alloc, result.Request, "wrong allocation")
	assert.Equal(t, result.NodeID, "node-1", "wrong node id")
}

func TestAllocationResultString(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	result := &AllocationResult{
		ResultType:     Allocated,
		NodeID:         "node-1",
		ReservedNodeID: "node-2",
		Request:        ask,
	}
	assert.Equal(t, result.String(), "resultType=Allocated, nodeID=node-1, reservedNodeID=node-2, allocationKey=ask-1", "wrong content")

	// validate nil ask
	result.Request = nil
	assert.Equal(t, result.String(), "resultType=Allocated, nodeID=node-1, reservedNodeID=node-2, allocationKey=", "wrong content")

	// validate nil result
	result = nil
	assert.Equal(t, result.String(), "nil allocation result", "wrong content")
}

func TestSIFromNilAlloc(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil allocation")
		}
	}()
	var alloc *Allocation
	var nilSI *si.Allocation
	// ignore nil check from IDE we really want to do this
	allocSI := alloc.NewSIFromAllocation()
	assert.Equal(t, allocSI, nilSI, "Expected nil response from nil allocation")
}

//nolint:staticcheck
func TestSIFromAlloc(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	expectedSI := &si.Allocation{
		AllocationKey:    "ask-1",
		NodeID:           "node-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
		Originator:       true,
		PreemptionPolicy: &si.PreemptionPolicy{
			AllowPreemptSelf:  true,
			AllowPreemptOther: false,
		},
	}
	alloc := newAllocationWithKey("ask-1", "app-1", "node-1", res)
	alloc.originator = true
	alloc.allowPreemptSelf = false
	alloc.allowPreemptOther = true
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}

	allocSI := alloc.NewSIFromAllocation()
	assert.Equal(t, expectedSI.AllocationKey, allocSI.AllocationKey, "wrong AllocationKey")
	assert.Equal(t, expectedSI.NodeID, allocSI.NodeID, "wrong NodeID")
	assert.Equal(t, expectedSI.ApplicationID, allocSI.ApplicationID, "wrong ApplicationID")
	assert.Check(t, allocSI.Originator, "originator flag should be set")
	assert.Check(t, !allocSI.PreemptionPolicy.AllowPreemptSelf, "allowPreemptSelf flag should not be set")
	assert.Check(t, allocSI.PreemptionPolicy.AllowPreemptOther, "aloowPreemptOther flag should be set")

	alloc.originator = false
	alloc.allowPreemptSelf = true
	alloc.allowPreemptOther = false
	allocSI = alloc.NewSIFromAllocation()
	assert.Check(t, !allocSI.Originator, "originator flag should not be set")
	assert.Check(t, allocSI.PreemptionPolicy.AllowPreemptSelf, "allowPreemptSelf flag should be set")
	assert.Check(t, !allocSI.PreemptionPolicy.AllowPreemptOther, "aloowPreemptOther flag should not be set")
}

func TestNewAllocFromNilSI(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil SI allocation")
		}
	}()
	var nilAlloc *Allocation
	alloc := NewAllocationFromSI(nil)
	assert.Equal(t, alloc, nilAlloc, "Expected nil response from nil SI allocation")
}

func TestNewAllocFromSI(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	tags := make(map[string]string)
	tags[siCommon.CreationTime] = strconv.FormatInt(past, 10)
	allocSI := &si.Allocation{
		AllocationKey:    "ask-1",
		NodeID:           "node-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
		TaskGroupName:    "",
		Placeholder:      true,
		AllocationTags:   tags,
		Originator:       true,
		PreemptionPolicy: &si.PreemptionPolicy{
			AllowPreemptSelf:  false,
			AllowPreemptOther: true,
		},
	}
	var nilAlloc *Allocation
	alloc := NewAllocationFromSI(allocSI)
	assert.Equal(t, alloc, nilAlloc, "placeholder allocation created without a TaskGroupName")
	allocSI.TaskGroupName = "TestNewAllocFromSI"
	alloc = NewAllocationFromSI(allocSI)
	assert.Assert(t, alloc != nilAlloc, "placeholder ask creation failed unexpectedly")
	assert.Assert(t, alloc.IsPlaceholder(), "allocation should have been a placeholder")
	assert.Equal(t, alloc.GetTaskGroup(), allocSI.TaskGroupName, "TaskGroupName not set as expected")
	assert.Equal(t, alloc.GetCreateTime(), time.Unix(past, 0)) //nolint:staticcheck
	assert.Assert(t, alloc.IsOriginator(), "allocation should have been an originator")
	assert.Assert(t, !alloc.IsAllowPreemptSelf(), "ask should not have allow-preempt-self set")
	assert.Assert(t, alloc.IsAllowPreemptOther(), "ask should have allow-preempt-other set")
	assert.Assert(t, reflect.DeepEqual(alloc.tags, alloc.GetTagsClone()))

	allocSI.Originator = false
	allocSI.PreemptionPolicy.AllowPreemptSelf = true
	allocSI.PreemptionPolicy.AllowPreemptOther = false
	allocSI.AllocationTags[siCommon.CreationTime] = "xyz"
	startTime := time.Now().Unix()
	alloc = NewAllocationFromSI(allocSI)
	endTime := time.Now().Unix()
	assert.Assert(t, alloc.GetCreateTime().Unix() >= startTime, "alloc create time is too early")
	assert.Assert(t, alloc.GetCreateTime().Unix() <= endTime, "alloc create time is too late")
	assert.Assert(t, !alloc.IsOriginator(), "alloc should not have been an originator")
	assert.Assert(t, alloc.IsAllowPreemptSelf(), "alloc should have allow-preempt-self set")
	assert.Assert(t, !alloc.IsAllowPreemptOther(), "alloc should not have allow-preempt-other set")

	allocSI.PreemptionPolicy = nil
	alloc = NewAllocationFromSI(allocSI)
	assert.Assert(t, !alloc.IsAllowPreemptSelf(), "alloc should not have allow-preempt-self set")
	assert.Assert(t, !alloc.IsAllowPreemptOther(), "alloc should not have allow-preempt-other set")
}

func TestNewForeignAllocFromSI(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": 1,
	})
	siAlloc := &si.Allocation{
		AllocationKey:    "foreign-1",
		NodeID:           "node-1",
		ResourcePerAlloc: res.ToProto(),
		TaskGroupName:    "",
		AllocationTags: map[string]string{
			siCommon.Foreign: siCommon.AllocTypeDefault,
		},
	}

	// default
	alloc := NewAllocationFromSI(siAlloc)
	assert.Assert(t, alloc.IsPreemptable())
	assert.Assert(t, alloc.IsForeign())
	assert.Equal(t, "foreign-1", alloc.GetAllocationKey())
	assert.Equal(t, "node-1", alloc.GetNodeID())
	assert.Assert(t, resources.Equals(res, alloc.GetAllocatedResource()))

	// static
	siAlloc.AllocationTags[siCommon.Foreign] = siCommon.AllocTypeStatic
	alloc = NewAllocationFromSI(siAlloc)
	assert.Assert(t, !alloc.IsPreemptable())

	// illegal value for foreign type
	siAlloc.AllocationTags[siCommon.Foreign] = "xyz"
	alloc = NewAllocationFromSI(siAlloc)
	assert.Assert(t, alloc.IsPreemptable())
}
