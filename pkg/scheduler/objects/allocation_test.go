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
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	siCommon "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const past = 1640995200 // 2022-1-1 00:00:00

func TestAllocToString(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("String panic on nil allocation")
		}
	}()
	var alloc *Allocation
	// ignore nil check from IDE we really want to do this
	allocString := alloc.String()
	assert.Equal(t, allocString, "nil allocation", "Unexpected string returned for nil allocation")
}

func TestNewAlloc(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := NewAllocation("node-1", ask)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	assert.Equal(t, alloc.GetAllocationKey(), "ask-1")
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")
	assert.Assert(t, !alloc.IsPlaceholder(), "ask should not have been a placeholder")
	assert.Equal(t, time.Now().Round(time.Second), alloc.GetCreateTime().Round(time.Second))
	assert.Equal(t, alloc.GetInstanceType(), "", "Default instance type should be empty")
	alloc.SetInstanceType(instType1)
	assert.Equal(t, alloc.GetInstanceType(), instType1, "Instance type not set as expected")
	allocStr := alloc.String()
	expected := "applicationID=app-1, allocationKey=ask-1, Node=node-1"
	assert.Equal(t, allocStr, expected, "Strings should have been equal")
	assert.Assert(t, !alloc.IsPlaceholderUsed(), fmt.Sprintf("Alloc should not be placeholder replacement by default: got %t, expected %t", alloc.IsPlaceholderUsed(), false))
	// check that createTime is properly copied from the ask
	tags := make(map[string]string)
	tags[siCommon.CreationTime] = strconv.FormatInt(past, 10)
	ask.tags = CloneAllocationTags(tags)
	ask.createTime = time.Unix(past, 0)
	alloc = NewAllocation("node-1", ask)
	assert.Equal(t, alloc.GetCreateTime(), ask.GetCreateTime(), "createTime was not copied from the ask")
	assert.Assert(t, reflect.DeepEqual(ask.tags, ask.GetTagsClone()))
}

func TestNewAllocatedAllocationResult(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := NewAllocation("node-1", ask)
	result := newAllocatedAllocationResult("node-1", ask, alloc)
	if result == nil {
		t.Fatal("NewAllocatedAllocationResult create failed while it should not")
	}
	assert.Equal(t, result.ResultType, Allocated, "NewAllocatedAllocationResult should have Allocated result type")
	assert.Equal(t, ask, result.Ask, "wrong ask")
	assert.Equal(t, alloc, result.Allocation, "wrong allocation")
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
	assert.Equal(t, ask, result.Ask, "wrong ask")
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
	assert.Equal(t, ask, result.Ask, "wrong ask")
	assert.Equal(t, result.NodeID, "node-1", "wrong node id")
}

func TestNewReplacedAllocationResult(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := NewAllocation("node-1", ask)
	result := newReplacedAllocationResult("node-1", ask, alloc)
	if result == nil {
		t.Fatal("NewReplacedllocationResult create failed while it should not")
	}
	assert.Equal(t, result.ResultType, Replaced, "NewReplacedAllocationResult should have Allocated result type")
	assert.Equal(t, ask, result.Ask, "wrong ask")
	assert.Equal(t, alloc, result.Allocation, "wrong allocation")
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
		Ask:            ask,
	}
	assert.Equal(t, result.String(), "resultType=Allocated, nodeID=node-1, reservedNodeID=node-2, allocationKey=ask-1", "wrong content")

	// validate nil ask
	result.Ask = nil
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
	ask := newAllocationAsk("ask-1", "app-1", res)
	ask.originator = true
	ask.allowPreemptSelf = false
	ask.allowPreemptOther = true
	alloc := NewAllocation("node-1", ask)
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
	alloc.ask.allowPreemptSelf = true
	alloc.ask.allowPreemptOther = false
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
	assert.Assert(t, alloc.GetAsk().IsPlaceholder(), "ask should have been a placeholder")
	assert.Assert(t, alloc.IsPlaceholder(), "allocation should have been a placeholder")
	assert.Equal(t, alloc.GetTaskGroup(), allocSI.TaskGroupName, "TaskGroupName not set as expected")
	assert.Equal(t, alloc.GetAsk().GetCreateTime(), time.Unix(past, 0)) //nolint:staticcheck
	assert.Assert(t, alloc.GetAsk().IsOriginator(), "ask should have been an originator")
	assert.Assert(t, alloc.IsOriginator(), "allocation should have been an originator")
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptSelf(), "ask should not have allow-preempt-self set")
	assert.Assert(t, alloc.GetAsk().IsAllowPreemptOther(), "ask should have allow-preempt-other set")
	assert.Assert(t, reflect.DeepEqual(alloc.tags, alloc.GetTagsClone()))

	allocSI.Originator = false
	allocSI.PreemptionPolicy.AllowPreemptSelf = true
	allocSI.PreemptionPolicy.AllowPreemptOther = false
	allocSI.AllocationTags[siCommon.CreationTime] = "xyz"
	startTime := time.Now().Unix()
	alloc = NewAllocationFromSI(allocSI)
	endTime := time.Now().Unix()
	assert.Assert(t, alloc.GetAsk().GetCreateTime().Unix() >= startTime, "alloc create time is too early")
	assert.Assert(t, alloc.GetAsk().GetCreateTime().Unix() <= endTime, "alloc create time is too late")
	assert.Assert(t, !alloc.GetAsk().IsOriginator(), "ask should not have been an originator")
	assert.Assert(t, !alloc.IsOriginator(), "allocation should not have been an originator")
	assert.Assert(t, alloc.GetAsk().IsAllowPreemptSelf(), "ask should have allow-preempt-self set")
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptOther(), "ask should not have allow-preempt-other set")

	allocSI.PreemptionPolicy = nil
	alloc = NewAllocationFromSI(allocSI)
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptSelf(), "ask should not have allow-preempt-self set")
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptOther(), "ask should not have allow-preempt-other set")
}
