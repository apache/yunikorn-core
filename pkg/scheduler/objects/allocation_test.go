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
	assert.Equal(t, alloc.GetResult(), Allocated, "New alloc should default to result Allocated")
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")
	assert.Assert(t, !alloc.IsPlaceholder(), "ask should not have been a placeholder")
	assert.Equal(t, time.Now().Round(time.Second), alloc.GetCreateTime().Round(time.Second))
	assert.Equal(t, alloc.GetInstanceType(), "", "Default instance type should be empty")
	alloc.SetInstanceType(instType1)
	assert.Equal(t, alloc.GetInstanceType(), instType1, "Instance type not set as expected")
	allocStr := alloc.String()
	expected := "applicationID=app-1, allocationKey=ask-1, Node=node-1, result=Allocated"
	assert.Equal(t, allocStr, expected, "Strings should have been equal")
	assert.Assert(t, !alloc.IsPlaceholderUsed(), fmt.Sprintf("Alloc should not be placeholder replacement by default: got %t, expected %t", alloc.IsPlaceholderUsed(), false))
	created := alloc.GetCreateTime()
	// move time 10 seconds back
	alloc.SetCreateTime(created.Add(time.Second * -10))
	createdNow := alloc.GetCreateTime()
	if createdNow.Equal(created) {
		t.Fatal("create time stamp should have been modified")
	}
	// check that createTime is properly copied from the ask
	tags := make(map[string]string)
	tags[siCommon.CreationTime] = strconv.FormatInt(past, 10)
	ask.tags = CloneAllocationTags(tags)
	ask.createTime = time.Unix(past, 0)
	alloc = NewAllocation("node-1", ask)
	assert.Equal(t, alloc.GetCreateTime(), ask.GetCreateTime(), "createTime was not copied from the ask")
}

func TestNewReservedAlloc(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := newReservedAllocation("node-1", ask)
	if alloc == nil {
		t.Fatal("NewReservedAllocation create failed while it should not")
	}
	assert.Equal(t, alloc.GetResult(), Reserved, "NewReservedAlloc should have Reserved result")
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")
	allocStr := alloc.String()
	expected := "applicationID=app-1, allocationKey=ask-1, Node=node-1, result=Reserved"
	assert.Equal(t, allocStr, expected, "Strings should have been equal")
}

func TestNewUnreservedAlloc(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := newUnreservedAllocation("node-1", ask)
	if alloc == nil {
		t.Fatal("NewReservedAllocation create failed while it should not")
	}
	assert.Equal(t, alloc.GetResult(), Unreserved, "NewReservedAlloc should have Reserved result")
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")
	allocStr := alloc.String()
	expected := "applicationID=app-1, allocationKey=ask-1, Node=node-1, result=Unreserved"
	assert.Equal(t, allocStr, expected, "Strings should have been equal")
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
	alloc := NewAllocation("node-1", ask)
	ask.originator = true
	ask.allowPreemptSelf = false
	ask.allowPreemptOther = true
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

	alloc.ask.originator = false
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
	allocSI.TaskGroupName = "testgroup"
	alloc = NewAllocationFromSI(allocSI)
	assert.Assert(t, alloc != nilAlloc, "placeholder ask creation failed unexpectedly")
	assert.Assert(t, alloc.IsPlaceholder(), "ask should have been a placeholder")
	assert.Equal(t, alloc.GetTaskGroup(), "testgroup", "TaskGroupName not set as expected")
	assert.Equal(t, alloc.GetAsk().GetCreateTime(), time.Unix(past, 0)) //nolint:staticcheck
	assert.Assert(t, alloc.GetAsk().IsOriginator(), "ask should have been an originator")
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptSelf(), "ask should not have allow-preempt-self set")
	assert.Assert(t, alloc.GetAsk().IsAllowPreemptOther(), "ask should have allow-preempt-other set")

	allocSI.Originator = false
	allocSI.PreemptionPolicy.AllowPreemptSelf = true
	allocSI.PreemptionPolicy.AllowPreemptOther = false
	allocSI.AllocationTags[siCommon.CreationTime] = "xyz"
	alloc = NewAllocationFromSI(allocSI)
	assert.Equal(t, alloc.GetAsk().GetCreateTime().Unix(), int64(-1)) //nolint:staticcheck
	assert.Assert(t, !alloc.GetAsk().IsOriginator(), "ask should not have been an originator")
	assert.Assert(t, alloc.GetAsk().IsAllowPreemptSelf(), "ask should have allow-preempt-self set")
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptOther(), "ask should not have allow-preempt-other set")

	allocSI.PreemptionPolicy = nil
	alloc = NewAllocationFromSI(allocSI)
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptSelf(), "ask should not have allow-preempt-self set")
	assert.Assert(t, !alloc.GetAsk().IsAllowPreemptOther(), "ask should not have allow-preempt-other set")
}
