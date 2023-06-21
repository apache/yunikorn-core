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
	"github.com/google/go-cmp/cmp/cmpopts"
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
	alloc := NewAllocation("test-uuid", "node-1", "itype1", ask)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	assert.Equal(t, alloc.GetResult(), Allocated, "New alloc should default to result Allocated")
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")
	assert.Assert(t, !alloc.IsPlaceholder(), "ask should not have been a placeholder")
	assert.Equal(t, time.Now().Round(time.Second), alloc.GetCreateTime().Round(time.Second))
	allocStr := alloc.String()
	expected := "applicationID=app-1, uuid=test-uuid, allocationKey=ask-1, Node=node-1, result=Allocated"
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
	alloc = NewAllocation("test-uuid", "node-1", "itype1", ask)
	assert.Equal(t, alloc.GetCreateTime(), ask.GetCreateTime(), "createTime was not copied from the ask")
}

func TestNewReservedAlloc(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := newReservedAllocation(Reserved, "node-1", "instType1", ask)
	if alloc == nil {
		t.Fatal("NewReservedAllocation create failed while it should not")
	}
	assert.Equal(t, alloc.GetResult(), Reserved, "NewReservedAlloc should have Reserved result")
	assert.Equal(t, alloc.GetUUID(), "", "NewReservedAlloc should not have uuid")
	assert.Assert(t, resources.Equals(alloc.GetAllocatedResource(), res), "Allocated resource not set correctly")
	allocStr := alloc.String()
	expected := "applicationID=app-1, uuid=N/A, allocationKey=ask-1, Node=node-1, result=Reserved"
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
		UUID:             "test-uuid",
		NodeID:           "node-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
	}
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := NewAllocation("test-uuid", "node-1", "itype1", ask)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	allocSI := alloc.NewSIFromAllocation()
	assert.DeepEqual(t, allocSI, expectedSI, cmpopts.IgnoreUnexported(si.Allocation{}, si.Resource{}, si.Quantity{}))
}

func TestNewAllocFromNilSI(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatal("panic on nil SI allocation")
		}
	}()
	var nilAlloc *Allocation
	alloc := NewAllocationFromSI(nil, "itype1")
	assert.Equal(t, alloc, nilAlloc, "Expected nil response from nil SI allocation")
}

func TestNewAllocFromSI(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	tags := make(map[string]string)
	tags[siCommon.CreationTime] = strconv.FormatInt(past, 10)
	allocSI := &si.Allocation{
		AllocationKey:    "ask-1",
		UUID:             "test-uuid",
		NodeID:           "node-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
		TaskGroupName:    "",
		Placeholder:      true,
		AllocationTags:   tags,
	}
	var nilAlloc *Allocation
	alloc := NewAllocationFromSI(allocSI, "itype1")
	assert.Equal(t, alloc, nilAlloc, "placeholder allocation created without a TaskGroupName")
	allocSI.TaskGroupName = "testgroup"
	alloc = NewAllocationFromSI(allocSI, "itype1")
	assert.Assert(t, alloc != nilAlloc, "placeholder ask creation failed unexpectedly")
	assert.Assert(t, alloc.IsPlaceholder(), "ask should have been a placeholder")
	assert.Equal(t, alloc.GetTaskGroup(), "testgroup", "TaskGroupName not set as expected")
	assert.Equal(t, alloc.GetAsk().GetCreateTime(), time.Unix(past, 0)) //nolint:staticcheck

	allocSI.AllocationTags[siCommon.CreationTime] = "xyz"
	alloc = NewAllocationFromSI(allocSI, "itype1")
	assert.Equal(t, alloc.GetAsk().GetCreateTime().Unix(), int64(-1)) //nolint:staticcheck
}
