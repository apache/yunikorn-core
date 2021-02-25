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

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

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
	alloc := NewAllocation("test-uuid", "node-1", ask)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	assert.Equal(t, alloc.Result, Allocated, "New alloc should default to result Allocated")
	assert.Assert(t, resources.Equals(alloc.AllocatedResource, res), "Allocated resource not set correctly")
	assert.Assert(t, !alloc.IsPlaceholder(), "ask should not have been a placeholder")
	allocStr := alloc.String()
	expected := "ApplicationID=app-1, UUID=test-uuid, AllocationKey=ask-1, Node=node-1, Result=Allocated"
	assert.Equal(t, allocStr, expected, "Strings should have been equal")
}

func TestNewReservedAlloc(t *testing.T) {
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := newReservedAllocation(Reserved, "node-1", ask)
	if alloc == nil {
		t.Fatal("NewReservedAllocation create failed while it should not")
	}
	assert.Equal(t, alloc.Result, Reserved, "NewReservedAlloc should have Reserved result")
	assert.Equal(t, alloc.UUID, "", "NewReservedAlloc should not have UUID")
	assert.Assert(t, resources.Equals(alloc.AllocatedResource, res), "Allocated resource not set correctly")
	allocStr := alloc.String()
	expected := "ApplicationID=app-1, UUID=N/A, AllocationKey=ask-1, Node=node-1, Result=Reserved"
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
	alloc := NewAllocation("test-uuid", "node-1", ask)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	allocSI := alloc.NewSIFromAllocation()
	assert.DeepEqual(t, allocSI, expectedSI)
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
	allocSI := &si.Allocation{
		AllocationKey:    "ask-1",
		UUID:             "test-uuid",
		NodeID:           "node-1",
		ApplicationID:    "app-1",
		ResourcePerAlloc: res.ToProto(),
		TaskGroupName:    "",
		Placeholder:      true,
	}
	var nilAlloc *Allocation
	alloc := NewAllocationFromSI(allocSI)
	assert.Equal(t, alloc, nilAlloc, "placeholder allocation created without a TaskGroupName")
	allocSI.TaskGroupName = "testgroup"
	alloc = NewAllocationFromSI(allocSI)
	assert.Assert(t, alloc != nilAlloc, "placeholder ask creation failed unexpectedly")
	assert.Assert(t, alloc.IsPlaceholder(), "ask should have been a placeholder")
	assert.Equal(t, alloc.getTaskGroup(), "testgroup", "TaskGroupName not set as expected")
}
