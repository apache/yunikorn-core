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
	expectedSI := &si.Allocation{
		AllocationKey: "ask-1",
		UUID:          "test-uuid",
		NodeID:        "node-1",
		ApplicationID: "app-1",
	}
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "Resource creation failed")
	ask := newAllocationAsk("ask-1", "app-1", res)
	alloc := NewAllocation("test-uuid", "node-1", ask)
	if alloc == nil {
		t.Fatal("NewAllocation create failed while it should not")
	}
	allocSI := alloc.NewSIFromAllocation()
	assert.DeepEqual(t, allocSI, expectedSI)
}
