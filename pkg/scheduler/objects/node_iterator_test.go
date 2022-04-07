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

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

// empty test for random iterator
func TestRoundRobinIteratorEmpty(t *testing.T) {
	// nil list
	rni, ok := NewRoundRobinNodeIterator(nil).(*roundRobinNodeIterator)
	if !ok {
		t.Fatal("wrong iterator type returned")
	}
	if rni == nil {
		t.Fatal("failed to create basic iterator")
	}
	if rni.HasNext() || rni.startIdx != 0 || rni.countIdx != 0 {
		t.Errorf("HasNext on nil node list should not have side effects: %v", rni)
	}
	if node := rni.Next(); node != nil {
		t.Errorf("nil node list does not have next node: %v", node)
	}

	// slice with a length of 0: first HasNext call
	rni, ok = NewRoundRobinNodeIterator(make([]*Node, 0)).(*roundRobinNodeIterator)
	if !ok {
		t.Fatal("wrong iterator type returned")
	}
	if rni == nil {
		t.Fatal("failed to create iterator with empty slice")
	}
	if rni.HasNext() || rni.startIdx != 0 || rni.countIdx != 0 {
		t.Errorf("HasNext on empty list should not have side effects: %v", rni)
	}
	if node := rni.Next(); node != nil {
		t.Errorf("empty node list does not have Next: %v", node)
	}
	// slice with a length of 0: direct Next call
	rni, ok = NewRoundRobinNodeIterator(make([]*Node, 0)).(*roundRobinNodeIterator)
	if !ok {
		t.Fatal("wrong iterator type returned")
	}
	if rni == nil {
		t.Fatal("failed to create iterator with empty slice")
	}
	// Next call first, then HasNext
	node := rni.Next()
	if node != nil || rni.startIdx != 0 || rni.countIdx != 0 {
		t.Errorf("Next on empty list should not have side effects: %v", rni)
	}
	if rni.HasNext() {
		t.Error("empty node list must not return true for HasNext")
	}
}

// test iterating over the slice: random start
func TestRoundRobinNodeIterating(t *testing.T) {
	// slice with a length of 5
	length := 5
	rni, ok := NewRoundRobinNodeIterator(newSchedNodeList(length)).(*roundRobinNodeIterator)
	if !ok {
		t.Fatal("wrong iterator type returned")
	}
	if rni == nil {
		t.Fatal("failed to create iterator with set slice")
	}
	start := rni.startIdx
	if start == -1 {
		t.Fatal("set node list should have random start set")
	}
	// walk over the whole list
	for i := 0; i < length; i++ {
		loc := (i + start) % length
		node := rni.Next()
		if node == nil || node.NodeID != "node-"+strconv.Itoa(loc) {
			t.Errorf("incorrect node returned: %v", node)
		}
	}
	// check were we are: should have done the whole slice HasNext is false
	if rni.HasNext() || rni.countIdx != length {
		t.Errorf("should have finished the slice expected at: %d, am at: %d", length, rni.countIdx)
	}
	if node := rni.Next(); node != nil {
		t.Errorf("next should not have returned a node: %v", node)
	}

	// Reset the iterator
	rni.Reset()
	if rni.startIdx != -1 || rni.countIdx != 0 || !rni.HasNext() {
		t.Fatal("reset did not set counters back")
	}
	// next will set a start point and return node
	node := rni.Next()
	if node == nil || rni.startIdx == -1 {
		t.Fatalf("next should have set the start %d, and returned node: %v", rni.startIdx, node)
	}
	if node.NodeID != "node-"+strconv.Itoa(rni.startIdx) {
		t.Errorf("incorrect node returned: %v", node)
	}
}

// base test for default iterator
func TestDefaultNodeEmpty(t *testing.T) {
	// nil list
	dni, ok := NewDefaultNodeIterator(nil).(*defaultNodeIterator)
	if !ok {
		t.Fatal("wrong iterator type returned")
	}
	if dni == nil {
		t.Fatal("failed to create basic iterator")
	}
	if dni.HasNext() || dni.countIdx != 0 {
		t.Error("nil node list should not return true on HasNext")
	}
	if node := dni.Next(); node != nil {
		t.Errorf("nil node list does not have next node: %v", node)
	}
	// slice with a length of 0
	dni, ok = NewDefaultNodeIterator(make([]*Node, 0)).(*defaultNodeIterator)
	if !ok {
		t.Fatal("wrong iterator type returned")
	}
	if dni == nil {
		t.Fatal("failed to create iterator with empty slice")
	}
	if dni.HasNext() {
		t.Error("empty node list should not return true on HasNext")
	}
	if node := dni.Next(); node != nil {
		t.Errorf("empty node list does not have Next node: %v", node)
	}
}

// test iterating over the slice: default start
func TestDefaultNodeIterating(t *testing.T) {
	// slice with a length of 5
	length := 5
	dni, ok := NewDefaultNodeIterator(newSchedNodeList(length)).(*defaultNodeIterator)
	if !ok {
		t.Fatal("wrong iterator type returned")
	}
	if dni == nil {
		t.Fatal("failed to create iterator with set slice")
	}
	// walk over the whole list
	for i := 0; i < length; i++ {
		node := dni.Next()
		if node == nil || node.NodeID != "node-"+strconv.Itoa(i) {
			t.Errorf("incorrect node returned: %v", node)
		}
	}
	// check were we are: should have done the whole slice HasNext is false
	if dni.HasNext() || dni.countIdx != length {
		t.Errorf("should have finished the slice expected at: %d, am at: %d", length, dni.countIdx)
	}
	if node := dni.Next(); node != nil {
		t.Errorf("next should not have returned a node: %v", node)
	}

	// Reset the iterator
	dni.Reset()
	if dni.countIdx != 0 || !dni.HasNext() {
		t.Fatalf("reset did not set counter back: %d", dni.countIdx)
	}
	// next will restart from the beginning
	node := dni.Next()
	if node == nil {
		t.Fatal("next should have returned a node")
	}
	if node.NodeID != "node-0" {
		t.Errorf("incorrect node returned expected node-0 got: %v", node)
	}
}

// A list of nodes that can be iterated over.
func newSchedNodeList(number int) []*Node {
	list := make([]*Node, number)
	for i := 0; i < number; i++ {
		num := strconv.Itoa(i)
		node := newNode("node-"+num, make(map[string]resources.Quantity))
		list[i] = node
	}
	return list
}
