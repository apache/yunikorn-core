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
	"math/rand"
)

// NodeIterator iterates over a list of nodes based on the defined policy
type NodeIterator interface {
	// returns true if there are more values to iterate over
	HasNext() bool
	// returns the next node from the iterator
	Next() *Node
	// reset the iterator to a clean state
	Reset()
}

// All iterators extend the base iterator
type baseIterator struct {
	NodeIterator
	countIdx int
	size     int
	nodes    []*Node
}

// Reset the iterator to start from the beginning
func (bi *baseIterator) Reset() {
	bi.countIdx = 0
}

// HasNext returns true if there is a next element in the array.
// Returns false if there are no more elements or list is empty.
func (bi *baseIterator) HasNext() bool {
	return !(bi.countIdx+1 > bi.size)
}

// Next returns the next element and advances to next element in array.
// Returns nil at the end of iteration.
func (bi *baseIterator) Next() *Node {
	if (bi.countIdx + 1) > bi.size {
		return nil
	}

	value := bi.nodes[bi.countIdx]
	bi.countIdx++
	return value
}

// Default iterator, wraps the base iterator.
// Iterates over the list from the start, position zero, to end.
type defaultNodeIterator struct {
	baseIterator
}

// Create a new default iterator
func NewDefaultNodeIterator(schedulerNodes []*Node) NodeIterator {
	it := &defaultNodeIterator{}
	it.nodes = schedulerNodes
	it.size = len(schedulerNodes)
	return it
}

// Random iterator, wraps the base iterator
// Iterates over the list from a random starting position in the list.
// The iterator automatically wraps at the end of the list.
type roundRobinNodeIterator struct {
	baseIterator
	startIdx int
}

// The starting point is randomised in the slice.
func NewRoundRobinNodeIterator(schedulerNodes []*Node) NodeIterator {
	it := &roundRobinNodeIterator{}
	it.nodes = schedulerNodes
	it.size = len(schedulerNodes)
	if it.size > 0 {
		it.startIdx = rand.Intn(it.size)
	}
	return it
}

// Next returns the next element and advances to next element in array.
// Returns nil at the end of iteration.
func (ri *roundRobinNodeIterator) Next() *Node {
	// prevent panic on Next when slice is empty
	if (ri.countIdx + 1) > ri.size {
		return nil
	}

	// after reset initialize the rand seed based on number of nodes.
	if ri.startIdx == -1 {
		ri.startIdx = rand.Intn(ri.size)
	}

	idx := (ri.countIdx + ri.startIdx) % ri.size
	value := ri.nodes[idx]
	ri.countIdx++
	return value
}

func (ri *roundRobinNodeIterator) Reset() {
	ri.countIdx = 0
	ri.startIdx = -1
}
