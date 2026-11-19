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

import "sort"

// Storing allocation asks in a sorted slice.
//
// In the overwhelming majority of the cases, new asks are added to the end of the list. Therefore we don't need to
// use advanced data structures to maintain the sorted invariant of the slice.
type sortedRequests []*Allocation

func (s *sortedRequests) insert(ask *Allocation) {
	size := len(*s)

	if size > 0 && ask.LessThan((*s)[size-1]) {
		// fast path, insert at the end (most likely)
		s.insertAt(size, ask)
		return
	}

	// the slow path is exactly reinsert's placement; share it so the two cannot diverge
	s.reinsert(ask)
}

// reinsert puts an ask that is returning to the pending set (deallocated after a reverted or
// failed allocation) back into the slice. Unlike insert it never takes the append fast path: with
// LessThan reporting a (priority, createTime) tie as true in both directions, the binary search
// stops at the FIRST tie-peer, so the returning ask lands at the head of its tie-group and is
// retried before the peers it had already been tried ahead of.
func (s *sortedRequests) reinsert(ask *Allocation) {
	idx := sort.Search(len(*s), func(i int) bool {
		return (*s)[i].LessThan(ask)
	})
	s.insertAt(idx, ask)
}

func (s *sortedRequests) insertAt(index int, ask *Allocation) {
	*s = append(*s, nil)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = ask
}

// remove drops the entry that IS the passed ask, matching on pointer identity: that is cheaper than
// comparing allocationKeys, and every caller passes the object it resolved from sa.requests or read
// out of the slice itself.
func (s *sortedRequests) remove(ask *Allocation) {
	for i, a := range *s {
		if a == ask {
			s.removeAt(i)
			return
		}
	}
}

func (s *sortedRequests) removeAt(index int) {
	copy((*s)[index:], (*s)[index+1:])
	(*s)[len(*s)-1] = nil
	*s = (*s)[:len(*s)-1]
}
