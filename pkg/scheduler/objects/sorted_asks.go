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

	idx := sort.Search(size, func(i int) bool {
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

func (s *sortedRequests) remove(ask *Allocation) {
	for i, a := range *s {
		if a.allocationKey == ask.allocationKey {
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
