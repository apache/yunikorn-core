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
	"github.com/google/btree"
)

// NodeIterator iterates over a list of nodes based on the defined policy
type NodeIterator interface {
	// ForEachNode Calls the provided function on the sorted Node object until it returns false
	ForEachNode(func(*Node) bool)
}

type treeIterator struct {
	accept  func(*Node) bool
	getTree func() *btree.BTree
}

// ForEachNode Calls the provided "f" function on the sorted Node object until it returns false.
// The accept() function checks if the node should be a candidate or not.
func (ti *treeIterator) ForEachNode(f func(*Node) bool) {
	ti.getTree().Ascend(func(item btree.Item) bool {
		if ref, ok := item.(nodeRef); ok {
			node := ref.node
			if ti.accept(node) {
				return f(node)
			}
		}

		return true
	})
}

func NewTreeIterator(accept func(*Node) bool, getTree func() *btree.BTree) *treeIterator {
	ti := &treeIterator{
		getTree: getTree,
		accept:  accept,
	}
	return ti
}
