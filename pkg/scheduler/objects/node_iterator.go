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
	// Reset the iterator to a clean state
	Reset()

	// ForEachNode Calls the provided function on each Node object unil it returns false
	ForEachNode(func(*Node) bool)
}

type treeIterator struct {
	accept  func(*Node) bool
	getTree func() *btree.BTree
	tree    *btree.BTree
}

func (ti *treeIterator) Reset() {
	ti.tree = ti.getTree()
}

func (ti *treeIterator) ForEachNode(f func(*Node) bool) {
	ti.tree.Ascend(func(item btree.Item) bool {
		node := item.(nodeRef).node
		if ti.accept(node) {
			return f(node)
		}

		return true
	})
}

func NewTreeIterator(accept func(*Node) bool, getTree func() *btree.BTree) *treeIterator {
	ti := &treeIterator{
		getTree: getTree,
		accept:  accept,
	}
	ti.tree = getTree()
	return ti
}
