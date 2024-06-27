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

	"github.com/google/btree"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestTreeIterator_AcceptAll(t *testing.T) {
	tree := getTree()
	treeItr := NewTreeIterator(acceptAll, func() *btree.BTree {
		return tree
	})

	checked := make([]*Node, 0)
	treeItr.ForEachNode(func(node *Node) bool {
		checked = append(checked, node)
		return true
	})

	assert.Equal(t, 10, len(checked))
}

func TestTreeIterator_AcceptUnreserved(t *testing.T) {
	tree := getTree()
	treeItr := NewTreeIterator(acceptUnreserved, func() *btree.BTree {
		return tree
	})

	checked := make([]*Node, 0)
	unreservedIds := make(map[int]bool)
	treeItr.ForEachNode(func(node *Node) bool {
		checked = append(checked, node)
		i, err := strconv.Atoi(node.Hostname)
		assert.NilError(t, err, "conversion failure")
		unreservedIds[i] = true
		return true
	})

	assert.Equal(t, 5, len(checked))
	for i := 5; i < 10; i++ { // node 5-10 are unreserved
		assert.Assert(t, unreservedIds[i])
	}
}

func getTree() *btree.BTree {
	nodesReserved := newSchedNodeList(0, 5, true)
	nodes := newSchedNodeList(5, 10, false)
	nodes = append(nodes, nodesReserved...)

	tree := btree.New(7)
	for _, n := range nodes {
		tree.ReplaceOrInsert(nodeRef{
			node:      n,
			nodeScore: 1,
		})
	}

	return tree
}

// A list of nodes that can be iterated over.
func newSchedNodeList(start, end int, reserved bool) []*Node {
	list := make([]*Node, 0)
	for i := start; i < end; i++ {
		num := strconv.Itoa(i)
		node := newNode("node-"+num, make(map[string]resources.Quantity))
		node.Hostname = num
		if reserved {
			node.reservations = map[string]*reservation{
				"dummy": {},
			}
		}
		list = append(list, node)
	}
	return list
}
