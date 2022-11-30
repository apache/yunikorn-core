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

package scheduler

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
)

func createPartitionContext(t *testing.T) *PartitionContext {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues:    nil,
			},
		},
	}
	cc := &ClusterContext{}
	partition, err := newPartitionContext(conf, "test", cc)
	assert.NilError(t, err)
	return partition
}

func TestStopPartitionManager(t *testing.T) {
	p := createPartitionContext(t)

	p.partitionManager.Stop()

	// this call should not be blocked forever
	p.partitionManager.cleanExpiredApps()

	// this call should not be blocked forever
	p.partitionManager.cleanRoot()
}

func TestCleanQueues(t *testing.T) {
	p := createPartitionContext(t)

	root := p.GetQueue("root")
	assert.Assert(t, root != nil)

	// add new queue to partition
	queue, err := p.createQueue("root.test", security.UserGroup{})
	assert.NilError(t, err)
	assert.Equal(t, false, queue.IsManaged())
	assert.Equal(t, 1, len(p.root.GetCopyOfChildren()))

	// make sure all queues are removed
	p.partitionManager.cleanQueues(p.root)
	assert.Equal(t, 0, len(p.root.GetCopyOfChildren()))
}

func TestRemoveAll(t *testing.T) {
	p := createPartitionContext(t)

	_, err := p.createQueue("root.test", security.UserGroup{})
	assert.NilError(t, err)

	// add new node to partition
	err = p.addNodeToList(&objects.Node{})
	assert.NilError(t, err)
	assert.Equal(t, 1, p.nodes.GetNodeCount())

	// add new application to partition
	err = p.AddApplication(newApplication("app", p.Name, "root.test"))
	assert.NilError(t, err)
	assert.Equal(t, 1, len(p.applications))

	// make sure all nodes and applications are removed
	p.partitionManager.remove()
	assert.Equal(t, 0, len(p.applications))
	assert.Equal(t, 0, p.nodes.GetNodeCount())
}
