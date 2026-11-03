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
	"fmt"
	"runtime"
	"sync"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
)

func createPartitionContext(t *testing.T, rules ...configs.PlacementRule) *PartitionContext {
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
		PlacementRules: rules,
	}
	cc := &ClusterContext{}
	partition, err := newPartitionContext(conf, "test", cc, false)
	assert.NilError(t, err)
	return partition
}

func TestStopPartitionManager(t *testing.T) {
	p := createPartitionContext(t)
	// stop the resolver
	defer p.userGroupCache.Stop()

	p.partitionManager.Stop()

	// this call should not be blocked forever
	p.partitionManager.cleanExpiredApps()

	// this call should not be blocked forever
	p.partitionManager.cleanRoot()
}

func TestCleanQueues(t *testing.T) {
	p := createPartitionContext(t)
	// stop the resolver
	defer p.userGroupCache.Stop()

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
	// stop the resolver
	defer p.userGroupCache.Stop()

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
	assert.Assert(t, p.isDraining())
	assert.Equal(t, 0, len(p.applications))
	assert.Equal(t, 0, p.nodes.GetNodeCount())
}

// The cleaner must not remove a dynamic queue between AddApplication finding or creating it and adding the
// application to it: the application would end up in a queue that is no longer part of the hierarchy.
func TestCleanQueuesAddApplication(t *testing.T) {
	setupUGM()
	p := createPartitionContext(t, configs.PlacementRule{Name: "provided", Create: true})
	// stop the resolver
	defer p.userGroupCache.Stop()

	// run the cleaner in a loop until the test finishes
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				p.partitionManager.cleanQueues(p.root)
				runtime.Gosched()
			}
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	const queueName = "root.parent.leaf"
	for i := 0; i < 1000; i++ {
		appID := fmt.Sprintf("app-%d", i)
		app := newApplication(appID, p.Name, queueName)
		assert.NilError(t, p.AddApplication(app), "add application failed")
		assert.Assert(t, app.GetQueue() == p.GetQueue(queueName), "application %s added to a queue that is not in the hierarchy", appID)
		// empty the queue again so the cleaner can remove it
		p.removeApplication(appID)
	}
}
