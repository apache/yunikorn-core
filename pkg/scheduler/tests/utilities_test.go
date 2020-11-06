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

package tests

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
)

const (
	partition = "[rm:123]default"
	appID2    = "app-2"
	appID1    = "app-1"
)

// Returns the calling function name, file name and line.
// Used in waitFor.. functions to show where the call was made from.
// The 2 caller skip steps over the waitFor.. caller back into the real test call
func caller() string {
	pc, file, line, ok := runtime.Caller(2)
	funcName := "unknown"
	if ok {
		name := runtime.FuncForPC(pc).Name()
		name = name[strings.LastIndex(name, ".")+1:]
		file = file[strings.LastIndex(file, string(os.PathSeparator))+1:]
		funcName = fmt.Sprintf("%s in %s:%d", name, file, line)
	}
	return funcName
}

func waitForPendingQueueResource(t *testing.T, queue *objects.Queue, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return queue.GetPendingResource().Resources[resources.MEMORY] == memory
	})
	if err != nil {
		log.Logger().Info("queue detail",
			zap.Any("queue", queue))
		t.Fatalf("Failed to wait pending resource on queue %s, expected %v, actual %v, called from: %s", queue.QueuePath, memory, queue.GetPendingResource().Resources[resources.MEMORY], caller())
	}
}

func waitForPendingAppResource(t *testing.T, app *objects.Application, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return app.GetPendingResource().Resources[resources.MEMORY] == memory
	})
	assert.NilError(t, err, "Failed to wait for pending resource, expected %v, actual %v, called from: %s", memory, app.GetPendingResource().Resources[resources.MEMORY], caller())
}

func waitForAllocatedAppResource(t *testing.T, app *objects.Application, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return app.GetAllocatedResource().Resources[resources.MEMORY] == memory
	})
	assert.NilError(t, err, "Failed to wait for pending resource, expected %v, actual %v, called from: %s", memory, app.GetPendingResource().Resources[resources.MEMORY], caller())
}

func waitForAllocatedQueueResource(t *testing.T, queue *objects.Queue, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return queue.GetAllocatedResource().Resources[resources.MEMORY] == memory
	})
	assert.NilError(t, err, "Failed to wait for allocations on queue %s, called from: %s", queue.QueuePath, caller())
}

func waitForAllocatedNodeResource(t *testing.T, cc *scheduler.ClusterContext, partitionName string, nodeIDs []string, allocatedMemory resources.Quantity, timeoutMs int) {
	var totalNodeResource resources.Quantity
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		totalNodeResource = 0
		for _, nodeID := range nodeIDs {
			totalNodeResource += cc.GetPartition(partitionName).GetNode(nodeID).GetAllocatedResource().Resources[resources.MEMORY]
		}
		return totalNodeResource == allocatedMemory
	})
	assert.NilError(t, err, "Failed to wait for allocations on partition %s and node %v, called from: %s", partitionName, nodeIDs, caller())
}

func waitForAvailableNodeResource(t *testing.T, cc *scheduler.ClusterContext, partitionName string, nodeIDs []string, availableMemory resources.Quantity, timeoutMs int) {
	var totalNodeResource resources.Quantity
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		totalNodeResource = 0
		for _, nodeID := range nodeIDs {
			totalNodeResource += cc.GetPartition(partitionName).GetNode(nodeID).GetAvailableResource().Resources[resources.MEMORY]
		}
		return totalNodeResource == availableMemory
	})
	assert.NilError(t, err, "Failed to wait for available resource %v and node %v, called from: %s", availableMemory, nodeIDs, caller())
}

func waitForNewNode(t *testing.T, cc *scheduler.ClusterContext, nodeID string, partitionName string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		node := cc.GetNode(nodeID, partitionName)
		return node != nil
	})
	assert.NilError(t, err, "Failed to wait for new scheduling node on partition %s, node %v, called from: %s", partitionName, nodeID, caller())
}

func waitForRemovedNode(t *testing.T, context *scheduler.ClusterContext, nodeID string, partitionName string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		node := context.GetNode(nodeID, partitionName)
		return node == nil
	})
	assert.NilError(t, err, "Failed to wait for removal of scheduling node on partition %s, node %v, called from: %s", partitionName, nodeID, caller())
}

func getApplication(pc *scheduler.PartitionContext, appID string) (*objects.Application, error) {
	for _, app := range pc.GetApplications() {
		if app.ApplicationID == appID {
			return app, nil
		}
	}
	return nil, fmt.Errorf("cannot find app %s from cache", appID)
}

func newAddAppRequest(apps map[string]string) []*si.AddApplicationRequest {
	var requests []*si.AddApplicationRequest
	for app, queue := range apps {
		request := si.AddApplicationRequest{
			ApplicationID: app,
			QueueName:     queue,
			PartitionName: "",
			Ugi: &si.UserGroupInformation{
				User: "testuser",
			},
		}
		requests = append(requests, &request)
	}
	return requests
}
