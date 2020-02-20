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

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common"
	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/si"
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

func waitForPendingQueueResource(t *testing.T, queue *scheduler.SchedulingQueue, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return queue.GetPendingResource().Resources[resources.MEMORY] == memory
	})
	if err != nil {
		log.Logger().Info("queue detail",
			zap.Any("queue", queue))
		t.Fatalf("Failed to wait pending resource on queue %s, expected %v, actual %v, called from: %s", queue.Name, memory, queue.GetPendingResource().Resources[resources.MEMORY], caller())
	}
}

func waitForPendingAppResource(t *testing.T, app *scheduler.SchedulingApplication, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return app.GetPendingResource().Resources[resources.MEMORY] == memory
	})
	if err != nil {
		t.Fatalf("Failed to wait for pending resource, expected %v, actual %v, called from: %s", memory, app.GetPendingResource().Resources[resources.MEMORY], caller())
	}
}

func waitForAllocatedAppResource(t *testing.T, app *scheduler.SchedulingApplication, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return app.GetAllocatedResource().Resources[resources.MEMORY] == memory
	})
	if err != nil {
		t.Fatalf("Failed to wait for pending resource, expected %v, actual %v, called from: %s", memory, app.GetPendingResource().Resources[resources.MEMORY], caller())
	}
}

func waitForAllocatedQueueResource(t *testing.T, queue *scheduler.SchedulingQueue, memory resources.Quantity, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		return queue.GetAllocatedResource().Resources[resources.MEMORY] == memory
	})
	if err != nil {
		t.Fatalf("Failed to wait for allocations on queue %s, called from: %s", queue.Name, caller())
	}
}

func waitForNodesAllocatedResource(t *testing.T, cache *cache.ClusterInfo, partitionName string, nodeIDs []string, allocatedMemory resources.Quantity, timeoutMs int) {
	var totalNodeResource resources.Quantity
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		totalNodeResource = 0
		for _, nodeID := range nodeIDs {
			totalNodeResource += cache.GetPartition(partitionName).GetNode(nodeID).GetAllocatedResource().Resources[resources.MEMORY]
		}
		return totalNodeResource == allocatedMemory
	})
	if err != nil {
		t.Fatalf("Failed to wait for allocations on partition %s and node %v, called from: %s", partitionName, nodeIDs, caller())
	}
}

func waitForNewSchedulerNode(t *testing.T, context *scheduler.ClusterSchedulingContext, nodeID string, partitionName string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		node := context.GetSchedulingNode(nodeID, partitionName)
		return node != nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for new scheduling node on partition %s, node %v, called from: %s", partitionName, nodeID, caller())
	}
}

func waitForRemovedSchedulerNode(t *testing.T, context *scheduler.ClusterSchedulingContext, nodeID string, partitionName string, timeoutMs int) {
	err := common.WaitFor(10*time.Millisecond, time.Duration(timeoutMs)*time.Millisecond, func() bool {
		node := context.GetSchedulingNode(nodeID, partitionName)
		return node == nil
	})
	if err != nil {
		t.Fatalf("Failed to wait for removal of scheduling node on partition %s, node %v, called from: %s", partitionName, nodeID, caller())
	}
}

func getApplicationInfoFromPartition(partitionInfo *cache.PartitionInfo, appID string) (*cache.ApplicationInfo, error) {
	for _, appInfo := range partitionInfo.GetApplications() {
		if appInfo.ApplicationID == appID {
			return appInfo, nil
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
