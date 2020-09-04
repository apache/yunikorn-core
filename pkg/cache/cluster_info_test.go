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

package cache

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
	"github.com/apache/incubator-yunikorn-core/pkg/common/testUtils"
)

func TestUpdateSchedulerConfig(t *testing.T) {
	confDeletedPartition := configs.SchedulerConfig{
		Partitions: []configs.PartitionConfig{},
	}
	confUpdatedPartition := configs.SchedulerConfig{
		Partitions: []configs.PartitionConfig{{
			Name: "default",
			Queues: []configs.QueueConfig{{
				Name:   "root",
				Parent: true,
				Queues: []configs.QueueConfig{{
					Name: "leaf",
				}},
			}},
		}},
	}

	testCases := []struct {
		name      string
		newConfig configs.SchedulerConfig
		deleted   bool
		updated   bool
	}{
		{"Deleted partition", confDeletedPartition, true, false},
		{"Updated partition", confUpdatedPartition, false, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			clusterInfo := NewClusterInfo()
			configs.MockSchedulerConfigByData([]byte(configDefault))
			if _, err := SetClusterInfoFromConfigFile(clusterInfo, "rm-123", "default-policy-group"); err != nil {
				t.Errorf("Error when load clusterInfo from config %v", err)
				return
			}
			mockHandler := &testUtils.MockEventHandler{EventHandled: false}
			clusterInfo.EventHandlers.SchedulerEventHandler = mockHandler
			err := clusterInfo.UpdateSchedulerConfig(&tc.newConfig)
			assert.NilError(t, err, "Error is not expected")
			assert.Equal(t, tc.deleted, mockHandler.PartitionDeleteHandled, "Deleted partition not handled")
			assert.Equal(t, tc.updated, mockHandler.PartitionUpdateHandled, "Updated partition not handled")
		})
	}
}
