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

package webservice

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	yunikornLog "github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

const (
	stateLogCallDepth = 2
)

var stateDump sync.Mutex // ensures only one state dump can be handled at a time

type AggregatedStateInfo struct {
	Timestamp        int64                            `json:"timestamp,omitempty"`
	Partitions       []*dao.PartitionInfo             `json:"partitions,omitempty"`
	Applications     []*dao.ApplicationDAOInfo        `json:"applications,omitempty"`
	AppHistory       []*dao.ApplicationHistoryDAOInfo `json:"appHistory,omitempty"`
	Nodes            []*dao.NodesDAOInfo              `json:"nodes,omitempty"`
	ClusterInfo      []*dao.ClusterDAOInfo            `json:"clusterInfo,omitempty"`
	ContainerHistory []*dao.ContainerHistoryDAOInfo   `json:"containerHistory,omitempty"`
	Queues           []dao.PartitionQueueDAOInfo      `json:"queues,omitempty"`
	RMDiagnostics    map[string]interface{}           `json:"rmDiagnostics,omitempty"`
	LogLevel         string                           `json:"logLevel,omitempty"`
}

func getFullStateDump(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	if err := doStateDump(w); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func handlePeriodicStateDump(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	yunikornLog.Log(yunikornLog.Deprecation).Warn("Periodic state dumps are no longer supported. The /ws/v1/periodicstatedump endpoint will be removed in a future release.")
}

func doStateDump(w io.Writer) error {
	stateDump.Lock()
	defer stateDump.Unlock()

	partitionContext := schedulerContext.GetPartitionMapClone()
	records := imHistory.GetRecords()
	zapConfig := yunikornLog.GetZapConfigs()

	var aggregated = AggregatedStateInfo{
		Timestamp:        time.Now().UnixNano(),
		Partitions:       getPartitionInfoDAO(partitionContext),
		Applications:     getApplicationsDAO(partitionContext),
		AppHistory:       getAppHistoryDAO(records),
		Nodes:            getPartitionNodesDAO(partitionContext),
		ClusterInfo:      getClusterDAO(partitionContext),
		ContainerHistory: getContainerHistoryDAO(records),
		Queues:           getPartitionQueuesDAO(partitionContext),
		RMDiagnostics:    getResourceManagerDiagnostics(),
		LogLevel:         zapConfig.Level.Level().String(),
	}

	var prettyJSON []byte
	var err error
	prettyJSON, err = json.MarshalIndent(aggregated, "", "  ")
	if err != nil {
		return err
	}

	stateLog := log.New(w, "", 0)
	if err = stateLog.Output(stateLogCallDepth, string(prettyJSON)); err != nil {
		return err
	}

	return nil
}
