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
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	metrics2 "github.com/apache/incubator-yunikorn-core/pkg/metrics"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	defaultStateDumpPeriod = 60
)

var (
	periodicStateDump bool
	abort             chan struct{}
	startStop         sync.Mutex
	stateDump         sync.Mutex // guards against simultaneous periodic vs web request
)

type AggregatedStateInfo struct {
	Timestamp        string
	Partitions       []*dao.PartitionInfo
	Applications     []*dao.ApplicationDAOInfo
	AppHistory       []*dao.ApplicationHistoryDAOInfo
	Nodes            []*dao.NodesDAOInfo
	NodesUtilization []*dao.NodesUtilDAOInfo
	ClusterInfo      []*dao.ClusterDAOInfo
	ContainerHistory []*dao.ContainerHistoryDAOInfo
	Queues           []*dao.PartitionDAOInfo
	SchedulerHealth  *dao.SchedulerHealthDAOInfo
	LogLevel         string
}

func getFullStateDump(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	if err := doStateDump(w); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func enablePeriodicStateDump(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	writeHeaders(w)
	var period int
	var err error
	var zapField = zap.Int("defaultStateDumpPeriod", defaultStateDumpPeriod)

	if len(vars["period"]) == 0 {
		log.Logger().Info("using the default period for state dump",
			zapField)
		period = defaultStateDumpPeriod
	} else if period, err = strconv.Atoi(vars["period"]); err != nil {
		log.Logger().Warn("illegal value for period, using the default",
			zapField)
		period = defaultStateDumpPeriod
	}

	if period < 0 {
		log.Logger().Warn("period value is negative, using the default",
			zapField)
		period = defaultStateDumpPeriod
	}

	if err = startBackGroundStateDump(period); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func disablePeriodicStateDump(w http.ResponseWriter, r *http.Request) {
	if err := stopBackGroundStateDump(); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func doStateDump(w io.Writer) error {
	stateDump.Lock()
	defer stateDump.Unlock()

	partitionContext := schedulerContext.GetPartitionMapClone()
	records := imHistory.GetRecords()
	metrics := metrics2.GetSchedulerMetrics()
	schedulerHealth := scheduler.GetSchedulerHealthStatus(metrics, schedulerContext)
	zapConfig := log.GetConfig()

	var aggregated = AggregatedStateInfo{
		Timestamp:        time.Now().Format(time.RFC3339),
		Partitions:       getPartitionInfoDao(partitionContext),
		Applications:     getApplicationsDao(partitionContext),
		AppHistory:       getAppHistoryDao(records),
		Nodes:            getNodesDao(partitionContext),
		NodesUtilization: getNodesUtilizationDao(partitionContext),
		ClusterInfo:      getClusterInfoDao(partitionContext),
		ContainerHistory: getContainerHistoryDao(records),
		Queues:           getPartitionDAOInfo(partitionContext),
		SchedulerHealth:  &schedulerHealth,
		LogLevel:         zapConfig.Level.Level().String(),
	}

	var prettyJSON []byte
	var err error
	prettyJSON, err = json.MarshalIndent(aggregated, "", "  ")
	if err != nil {
		return err
	}
	if _, err = w.Write(prettyJSON); err != nil {
		return err
	}

	return nil
}

func startBackGroundStateDump(period int) error {
	startStop.Lock()
	defer startStop.Unlock()

	if periodicStateDump {
		return fmt.Errorf("state dump already running")
	}
	const fileName = "/tmp/yunikorn_statedump.txt" // TODO: configurable?
	file, err := os.Create(fileName)
	if err != nil {
		log.Logger().Error("unable to create file",
			zap.Error(err))
		return err
	}
	abort = make(chan struct{})
	periodicStateDump = true

	go func() {
		ticker := time.NewTicker(time.Duration(period) * time.Second)

		for {
			select {
			case <-abort:
				ticker.Stop()
				file.Close()
				return
			case <-ticker.C:
				if err := doStateDump(file); err != nil {
					log.Logger().Error("state dump failed", zap.Error(err))
					if err := stopBackGroundStateDump(); err != nil {
						log.Logger().Error("background stop failed",
							zap.Error(err))
					}
					return
				}
			}
		}
	}()

	log.Logger().Info("started periodic state dump", zap.String("filename", fileName),
		zap.Int("period", period))
	return nil
}

func stopBackGroundStateDump() error {
	startStop.Lock()
	defer startStop.Unlock()

	if !periodicStateDump {
		var errMsg = "state dump not running"
		log.Logger().Error(errMsg)
		return fmt.Errorf(errMsg)
	}

	abort <- struct{}{}
	close(abort)
	periodicStateDump = false

	return nil
}
