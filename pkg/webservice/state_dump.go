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
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	"github.com/apache/incubator-yunikorn-core/pkg/webservice/dao"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	defaultStateDumpPeriodSeconds time.Duration = 60 * time.Second //nolint:golint
	stateDumpFilePath             string        = "yunikorn-state.txt"
)

var (
	periodicStateDump bool
	abort             chan struct{}
	startStop         sync.Mutex
	stateDump         sync.Mutex // guards against simultaneous periodic vs web request
)

type AggregatedStateInfo struct {
	Timestamp          string
	Partitions         []*dao.PartitionInfo
	Applications       []*dao.ApplicationDAOInfo
	AppHistory         []*dao.ApplicationHistoryDAOInfo
	Nodes              []*dao.NodesDAOInfo
	NodesUtilization   []*dao.NodesUtilDAOInfo
	ClusterInfo        []*dao.ClusterDAOInfo
	ClusterUtilization []*dao.ClustersUtilDAOInfo
	ContainerHistory   []*dao.ContainerHistoryDAOInfo
	Queues             []*dao.PartitionDAOInfo
	LogLevel           string
}

func getFullStateDump(w http.ResponseWriter, r *http.Request) {
	writeHeaders(w)
	if err := doStateDump(w); err != nil {
		buildJSONErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}
}

func handlePeriodicStateDump(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	writeHeaders(w)
	if len(vars["switch"]) == 0 {
		buildJSONErrorResponse(w, "required parameter enabled/disabled is missing",
			http.StatusBadRequest)
		return
	}

	enabledSwitch := strings.ToLower(vars["switch"])
	switch enabledSwitch {
	case "enable":
		enablePeriodicStateDump(w, r)
	case "disable":
		disablePeriodicStateDump(w, r)
	default:
		buildJSONErrorResponse(w, fmt.Sprintf("required parameter enable/disable is illegal: %s", enabledSwitch),
			http.StatusBadRequest)
	}
}

func enablePeriodicStateDump(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	var convertedPeriod int
	var period time.Duration
	var err error
	var zapField = zap.Duration("defaultStateDumpPeriodSeconds", defaultStateDumpPeriodSeconds)

	if len(vars["periodSeconds"]) == 0 {
		log.Logger().Info("using the default period for state dump",
			zapField)
		period = defaultStateDumpPeriodSeconds
	} else {
		convertedPeriod, err = strconv.Atoi(vars["periodSeconds"])
		if err != nil {
			log.Logger().Warn("illegal value for period, using the default",
				zapField)
			period = defaultStateDumpPeriodSeconds
		} else {
			period = time.Duration(convertedPeriod) * time.Second
		}

		if period < 0 {
			log.Logger().Warn("period value is negative, using the default",
				zapField)
			period = defaultStateDumpPeriodSeconds
		}
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
	zapConfig := log.GetConfig()

	var aggregated = AggregatedStateInfo{
		Timestamp:          time.Now().Format(time.RFC3339),
		Partitions:         getPartitionInfoDAO(partitionContext),
		Applications:       getApplicationsDAO(partitionContext),
		AppHistory:         getAppHistoryDAO(records),
		Nodes:              getNodesDAO(partitionContext),
		NodesUtilization:   getNodesUtilDAO(partitionContext),
		ClusterInfo:        getClusterDAO(partitionContext),
		ClusterUtilization: getClustersUtilDAO(partitionContext),
		ContainerHistory:   getContainerHistoryDAO(records),
		Queues:             getPartitionDAO(partitionContext),
		LogLevel:           zapConfig.Level.Level().String(),
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

func startBackGroundStateDump(period time.Duration) error {
	startStop.Lock()
	defer startStop.Unlock()

	if periodicStateDump {
		var errMsg = "state dump already running"
		log.Logger().Error(errMsg)
		return fmt.Errorf(errMsg)
	}

	file, err := os.OpenFile(stateDumpFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Logger().Error("unable to open/create file",
			zap.Error(err))
		return err
	}
	abort = make(chan struct{})
	periodicStateDump = true

	go func() {
		ticker := time.NewTicker(period)

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

	log.Logger().Info("started periodic state dump", zap.String("filename", stateDumpFilePath),
		zap.Duration("period", period))
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
