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
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
)

type StreamingLimiter struct {
	perHost map[string]uint64 // number of connections per host
	total   uint64            // total number of connections
	sync.Mutex
}

func NewStreamingLimiter() *StreamingLimiter {
	return &StreamingLimiter{
		perHost: make(map[string]uint64),
	}
}

func (sl *StreamingLimiter) AddHost(host string) bool {
	sl.Lock()
	defer sl.Unlock()

	total, perHost := sl.getLimits()
	if sl.total >= total {
		log.Log(log.SchedHealth).Info("Number of total connections reached",
			zap.Uint64("limit", total),
			zap.String("host", host))
		return false
	}
	if sl.perHost[host] >= perHost {
		log.Log(log.SchedHealth).Info("Per host connection limit reached",
			zap.Uint64("limit", perHost),
			zap.String("host", host))
		return false
	}

	sl.total++
	sl.perHost[host]++
	return true
}

func (sl *StreamingLimiter) RemoveHost(host string) {
	sl.Lock()
	defer sl.Unlock()

	count, ok := sl.perHost[host]
	if !ok {
		log.Log(log.REST).Warn("Tried to remove a non-existing host from tracking",
			zap.String("host", host))
		return
	}

	sl.total--
	if count == 1 {
		delete(sl.perHost, host)
		return
	}
	sl.perHost[host]--
}

func (sl *StreamingLimiter) getLimits() (uint64, uint64) {
	total := configs.DefaultStreamConnections
	configMap := configs.GetConfigMap()

	if value, ok := configMap[configs.CMMaxStreamConnectionsTotal]; ok {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Log(log.SchedHealth).Warn("Failed to parse configuration value",
				zap.String("key", configs.CMMaxStreamConnectionsTotal),
				zap.String("value", value),
				zap.Error(err))
		} else {
			total = parsed
		}
	}

	perHost := configs.DefaultStreamPerHostConnections
	if value, ok := configMap[configs.CMMaxStreamPerHostConnections]; ok {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Log(log.SchedHealth).Warn("Failed to parse configuration value",
				zap.String("key", configs.CMMaxStreamPerHostConnections),
				zap.String("value", value),
				zap.Error(err))
		} else {
			perHost = parsed
		}
	}

	return total, perHost
}
