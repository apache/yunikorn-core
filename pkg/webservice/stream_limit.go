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

// StreamingLimiter tracks the concurrent streaming connections.
type StreamingLimiter struct {
	perHostStreams map[string]uint64 // number of connections per host
	streams        uint64            // number of connections (total)

	maxStreams        uint64 // maximum number of event streams
	maxPerHostStreams uint64 // maximum number of event streams per host

	sync.Mutex
}

func NewStreamingLimiter() *StreamingLimiter {
	sl := &StreamingLimiter{
		perHostStreams: make(map[string]uint64),
	}

	configs.AddConfigMapCallback("stream-limiter", func() {
		log.Log(log.REST).Info("Reloading streaming limit settings")
		sl.setLimits()
	})
	sl.setLimits()

	return sl
}

func (sl *StreamingLimiter) AddHost(host string) bool {
	sl.Lock()
	defer sl.Unlock()

	if sl.streams >= sl.maxStreams {
		log.Log(log.SchedHealth).Info("Number of maximum stream connections reached",
			zap.Uint64("limit", sl.maxStreams),
			zap.String("host", host))
		return false
	}
	if sl.perHostStreams[host] >= sl.maxPerHostStreams {
		log.Log(log.SchedHealth).Info("Per host connection limit reached",
			zap.Uint64("limit", sl.maxPerHostStreams),
			zap.String("host", host))
		return false
	}

	sl.streams++
	sl.perHostStreams[host]++
	return true
}

func (sl *StreamingLimiter) RemoveHost(host string) {
	sl.Lock()
	defer sl.Unlock()

	count, ok := sl.perHostStreams[host]
	if !ok {
		log.Log(log.REST).Warn("Tried to remove a non-existing host from tracking",
			zap.String("host", host))
		return
	}

	sl.streams--
	if count <= 1 {
		delete(sl.perHostStreams, host)
		return
	}
	sl.perHostStreams[host]--
}

func (sl *StreamingLimiter) setLimits() {
	maxStreams := configs.DefaultMaxStreams
	configMap := configs.GetConfigMap()

	if value, ok := configMap[configs.CMMaxEventStreams]; ok {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Log(log.SchedHealth).Warn("Failed to parse configuration value",
				zap.String("key", configs.CMMaxEventStreams),
				zap.String("value", value),
				zap.Error(err))
		} else {
			maxStreams = parsed
		}
	}

	maxStreamsPerHost := configs.DefaultMaxStreamsPerHost
	if value, ok := configMap[configs.CMMaxEventStreamsPerHost]; ok {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			log.Log(log.SchedHealth).Warn("Failed to parse configuration value",
				zap.String("key", configs.CMMaxEventStreamsPerHost),
				zap.String("value", value),
				zap.Error(err))
		} else {
			maxStreamsPerHost = parsed
		}
	}

	sl.maxStreams = maxStreams
	sl.maxPerHostStreams = maxStreamsPerHost
}
