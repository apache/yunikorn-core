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
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
)

func TestAddRemoveHost(t *testing.T) {
	sl := NewStreamingLimiter()
	assert.Assert(t, sl.AddHost("host-1"))
	assert.Assert(t, sl.AddHost("host-1"))
	assert.Assert(t, sl.AddHost("host-2"))
	assert.Equal(t, 2, len(sl.perHostStreams))
	assert.Equal(t, uint64(3), sl.streams)

	sl.RemoveHost("host-3") // remove non-existing
	assert.Equal(t, 2, len(sl.perHostStreams))
	assert.Equal(t, uint64(3), sl.streams)

	sl.RemoveHost("host-1")
	assert.Equal(t, 2, len(sl.perHostStreams))
	assert.Equal(t, uint64(2), sl.streams)

	sl.RemoveHost("host-2")
	assert.Equal(t, 1, len(sl.perHostStreams))
	assert.Equal(t, uint64(1), sl.streams)

	sl.RemoveHost("host-1")
	assert.Equal(t, 0, len(sl.perHostStreams))
	assert.Equal(t, uint64(0), sl.streams)
}

func TestAddHost_TotalLimitHit(t *testing.T) {
	current := configs.GetConfigMap()
	defer func() {
		configs.SetConfigMap(current)
	}()
	configs.SetConfigMap(map[string]string{
		configs.CMMaxEventStreams: "2",
	})
	sl := NewStreamingLimiter()

	assert.Assert(t, sl.AddHost("host-1"))
	assert.Assert(t, sl.AddHost("host-2"))
	assert.Assert(t, !sl.AddHost("host-3"))
}

func TestAddHost_PerHostLimitHit(t *testing.T) {
	current := configs.GetConfigMap()
	defer func() {
		configs.SetConfigMap(current)
	}()
	configs.SetConfigMap(map[string]string{
		configs.CMMaxEventStreamsPerHost: "2",
	})
	cl := NewStreamingLimiter()

	assert.Assert(t, cl.AddHost("host-1"))
	assert.Assert(t, cl.AddHost("host-1"))
	assert.Assert(t, !cl.AddHost("host-1"))
}

func TestGetLimits(t *testing.T) {
	current := configs.GetConfigMap()
	defer func() {
		configs.SetConfigMap(current)
	}()
	sl := NewStreamingLimiter()

	sl.setLimits()
	assert.Equal(t, uint64(100), sl.maxStreams)
	assert.Equal(t, uint64(15), sl.maxPerHostStreams)

	configs.SetConfigMap(map[string]string{
		configs.CMMaxEventStreams: "123",
	})
	sl.setLimits()
	assert.Equal(t, uint64(123), sl.maxStreams)
	assert.Equal(t, uint64(15), sl.maxPerHostStreams)

	configs.SetConfigMap(map[string]string{
		configs.CMMaxEventStreamsPerHost: "321",
	})
	sl.setLimits()
	assert.Equal(t, uint64(100), sl.maxStreams)
	assert.Equal(t, uint64(321), sl.maxPerHostStreams)

	configs.SetConfigMap(map[string]string{
		configs.CMMaxEventStreams:        "xxx",
		configs.CMMaxEventStreamsPerHost: "yyy",
	})
	sl.setLimits()
	assert.Equal(t, uint64(100), sl.maxStreams)
	assert.Equal(t, uint64(15), sl.maxPerHostStreams)
}
