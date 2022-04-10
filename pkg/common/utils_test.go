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

package common

import (
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"gotest.tools/assert"

	common "github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestGetNormalizedPartitionName(t *testing.T) {
	tests := []struct {
		partitionName string
		rmID          string
		want          string
	}{
		{"", "", "[]default"},
		{"", "RM", "[RM]default"},
		{"default", "RM", "[RM]default"},
		{"X", "RM", "[RM]X"},
		{"[NewRM]X", "RM", "[NewRM]X"},
	}
	for _, test := range tests {
		got := GetNormalizedPartitionName(test.partitionName, test.rmID)
		assert.Equal(t, got, test.want, "unexpected normalized partition name!")
	}
}

func TestGetRMIdFromPartitionName(t *testing.T) {
	tests := []struct {
		partitionName string
		want          string
	}{
		{"", ""},
		{"default", ""},
		{"[RM]default", "RM"},
	}
	for _, test := range tests {
		got := GetRMIdFromPartitionName(test.partitionName)
		assert.Equal(t, got, test.want, "unexpected rmID!")
	}
}

func TestGetPartitionNameWithoutClusterID(t *testing.T) {
	tests := []struct {
		partitionName string
		want          string
	}{
		{"", ""},
		{"default", "default"},
		{"[RM]default", "default"},
	}
	for _, test := range tests {
		got := GetPartitionNameWithoutClusterID(test.partitionName)
		assert.Equal(t, got, test.want, "unexpected partitionName without clusterID!")
	}
}

func TestGetBoolEnvVar(t *testing.T) {
	envVarName := "VAR"
	testCases := []struct {
		name     string
		value    string
		expected bool
	}{
		{"ENV var not set", "", true},
		{"ENV var set", "false", false},
		{"Invalid value", "someValue", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := os.Setenv(envVarName, tc.value)
			assert.NilError(t, err, "setting environment variable failed")
			val := GetBoolEnvVar(envVarName, true)
			assert.Equal(t, val, tc.expected, "test case failure: %s", tc.name)
			err = os.Unsetenv(envVarName)
			assert.NilError(t, err, "cleaning up environment variable failed")
		})
	}
}

func TestConvertSITimeout(t *testing.T) {
	testCases := []struct {
		name     string
		value    int64
		expected time.Duration
	}{
		{"negative value", -1, 0},
		{"zero value", 0, 0},
		{"small value", 100, time.Millisecond * 100},
		{"overflow value", math.MaxInt64 / 10, 0},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val := ConvertSITimeout(tc.value)
			assert.Equal(t, val, tc.expected, "test case failure: %s", tc.name)
		})
	}
}

func TestGetRequiredNodeFromAsk(t *testing.T) {
	tag := make(map[string]string)
	nodeName := GetRequiredNodeFromTag(tag)
	assert.Equal(t, nodeName, "")
	tag["TestValue"] = "ERROR"
	nodeName = GetRequiredNodeFromTag(tag)
	assert.Equal(t, nodeName, "")
	tag[common.DomainYuniKorn+common.KeyRequiredNode] = "Node1"
	nodeName = GetRequiredNodeFromTag(tag)
	assert.Equal(t, nodeName, "Node1")
	tag[common.DomainYuniKorn+common.KeyRequiredNode] = "Node2"
	nodeName = GetRequiredNodeFromTag(tag)
	assert.Equal(t, nodeName, "Node2")
}

func TestConvertSITimeoutWithAdjustment(t *testing.T) {
	var err error
	var current time.Time
	current, err = time.Parse(time.RFC1123Z, "Mon, 02 Jan 2020 12:00:00 -0000")
	assert.NilError(t, err, "Could not parse time")

	var created time.Time
	created, err = time.Parse(time.RFC1123Z, "Mon, 02 Jan 2020 11:50:00 -0000")
	assert.NilError(t, err, "Could not parse time")
	currentTime = func() time.Time {
		return current
	}

	siApp := &si.AddApplicationRequest{
		Tags: map[string]string{
			"yunikorn.apache.org/CreationTime": strconv.FormatInt(created.Unix(), 10),
		},
	}

	// 2min timeout --> timeout
	siApp.ExecutionTimeoutMilliSeconds = (2 * time.Minute).Milliseconds()
	timeout := ConvertSITimeoutWithAdjustment(siApp)
	assert.Equal(t, timeout, time.Millisecond)

	// 20min timeout --> no timeout, corrected to 10min
	siApp.ExecutionTimeoutMilliSeconds = (20 * time.Minute).Milliseconds()
	timeout = ConvertSITimeoutWithAdjustment(siApp)
	assert.Equal(t, timeout, 10*time.Minute)

	// 20min timeout, no creationTime --> no change
	siApp.Tags = map[string]string{}
	siApp.ExecutionTimeoutMilliSeconds = (20 * time.Minute).Milliseconds()
	timeout = ConvertSITimeoutWithAdjustment(siApp)
	assert.Equal(t, timeout, 20*time.Minute)

	// Illegal string --> no change
	siApp.Tags = map[string]string{
		"yunikorn.apache.org/CreationTime": "illegal",
	}
	siApp.ExecutionTimeoutMilliSeconds = (20 * time.Minute).Milliseconds()
	timeout = ConvertSITimeoutWithAdjustment(siApp)
	assert.Equal(t, timeout, 20*time.Minute)
}

func TestConvertSITimestamp(t *testing.T) {
	result := ConvertSITimestamp("160")
	assert.Equal(t, result, time.Unix(160, 0))

	result = ConvertSITimestamp("xzy")
	assert.Equal(t, result, time.Time{})
}
