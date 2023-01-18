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

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
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
	var tests = []struct {
		envVarName string
		setENV     bool
		testname   string
		value      string
		expected   bool
	}{
		{"VAR", true, "ENV var not set", "", true},
		{"VAR", true, "ENV var set", "false", false},
		{"VAR", true, "Invalid value", "someValue", true},
		{"UNKOWN", false, "ENV doesn't exist", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.testname, func(t *testing.T) {
			if tt.setENV {
				if err := os.Setenv(tt.envVarName, tt.value); err != nil {
					t.Error("Setting environment variable failed")
				}
			}
			if val := GetBoolEnvVar(tt.envVarName, true); val != tt.expected {
				t.Errorf("Got %v, expected %v", val, tt.expected)
			}
			if tt.setENV {
				if err := os.Unsetenv(tt.envVarName); err != nil {
					t.Error("Cleaning up environment variable failed")
				}
			}
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

func TestIsAllowPreemptSelf(t *testing.T) {
	assert.Check(t, IsAllowPreemptSelf(nil), "Nil policy should allow preempt of self")
	assert.Check(t, IsAllowPreemptSelf(&si.PreemptionPolicy{AllowPreemptSelf: true}), "Preempt self should be allowed if policy allows")
	assert.Check(t, !IsAllowPreemptSelf(&si.PreemptionPolicy{AllowPreemptSelf: false}), "Preempt self should not be allowed if policy does not allow")
}

func TestAllowPreemptOther(t *testing.T) {
	assert.Check(t, !IsAllowPreemptOther(nil), "Nil policy should not allow preempt of other")
	assert.Check(t, IsAllowPreemptOther(&si.PreemptionPolicy{AllowPreemptOther: true}), "Preempt other should be allowed if policy allows")
	assert.Check(t, !IsAllowPreemptOther(&si.PreemptionPolicy{AllowPreemptOther: false}), "Preempt other should not be allowed if policy does not allow")
}

func TestConvertSITimeoutWithAdjustment(t *testing.T) {
	created := time.Now().Unix() - 600
	defaultTimeout := 15 * time.Minute
	tagsWithCreationTime := map[string]string{
		common.DomainYuniKorn + common.CreationTime: strconv.FormatInt(created, 10),
	}
	tagsWithIllegalCreationTime := map[string]string{
		common.DomainYuniKorn + common.CreationTime: "illegal",
	}
	siApp := &si.AddApplicationRequest{}

	// no timeout, no creationTime --> default
	siApp.ExecutionTimeoutMilliSeconds = 0
	timeout := ConvertSITimeoutWithAdjustment(siApp, defaultTimeout)
	assert.Equal(t, timeout, defaultTimeout)

	// no timeout, illegal string --> default
	siApp.ExecutionTimeoutMilliSeconds = 0
	siApp.Tags = tagsWithIllegalCreationTime
	timeout = ConvertSITimeoutWithAdjustment(siApp, defaultTimeout)
	assert.Equal(t, timeout, defaultTimeout)

	// 2min timeout --> timeout
	siApp.Tags = tagsWithCreationTime
	siApp.ExecutionTimeoutMilliSeconds = (2 * time.Minute).Milliseconds()
	timeout = ConvertSITimeoutWithAdjustment(siApp, defaultTimeout)
	assert.Equal(t, timeout, time.Millisecond)

	// 20min timeout --> no timeout, corrected to 10min
	siApp.Tags = tagsWithCreationTime
	siApp.ExecutionTimeoutMilliSeconds = (20 * time.Minute).Milliseconds()
	timeout = ConvertSITimeoutWithAdjustment(siApp, defaultTimeout).Round(time.Minute)
	assert.Equal(t, timeout, 10*time.Minute)

	// 20min timeout, no creationTime --> no change
	siApp.Tags = map[string]string{}
	siApp.ExecutionTimeoutMilliSeconds = (20 * time.Minute).Milliseconds()
	timeout = ConvertSITimeoutWithAdjustment(siApp, defaultTimeout)
	assert.Equal(t, timeout, 20*time.Minute)

	// Illegal string --> no change
	siApp.Tags = tagsWithIllegalCreationTime
	siApp.ExecutionTimeoutMilliSeconds = (20 * time.Minute).Milliseconds()
	timeout = ConvertSITimeoutWithAdjustment(siApp, defaultTimeout)
	assert.Equal(t, timeout, 20*time.Minute)
}

func TestConvertSITimestamp(t *testing.T) {
	result := ConvertSITimestamp("160")
	assert.Equal(t, result, time.Unix(160, 0))

	result = ConvertSITimestamp("xzy")
	assert.Equal(t, result, time.Time{})

	result = ConvertSITimestamp("-2000000")
	assert.Equal(t, result, time.Unix(0, 0))

	result = ConvertSITimestamp("")
	assert.Equal(t, result, time.Time{})
}

func TestWaitFor(t *testing.T) {
	var tests = []struct {
		testname   string
		bound      int
		ErrorExist bool
	}{
		{"Timeout case", 10000, true},
		{"Fullfilling case", 10, false},
	}
	for _, tt := range tests {
		t.Run(tt.testname, func(t *testing.T) {
			count := 0
			err := WaitFor(time.Nanosecond, time.Millisecond, func() bool {
				if count <= tt.bound {
					count++
					return false
				}
				return true
			})
			switch tt.ErrorExist {
			case true:
				if errorExist := (err != nil); !errorExist {
					t.Errorf("ErrorExist: got %v, expected %v", errorExist, tt.ErrorExist)
				}
			case false:
				if errorExist := (err == nil); !errorExist {
					t.Errorf("ErrorExist: got %v, expected %v", errorExist, tt.ErrorExist)
				}
			}
		})
	}
}
