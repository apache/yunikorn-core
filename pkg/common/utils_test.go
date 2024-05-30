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
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/common"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const (
	testKey = "testKey"
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

func TestIsAppCreationForced(t *testing.T) {
	assert.Check(t, !IsAppCreationForced(nil), "nil tags should not result in forced app creation")
	tags := make(map[string]string)
	assert.Check(t, !IsAppCreationForced(tags), "empty tags should not result in forced app creation")
	tags[common.AppTagCreateForce] = "false"
	assert.Check(t, !IsAppCreationForced(tags), "false creation tag should not result in forced app creation")
	tags[common.AppTagCreateForce] = "invalid"
	assert.Check(t, !IsAppCreationForced(tags), "invalid creation tag should not result in forced app creation")
	tags[common.AppTagCreateForce] = "true"
	assert.Check(t, IsAppCreationForced(tags), "creation tag should result in forced app creation")
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
	target := false
	eval := func() bool {
		return target
	}
	tests := []struct {
		input    bool
		interval time.Duration
		timeout  time.Duration
		output   error
	}{
		{true, time.Duration(1) * time.Second, time.Duration(2) * time.Second, nil},
		{false, time.Duration(1) * time.Second, time.Duration(2) * time.Second, fmt.Errorf("timeout waiting for condition")},
		{true, time.Duration(3) * time.Second, time.Duration(2) * time.Second, nil},
	}
	for _, test := range tests {
		target = test.input
		get := WaitFor(test.interval, test.timeout, eval)
		if test.output == nil {
			assert.NilError(t, get)
		} else {
			assert.Equal(t, get.Error(), test.output.Error())
		}
	}
}

func TestGetConfigurationBool(t *testing.T) {
	testCases := []struct {
		name          string
		configs       map[string]string
		defaultValue  bool
		expectedValue bool
	}{
		{
			name:          "configs is nil",
			configs:       nil,
			defaultValue:  true,
			expectedValue: true,
		},
		{
			name:          "key not exist",
			configs:       map[string]string{},
			defaultValue:  true,
			expectedValue: true,
		},
		{
			name:          "key exist, value is not bool",
			configs:       map[string]string{testKey: "xyz"},
			defaultValue:  true,
			expectedValue: true,
		},
		{
			name:          "key exist, value is different from default value",
			configs:       map[string]string{testKey: "false"},
			defaultValue:  true,
			expectedValue: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedValue, GetConfigurationBool(tc.configs, testKey, tc.defaultValue))
		})
	}
}

func TestGetConfigurationUint(t *testing.T) {
	testCases := []struct {
		name          string
		configs       map[string]string
		defaultValue  uint64
		expectedValue uint64
	}{
		{
			name:          "configs is nil",
			configs:       nil,
			defaultValue:  100,
			expectedValue: 100,
		},
		{
			name:          "key not exist",
			configs:       map[string]string{},
			defaultValue:  100,
			expectedValue: 100,
		},
		{
			name:          "key exist, value is not uint64",
			configs:       map[string]string{testKey: "-1000"},
			defaultValue:  100,
			expectedValue: 100,
		},
		{
			name:          "key exist, value is different from default value",
			configs:       map[string]string{testKey: "1"},
			defaultValue:  100,
			expectedValue: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedValue, GetConfigurationUint(tc.configs, testKey, tc.defaultValue))
		})
	}
}

func TestGetConfigurationInt(t *testing.T) {
	testCases := []struct {
		name          string
		configs       map[string]string
		defaultValue  int
		expectedValue int
	}{
		{
			name:          "configs is nil",
			configs:       nil,
			defaultValue:  100,
			expectedValue: 100,
		},
		{
			name:          "key not exist",
			configs:       map[string]string{},
			defaultValue:  100,
			expectedValue: 100,
		},
		{
			name:          "key exist, value is not int",
			configs:       map[string]string{testKey: "xyz"},
			defaultValue:  100,
			expectedValue: 100,
		},
		{
			name:          "key exist, value is different from default value",
			configs:       map[string]string{testKey: "-1"},
			defaultValue:  100,
			expectedValue: -1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedValue, GetConfigurationInt(tc.configs, testKey, tc.defaultValue))
		})
	}
}
