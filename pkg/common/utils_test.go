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
	"os"
	"testing"

	"gotest.tools/assert"
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
			assert.DeepEqual(t, val, tc.expected)
			err = os.Unsetenv(envVarName)
			assert.NilError(t, err, "cleaning up environment variable failed")
		})
	}
}
