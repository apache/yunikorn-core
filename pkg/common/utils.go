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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
	interfaceCommon "github.com/apache/incubator-yunikorn-scheduler-interface/lib/go/common"
)

func GetNormalizedPartitionName(partitionName string, rmID string) string {
	if partitionName == "" {
		partitionName = "default"
	}

	// handle already normalized partition name
	if strings.HasPrefix(partitionName, "[") {
		return partitionName
	}
	return fmt.Sprintf("[%s]%s", rmID, partitionName)
}

func GetRMIdFromPartitionName(partitionName string) string {
	idx := strings.Index(partitionName, "]")
	if idx > 0 {
		rmID := partitionName[1:idx]
		return rmID
	}
	return ""
}

func GetPartitionNameWithoutClusterID(partitionName string) string {
	idx := strings.Index(partitionName, "]")
	if idx > 0 {
		return partitionName[idx+1:]
	}
	return partitionName
}

func WaitFor(interval time.Duration, timeout time.Duration, condition func() bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for condition")
		}
		if condition() {
			return nil
		}
		time.Sleep(interval)
		continue
	}
}

// Generate a new uuid. The chance that we generate a collision is really small.
// As long as we check the UUID before we communicate it back to the RM we can still replace it without a problem.
func GetNewUUID() string {
	return uuid.NewString()
}

func GetBoolEnvVar(key string, defaultVal bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			log.Logger().Debug("Failed to parse environment variable, using default value",
				zap.String("name", key),
				zap.String("value", value),
				zap.Bool("default", defaultVal))
			return defaultVal
		}
		return boolValue
	}
	return defaultVal
}

// Convert a SI execution timeout, given in milliseconds into a time.Duration object.
// This will always return a positive value or zero (0).
// A negative timeout will be converted into zero (0), which means never timeout.
// The conversion handles overflows in the conversion by setting it to zero (0) also.
func ConvertSITimeout(millis int64) time.Duration {
	// handle negative and 0 value (no timeout)
	if millis <= 0 {
		return time.Duration(0)
	}
	// just handle max wrapping, no need to handle min wrapping
	result := millis * int64(time.Millisecond)
	if result/millis != int64(time.Millisecond) {
		log.Logger().Warn("Timeout conversion wrapped: returned no timeout",
			zap.Int64("configured timeout in ms", millis))
		return time.Duration(0)
	}
	return time.Duration(result)
}

func GetRequiredNodeFromTag(tags map[string]string) string {
	var nodeName string
	if nodeName, ok := tags[interfaceCommon.DomainYuniKorn+interfaceCommon.KeyRequiredNode]; !ok {
		log.Logger().Debug("The required Node in allocationAsk is empty")
		return ""
	}
	return nodeName
}
