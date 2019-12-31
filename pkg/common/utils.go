/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

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
	"strings"
	"time"
)

func GetNormalizedPartitionName(partitionName string, rmID string) string {
	if partitionName == "" {
		partitionName = "default"
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
		} else {
			time.Sleep(interval)
			continue
		}
	}
}
