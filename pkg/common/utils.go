/*
Copyright 2019 The Unity Scheduler Authors

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
)

func GetNormalizedPartitionName(partitionName string, rmId string) string {
    if partitionName == "" {
        partitionName = "default"
    }

    return fmt.Sprintf("[%s]%s", rmId, partitionName)
}

func GetRMIdFromPartitionName(partitionName string) string {
    idx := strings.Index(partitionName, "]")
    if idx > 0 {
        rmId := partitionName[1:idx]
        return rmId
    }
    return ""
}

func GetPartitionNameWithoutClusterId(partitionName string) string {
    idx := strings.Index(partitionName, "]")
    if idx > 0 {
        return partitionName[idx+1:]
    }
    return partitionName
}
