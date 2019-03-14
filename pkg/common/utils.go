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
