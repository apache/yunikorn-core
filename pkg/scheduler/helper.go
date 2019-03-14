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

package scheduler

import "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/api"

const (
    defaultJobId = "_default_job_"
    defaultPartitionName = "_default_partition_"
)

func GetJobIdFromTags(tags map[string]string) string {
    jobId := tags[api.JOB_ID]

    if jobId == "" {
        return defaultJobId
    }
    return jobId
}

func GetPartitionFromTags(tags map[string]string) string {
    partitionName := tags[api.NODE_PARTITION]

    if partitionName == "" {
        return defaultPartitionName
    }
    return partitionName
}
