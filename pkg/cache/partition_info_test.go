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

package cache

import (
    "github.infra.cloudera.com/yunikorn/yunikorn-core/pkg/common/configs"
    "testing"
)

func TestLoadPartitionConfig(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: production
      - name: test
        queues:
          - name: admintest
`

    // create config from string
    configs.MockSchedulerConfigByData([]byte(data))
    conf, err := configs.SchedulerConfigLoader("default-policy-group")
    if err != nil {
        t.Errorf("Error when loading config %v", err)
        return
    }
    partition, err := NewPartitionInfo(conf.Partitions[0], "rm1")
    if err != nil {
        t.Errorf("Error when loading ParttionInfo from config %v", err)
        return
    }
    // There is a queue setup as the config must be valid when we run
    root := partition.getQueue("root")
    if root == nil {
        t.Errorf("root queue not found in partition")
    }
    adminTest :=  partition.getQueue("root.test.admintest")
    if adminTest == nil {
        t.Errorf("root.test.adminTest queue not found in partition")
    }
}

func TestLoadDeepQueueConfig(t *testing.T) {
    data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
          - name: level1
            queues:
              - name: level2
                queues:
                  - name: level3
                    queues:
                      - name: level4
                        queues:
                          - name: level5
`

    // create config from string
    configs.MockSchedulerConfigByData([]byte(data))
    conf, err := configs.SchedulerConfigLoader("default-policy-group")
    if err != nil {
        t.Errorf("Error when loading config %v", err)
        return
    }
    partition, err := NewPartitionInfo(conf.Partitions[0], "rm1")
    if err != nil {
        t.Errorf("Error when load ParttionInfo from config %v", err)
        return
    }
    // There is a queue setup as the config must be valid when we run
    root := partition.getQueue("root")
    if root == nil {
        t.Errorf("root queue not found in partition")
    }
    adminTest :=  partition.getQueue("root.level1.level2.level3.level4.level5")
    if adminTest == nil {
        t.Errorf("root.level1.level2.level3.level4.level5 queue not found in partition")
    }
}