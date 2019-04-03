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

package configs

import (
    "fmt"
    "github.com/golang/glog"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "log"
    "path"
)

type SchedulerConfig struct {
    Partitions []PartitionConfig
}

type PartitionConfig struct {
    Name   string
    Queues []QueueConfig
}

type QueueConfig struct {
    Name       string
    Resources  Resources
    Properties map[string]string
    Queues      []QueueConfig
}

type Resources struct {
    Guaranteed map[string]string
    Max        map[string]string
}

type LoadSchedulerConfigFunc func(policyGroup string) (*SchedulerConfig, error)

// Visible by tests
func LoadSchedulerConfigFromByteArray(content []byte) (*SchedulerConfig, error) {
    conf := &SchedulerConfig{}
    err := yaml.Unmarshal(content, conf)
    if err != nil {
        log.Fatalf("error: %v", err)
        return nil, err
    }

    return conf, nil
}

func loadSchedulerConfigFromFile(policyGroup string) (*SchedulerConfig, error) {
    var filePath string
    if configDir, ok := ConfigMap[SchedulerConfigPath]; ok {
        filePath = path.Join(configDir, fmt.Sprintf("%s.yaml", policyGroup))
    } else {
        filePath = DefaultSchedulerConfigPath
    }
    glog.V(3).Infof("loading configuration from path %s", filePath)
    buf, err := ioutil.ReadFile(filePath)
    if err != nil {
        log.Fatalf("error: %v", err)
        return nil, err
    }
    return LoadSchedulerConfigFromByteArray(buf)
}

// Default loader, can be updated by tests
var SchedulerConfigLoader LoadSchedulerConfigFunc = loadSchedulerConfigFromFile
