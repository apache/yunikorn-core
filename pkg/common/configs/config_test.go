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
    "encoding/json"
    "gopkg.in/yaml.v2"
    "io/ioutil"
    "path"
    "testing"
)

func TestConfigSerde(t *testing.T) {
    conf := SchedulerConfig{
        Partitions: []PartitionConfig{
            {
                Name: "default",
                Queues: []QueueConfig{
                    {
                        Name: "a",
                        Properties: map[string]string{
                            "acl": "abc",
                            "x":   "y",
                        },
                        Resources: Resources{
                            Guaranteed: map[string]string{
                                "a": "100",
                            },
                            Max: map[string]string{

                            },
                        },
                        Queues: []QueueConfig{
                            {
                                Name: "a1",
                                Properties: map[string]string{},
                                Resources: Resources{
                                    Guaranteed: map[string]string{
                                        "memory": "100",
                                    },
                                    Max: map[string]string{
                                        "cpu": "10",
                                    },
                                },
                                Queues: []QueueConfig{},
                            }, {
                                Name: "a2",
                                Properties: map[string]string{},
                                Resources: Resources{
                                    Guaranteed: map[string]string{
                                        "memory": "200",
                                    },
                                    Max: map[string]string{
                                        "cpu": "20",
                                    },
                                },
                                Queues: []QueueConfig{},
                            },

                        },
                    },
                    {
                        Name: "b",
                        Properties: map[string]string{
                            "acl": "abc",
                            "x":   "y",
                        },
                        Resources: Resources{
                            Guaranteed: map[string]string{
                                "a": "100",
                            },
                            Max: map[string]string{

                            },
                        },
                        Queues: []QueueConfig{
                            {
                                Name: "b1",
                                Properties: map[string]string{},
                                Resources: Resources{
                                    Guaranteed: map[string]string{
                                        "memory": "10",
                                    },
                                    Max: map[string]string{
                                        "cpu": "1",
                                    },
                                },
                                Queues: []QueueConfig{},
                            }, {
                                Name: "b2",
                                Properties: map[string]string{},
                                Resources: Resources{
                                    Guaranteed: map[string]string{
                                        "memory": "20",
                                    },
                                    Max: map[string]string{
                                        "cpu": "2",
                                    },
                                },
                                Queues: []QueueConfig{},
                            },
                        },
                    },
                },
            },
        },
    }

    d, err := yaml.Marshal(&conf)
    if err != nil {
        t.Errorf("error: %v", err)
    }

    t.Logf(string(d))

    newConf := SchedulerConfig{}

    err = yaml.Unmarshal(d, &newConf)

    if err != nil {
        t.Errorf("error: %v", err)
    }

    a, _ := json.Marshal(newConf)
    b, _ := json.Marshal(conf)

    if string(a) != string(b) {
        t.Error("issue in compare")
        t.Errorf("original=[%s]", a)
        t.Errorf("serde   =[%s]", b)
    }
}

func TestLoadQueueConfig(t *testing.T) {
    data := `
partitions:
  -
    name: default
    queues:
      -
        name: a
        properties:
          acl: abc
          x: y
        resources:
          guaranteed:
            a: 100
            b: 200
          max:
            a: 200
            b: 300
        queues:
          -
            name: a1
            properties:
              acl: a1bc
          -
            name: a2
            properties:
              acl: a2bc
      -
        name: b
        properties:
          acl: abc
          x: y
        children:
          - b1
          - b2
  -
    name: gpu
    queues:
      -
        name: a1
        properties:
          acl: abc
          x: y
      -
        name: b1
        properties:
          acl: abc
          x: y
`

    dir, err := ioutil.TempDir("", "test-scheduler-config")
    if err != nil {
        t.Errorf("error: %v", err)
    }

    err = ioutil.WriteFile(path.Join(dir, "test-scheduler-config.yaml"), []byte(data), 0644)
    if err != nil {
        t.Errorf("error: %v", err)
    }

    // Read from tmp
    ConfigMap[SCHEDULER_CONFIG_PATH] = dir
    conf, err := SchedulerConfigLoader("test-scheduler-config")

    if err != nil {
        t.Errorf("error: %v", err)
    }

    if conf.Partitions[0].Name != "default" {
        t.Errorf("Failed to load conf from file %v", conf)
    }

    if len(conf.Partitions[0].Queues) != 2 || len(conf.Partitions[0].Queues) != 2 {
        t.Errorf("Failed to load queues from file %v", conf)
    }

    if conf.Partitions[0].Queues[0].Queues[0].Name != "a1" {
        t.Errorf("Failed to load queues from file %v", conf)
    }

    if conf.Partitions[0].Queues[0].Resources.Guaranteed["a"] != "100" {
        t.Errorf("Failed to load guranteed resource from file %v", conf)
    }

    if conf.Partitions[0].Queues[0].Resources.Max["a"] != "200" {
        t.Errorf("Failed to load guranteed resource from file %v", conf)
    }

    if conf.Partitions[1].Queues[0].Resources.Max != nil {
        t.Errorf("Failed to load guranteed resource from file %v", conf)
    }
}