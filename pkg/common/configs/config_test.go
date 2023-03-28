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

package configs

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"gotest.tools/v3/assert"

	"gopkg.in/yaml.v2"
)

var emptySum = ""

const validConf = `
partitions:
  -
    name: default
    placementrules:
      - name: tag
        value: namespace
        create: true
    queues:
      - name: root
        submitacl: '*'
        properties:
          application.sort.policy: stateaware
          sample: value2
`

func TestConfigSerdeQueues(t *testing.T) {
	conf := SchedulerConfig{
		Partitions: []PartitionConfig{
			{
				Name: "default",
				Queues: []QueueConfig{
					{
						Name:   "a",
						Parent: true,
						Properties: map[string]string{
							"acl": "abc",
							"x":   "y",
						},
						Resources: Resources{
							Guaranteed: map[string]string{
								"a": "100",
							},
							Max: map[string]string{},
						},
						Queues: []QueueConfig{
							{
								Name:       "a1",
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
								Name:       "a2",
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
						Name:   "b",
						Parent: true,
						Properties: map[string]string{
							"acl": "abc",
							"x":   "y",
						},
						Resources: Resources{
							Guaranteed: map[string]string{
								"a": "100",
							},
							Max: map[string]string{},
						},
						Queues: []QueueConfig{
							{
								Name:       "b1",
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
								Name:       "b2",
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
		Checksum: emptySum,
	}

	SerdeTest(t, conf, "QueueConfig")
}

func TestConfigSerdeLimits(t *testing.T) {
	conf := SchedulerConfig{
		Partitions: []PartitionConfig{
			{
				Name: "default",
				Queues: []QueueConfig{
					{
						Name: "test",
						Limits: []Limit{
							{
								Limit: "queue limit",
								Users: []string{
									"user1",
								},
								Groups: []string{
									"group1",
								},
								MaxResources: map[string]string{
									"memory": "10",
									"vcores": "10",
								},
								MaxApplications: 1,
							},
						},
					},
				},
				Limits: []Limit{
					{
						Limit: "partition limit 1",
						Users: []string{
							"user1",
						},
						Groups: []string{
							"group1",
						},
						MaxResources: map[string]string{
							"memory": "10",
							"vcores": "10",
						},
						MaxApplications: 1,
					}, {
						Limit: "partition limit 2",
						Users: []string{
							"user2",
						},
						MaxResources: map[string]string{
							"memory": "10",
						},
					}, {
						Limit: "partition limit 3",
						Groups: []string{
							"group3",
						},
						MaxResources:    nil,
						MaxApplications: 1,
					},
				},
			},
		},
		Checksum: emptySum,
	}

	SerdeTest(t, conf, "LimitConfig")
}

func SerdeTest(t *testing.T, conf SchedulerConfig, description string) {
	// convert the object to yaml
	yamlConf, err := yaml.Marshal(&conf)
	assert.NilError(t, err, "error marshalling yaml config '%s'", description)

	// unmarshal what we have just created
	newConf := SchedulerConfig{}
	err = yaml.Unmarshal(yamlConf, &newConf)
	if err != nil {
		t.Errorf("error unmarshalling serde yaml '%s': %v", description, err)
	}

	// marshal as json and we still should get the same objects
	jsonConf, err := json.Marshal(conf)
	assert.NilError(t, err, "error marshalling yaml config '%s'", description)

	jsonConf2, err := json.Marshal(newConf)
	assert.NilError(t, err, "error marshalling json config from serde yaml config '%s'", description)

	if string(jsonConf) != string(jsonConf2) {
		t.Errorf("json marshaled strings differ for '%s':", description)
		t.Errorf("original=[%s]", jsonConf)
		t.Errorf("serde   =[%s]", jsonConf2)
	}
}

func CreateConfig(data string) (*SchedulerConfig, error) {
	return LoadSchedulerConfigFromByteArray([]byte(data))
}

func TestLoadQueueConfig(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
        queues:
          - name: production
            resources:
              guaranteed:
                {memory: 1000, vcore: 10}
              max:
                {memory: 10000, vcore: 100}
          - name: test
            properties:
              something: withAvalue
            resources:
              guaranteed:
                memory: 200
                vcore: 2
              max:
                memory: 3000
                vcore: 30
          - name: sandbox
            parent: true
            submitacl: " sandbox"
            resources:
              guaranteed:
                memory: 400
                vcore: 4
              max:
                memory: 5000
                vcore: 50
  - name: gpu
    queues:
      - name: production
        adminacl: "admin admin"
        maxapplications: 10
      - name: test
        submitacl: "*"
        resources:
          max: {memory: 1000, vcore: 10}
`

	// create the config and process it
	conf, err := CreateConfig(data)
	assert.NilError(t, err, "loading failed with error")

	if conf.Partitions[0].Name != "default" {
		t.Errorf("default partition not found in config: %v", conf)
	}

	// gone through validation: both 1 top level queues
	if len(conf.Partitions[0].Queues) != 1 || len(conf.Partitions[1].Queues) != 1 {
		t.Errorf("failed to load queues from file %v", conf)
	}

	// root.sandbox queue
	if conf.Partitions[0].Queues[0].Queues[2].Name != "sandbox" {
		t.Errorf("failed to load sub queues from file %v", conf)
	}

	// root.production queue
	if conf.Partitions[0].Queues[0].Queues[0].Resources.Guaranteed["memory"] != "1000" {
		t.Errorf("failed to load guaranteed resource from file %v", conf)
	}

	// root.test queue
	if conf.Partitions[0].Queues[0].Queues[1].Resources.Max["vcore"] != "30" {
		t.Errorf("failed to load max resource from file %v", conf)
	}

	// root.test queue
	if len(conf.Partitions[0].Queues[0].Queues[1].Properties) == 0 {
		t.Errorf("failed to load properties from file %v", conf)
	}

	// gpu.production
	if conf.Partitions[1].Queues[0].Queues[0].Name != "production" {
		t.Errorf("failed to load 2nd partition queues from file %v", conf)
	}

	if conf.Partitions[1].Queues[0].Queues[0].MaxApplications != 10 {
		t.Errorf("failed to load 2nd partition queues from file %v", conf)
	}

	// gpu.production
	if conf.Partitions[1].Queues[0].Queues[0].AdminACL != "admin admin" {
		t.Errorf("failed to load admin ACL from file %v", conf)
	}

	// gpu.production
	if conf.Partitions[1].Queues[0].Queues[1].SubmitACL != "*" {
		t.Errorf("failed to load submit ACL from file %v", conf)
	}
}

func TestDeepQueueHierarchy(t *testing.T) {
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
	// create the config and process it
	// validate the config and check after the update
	_, err := CreateConfig(data)
	if err != nil {
		t.Errorf("deep queue hierarchy test should not have failed: %v", err)
	}

	data = `
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
                          - name: $$$$
`
	// create the config and process it
	// validate the config and check after the update
	var conf *SchedulerConfig
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("deep queue hierarchy test should have failed: %v", conf)
	}
}

func TestParsePartitionFail(t *testing.T) {
	data := `
partitions:
  - name: default
`
	// create the config and process it
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	if err == nil {
		t.Errorf("no queue config parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
  - name: default
    queues:
      - name: root
  - name: part1
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	assert.Error(t, err, "duplicate partition name found with name default")
	if err == nil {
		t.Errorf("multiple default partitions parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
  - name: part1
    queues:
      - name: root
  - name: part1
    queues:
      - name: root
  - name: part2
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	assert.Error(t, err, "duplicate partition name found with name part1")
	if err == nil {
		t.Errorf("duplicate partitions parsing should have failed: %v", conf)
	}
}

func TestParseQueue(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_
`
	// This tests two things:
	// - every allowed char in the regexp
	// - max length of the name
	// there are 64 different allowed characters in the regexp and the max length is 64 too
	_, err := CreateConfig(data)
	assert.NilError(t, err, "every allowed char queue name should not have failed")
}

func TestParseQueueFail(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
        resources:
          max: {memory: 10000, vcore: 100}
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	if err == nil {
		t.Errorf("resource limit on root queue parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: test
      - name: test
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("duplicate queue names parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: this-name-is-longer-than-sixtyfour-characters-and-thus-fails-parsing
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("too long queue name parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: no.in-name
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("dot in queue names parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: special-$-name
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("special char in queue names parsing should have failed: %v", conf)
	}
}

func TestParseResourceFail(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: test
        resources:
          max:
            memory: text
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	if err == nil {
		t.Errorf("resource not a number queue parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: test
        resources:
          max: {memory: }
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("resource not a number queue parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: test
        resources:
          max:
            memory:
            vcore:
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("resource not a number queue parsing should have failed: %v", conf)
	}
}

func TestParseACLFail(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
        adminacl: "users groups something_to_fail_it"
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	if err == nil {
		t.Errorf("3 fields in ACL queue parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
        adminacl: *
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("unescaped wild card ACL queue parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
        adminacl: "user""
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("unbalanced quotes ACL queue parsing should have failed: %v", conf)
	}
}

func TestPartitionPreemptionParameter(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
    preemption:
      enabled: true
  - name: "partition-0"
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	assert.NilError(t, err, "should expect no error")

	if !conf.Partitions[0].Preemption.Enabled {
		t.Error("default partition's preemption should be enabled.")
	}

	if conf.Partitions[1].Preemption.Enabled {
		t.Error("partition-0's preemption should NOT be enabled by default")
	}
}

func TestParseRule(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        create: true
        filter:
          type: allow
          users:
            - test1
            - test1
          groups:
            - test1
            - test1
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	assert.NilError(t, err, "rule parsing should not have failed")
	rule := conf.Partitions[0].PlacementRules[0]
	if !rule.Create {
		t.Errorf("Create flag is not set correctly expected 'true' got 'false'")
	}
	if rule.Parent != nil {
		t.Errorf("Parent rule was set incorrectly expected 'nil' got %v", rule.Parent)
	}
	if rule.Filter.Type != "allow" {
		t.Errorf("Filter type set incorrectly expected 'allow' got %v", rule.Filter.Type)
	}
	if len(rule.Filter.Groups) != 2 || len(rule.Filter.Users) != 2 {
		t.Errorf("Failed rule or group filter parsing length should have been 2 for both, got: %v, %v", rule.Filter.Users, rule.Filter.Groups)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        create: false
        parent:
          name: PrimaryGroup
          create: false
          filter:
            type: allow
            users:
              - test1
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	assert.NilError(t, err, "rule parsing should not have failed")
	rule = conf.Partitions[0].PlacementRules[0]
	if rule.Create {
		t.Errorf("Create flag is not set correctly expected 'false' got 'true'")
	}
	if rule.Parent == nil {
		t.Errorf("Parent rule was set incorrectly expected 'rule ' got nil")
	}
	if rule.Parent.Create {
		t.Errorf("Create flag is not set correctly expected 'false' got 'true'")
	}
	if len(rule.Parent.Filter.Users) != 1 && len(rule.Parent.Filter.Groups) != 0 {
		t.Errorf("Failed rule or group filter parsing length should have been 1 and 0, got: %v, %v", rule.Filter.Users, rule.Filter.Groups)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        filter:
          users:
            - test.test
      - name: PrimaryGroup
        filter:
          users:
            - test1*
      - name: Something
        filter:
          users:
            - test[1-9]
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	assert.NilError(t, err, "rule parsing should not have failed")
	if len(conf.Partitions[0].PlacementRules) != 3 {
		t.Errorf("incorrect number of rules returned expected 3 got: %d", len(conf.Partitions[0].PlacementRules))
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: fixed
        value: root
      - name: tag
        value: Just Any value
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	assert.NilError(t, err, "rule parsing should not have failed")
	if len(conf.Partitions[0].PlacementRules) != 2 {
		t.Errorf("incorrect number of rules returned expected 2 got: %d", len(conf.Partitions[0].PlacementRules))
	}
	if conf.Partitions[0].PlacementRules[0].Value != "default" &&
		conf.Partitions[0].PlacementRules[1].Value != "Just Any value" {
		t.Errorf("incorrect values set inside the rules: %v", conf.Partitions[0].PlacementRules)
	}
}

func TestParseRuleFail(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        create: bogus
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	if err == nil {
		t.Errorf("not a boolean create flag rule parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        filter:
          type: bogus
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("filter type parsing should have failed rule parsing: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        filter:
          users: 
            - 99test
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("user parsing filter should have failed rule parsing: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        filter:
          groups:
            - test@group
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("group parsing filter should have failed rule parsing: %v", conf)
	}

	data = `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: User
        filter:
          users:
            - test[test
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("user parsing filter should have failed rule parsing: %v", conf)
	}
}

func TestRecurseParent(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
    placementrules:
      - name: user
        parent:
          name: fixed
          value: test
          parent:
            name: provided
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	if err != nil {
		t.Errorf("recursive parent rule parsing should not have failed: %v", conf)
	}
}

func TestPartitionLimits(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
    limits:
      - limit:
        users:
        - user1
        maxresources: {memory: 10000, vcore: 10}
        maxapplications: 5
      - limit:
        users:
        - user2
        groups:
        - prod
        maxapplications: 10
      - limit:
        groups:
        - dev
        - test
        maxapplications: 20
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	assert.NilError(t, err, "partition user parsing should not have failed")
	// gone through validation: 1 top level queues
	if len(conf.Partitions[0].Queues) != 1 {
		t.Errorf("failed to load queue %v", conf)
	}
	if len(conf.Partitions[0].Limits) != 3 {
		t.Errorf("partition users linked not correctly loaded  %v", conf)
	}

	// limit 1 check
	limit := conf.Partitions[0].Limits[0]
	if len(limit.Users) != 1 && limit.Users[0] != "user1" && len(limit.Groups) != 0 {
		t.Errorf("failed to load max apps %v", limit)
	}
	if len(limit.Groups) != 0 {
		t.Errorf("group list should be empty and is not: %v", limit)
	}
	if limit.MaxApplications != 10 && (len(limit.MaxResources) != 2 && limit.MaxResources["memory"] != "10000") {
		t.Errorf("failed to load max resources %v", limit)
	}

	// limit 2 check
	limit = conf.Partitions[0].Limits[1]
	if len(limit.Users) != 1 && limit.Users[0] != "user2" &&
		len(limit.Groups) != 1 && limit.Groups[0] != "prod" {
		t.Errorf("user and group list have incorrect entries (limit 2): %v", limit)
	}
	if limit.MaxApplications != 5 && (limit.MaxResources != nil || len(limit.MaxResources) != 0) {
		t.Errorf("loaded resource limits incorrectly (limit 2): %v", limit)
	}

	// limit 3 check
	limit = conf.Partitions[0].Limits[2]
	if len(limit.Groups) != 2 && limit.Groups[0] != "dev" && limit.Groups[1] != "test" {
		t.Errorf("failed to load groups from config (limit 3): %v", limit)
	}
	if len(limit.Users) != 0 {
		t.Errorf("user list should be empty and is not (limit 3): %v", limit)
	}
	if limit.MaxApplications != 20 && (limit.MaxResources != nil || len(limit.MaxResources) != 0) {
		t.Errorf("loaded resource limits incorrectly (limit 3): %v", limit)
	}
}

func TestQueueLimits(t *testing.T) {
	data := `
partitions:
  - name: default
    queues:
      - name: root
        limits:
          - limit:
            users: 
            - user1
            maxresources: {memory: 10000, vcore: 10}
            maxapplications: 5
          - limit:
            users:
            - user2
            groups:
            - prod
            maxapplications: 10
        queues:
          - name: level1
            limits:
              - limit:
                users: 
                - subuser
                maxapplications: 1
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	assert.NilError(t, err, "config parsing should not have failed")
	// gone through validation: 1 top level queues
	if len(conf.Partitions[0].Queues) != 1 {
		t.Errorf("failed to load queue from config: %v", conf)
	}
	if len(conf.Partitions[0].Limits) != 0 {
		t.Errorf("root queue limits linked to partition: %v", conf)
	}

	// limit number
	if len(conf.Partitions[0].Queues[0].Limits) != 2 {
		t.Errorf("failed to load queue limits from config: %v", conf)
	}

	// limit 1 check
	limit := conf.Partitions[0].Queues[0].Limits[0]
	if len(limit.Users) != 1 && limit.Users[0] != "user1" && len(limit.Groups) != 0 {
		t.Errorf("user and group list have incorrect entries: %v", limit)
	}
	if limit.MaxApplications != 5 && (len(limit.MaxResources) != 2 && limit.MaxResources["memory"] != "10000") {
		t.Errorf("loaded resource limits incorrectly: %v", limit)
	}

	// limit 2 check
	limit = conf.Partitions[0].Queues[0].Limits[1]
	if len(limit.Users) != 1 && limit.Users[0] != "user2" && limit.Groups[0] != "prod" {
		t.Errorf("user and group list have incorrect entries (limit 2): %v", limit)
	}
	if limit.MaxApplications != 10 && (limit.MaxResources != nil || len(limit.MaxResources) != 0) {
		t.Errorf("loaded resource limits incorrectly (limit 2): %v", limit)
	}

	// sub queue limit check
	limit = conf.Partitions[0].Queues[0].Queues[0].Limits[0]
	if len(limit.Users) != 1 && limit.Users[0] != "subuser" && len(limit.Groups) != 0 {
		t.Errorf("user and group list have incorrect entries (sub queue): %v", limit)
	}
	if limit.MaxApplications != 1 && (limit.MaxResources != nil || len(limit.MaxResources) != 0) {
		t.Errorf("loaded resource limits incorrectly (sub queue): %v", limit)
	}
}

func TestComplexUsers(t *testing.T) {
	data := `
partitions:
  - name: default
    limits:
      - limit: dot user
        users:
        - user.lastname
        maxapplications: 1
      - limit: "@ user"
        users:
        - user@domain
        maxapplications: 1
      - limit: wildcard user
        users:
        - "*"
        maxapplications: 1
      - limit: wildcard group
        groups:
        - "*"
        maxapplications: 1
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	assert.NilError(t, err, "config parsing should not have failed")
	// gone through validation: 1 top level queues
	if len(conf.Partitions[0].Queues) != 1 && len(conf.Partitions[0].Queues[0].Limits) != 0 {
		t.Errorf("failed to load queues from config: %v", conf)
	}
	if len(conf.Partitions[0].Limits) != 4 {
		t.Errorf("failed to load partition limits from config: %v", conf)
	}

	// user with dot check
	limit := conf.Partitions[0].Limits[0]
	if len(limit.Users) != 1 && limit.Users[0] != "user.lastname" {
		t.Errorf("failed to load 'dot' user from config: %v", limit)
	}

	// user with @ check
	limit = conf.Partitions[0].Limits[1]
	if len(limit.Users) != 1 && limit.Users[0] != "user@domain" {
		t.Errorf("failed to load '@' user from config: %v", limit)
	}

	// user wildcard
	limit = conf.Partitions[0].Limits[2]
	if len(limit.Users) != 0 && limit.Users[0] != "*" {
		t.Errorf("failed to load wildcard user from config: %v", limit)
	}

	// group wildcard
	limit = conf.Partitions[0].Limits[3]
	if len(limit.Groups) != 1 && limit.Groups[0] != "*" {
		t.Errorf("failed to load wildcard group from config: %v", limit)
	}
}

func TestLimitsFail(t *testing.T) {
	data := `
partitions:
  - name: default
    limits:
      - limit:
        users:
        - user1
        maxresources: {}
        maxapplications: 0
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err := CreateConfig(data)
	if err == nil {
		t.Errorf("limit parsing should have failed resource nil: %v", conf)
	}

	data = `
partitions:
  - name: default
    limits:
      - limit:
        users:
        - user1
        maxapplications: 0
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("limit parsing should have failed max app zero: %v", conf)
	}

	data = `
partitions:
  - name: default
    limits:
      - limit:
        users:
        - user1
        maxresources: {memory: 0}
        maxapplications: 0
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("limit parsing should have failed all limits zero: %v", conf)
	}

	data = `
partitions:
  - name: default
    limits:
      - limit:
        users:
        - user
        maxresources: {memory: x}
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("queue user parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    limits:
      - limit:
        users:
        - user
        maxresources: {memory: 0}
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("queue user parsing should have failed: %v", conf)
	}

	data = `
partitions:
  - name: default
    limits:
      - limit: illegal user
        users:
        - user space
        maxapplications: 1
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("limit parsing should have failed user space: %v", conf)
	}

	data = `
partitions:
  - name: default
    limits:
      - limit: illegal group
        groups:
        - group@domain
        maxapplications: 1
    queues:
      - name: root
`
	// validate the config and check after the update
	conf, err = CreateConfig(data)
	if err == nil {
		t.Errorf("limit parsing should have failed group @: %v", conf)
	}

	data = `
partitions:
  - name: default
    limits:
      - limit: dot user
        users:
        - user.lastname
        maxapplications: 1
      - limit: "@ user"
        users:
        - user@domain
        maxapplications: 1
      - limit: wildcard user
        users:
        - "*"
        maxapplications: 1
      - limit: wildcard group
        groups:
        - "*"
        maxapplications: 1
      - limit: error no wildcard user after wildcard user
        users:
        - "user1"
        maxapplications: 1
    queues:
      - name: root
`
	// validate the config and check after the update
	_, err = CreateConfig(data)
	assert.ErrorContains(t, err, "should not set no wildcard user user1 after wildcard user limit")

	data = `
partitions:
  - name: default
    limits:
      - limit: dot user
        users:
        - user.lastname
        maxapplications: 1
      - limit: "@ user"
        users:
        - user@domain
        maxapplications: 1
      - limit: wildcard user
        users:
        - "*"
        maxapplications: 1
      - limit: wildcard group
        groups:
        - "*"
        maxapplications: 1
      - limit: error no wildcard group after wildcard user
        groups:
        - "group1"
        maxapplications: 1
    queues:
      - name: root
`
	// validate the config and check after the update
	_, err = CreateConfig(data)
	assert.ErrorContains(t, err, "should not set no wildcard group group1 after wildcard group limit")

	data = `
partitions:
  - name: default
    limits:
      - limit: dot user
        users:
        - user.lastname
        maxapplications: 1
      - limit: "@ user"
        users:
        - user@domain
        maxapplications: 1
      - limit: wildcard user
        users:
        - "*"
        maxapplications: 1
      - limit: wildcard group
        groups:
        - "*"
        maxapplications: 1
      - limit: more than one wildcard user
        users:
        - "*"
        maxapplications: 2
    queues:
      - name: root
`
	// validate the config and check after the update
	_, err = CreateConfig(data)
	assert.ErrorContains(t, err, "should not set more than one wildcard user")

	data = `
partitions:
  - name: default
    limits:
      - limit: dot user
        users:
        - user.lastname
        maxapplications: 1
      - limit: "@ user"
        users:
        - user@domain
        maxapplications: 1
      - limit: wildcard user
        users:
        - "*"
        maxapplications: 1
      - limit: wildcard group
        groups:
        - "*"
        maxapplications: 1
      - limit: more than one wildcard group
        groups:
        - "*"
        maxapplications: 2
    queues:
      - name: root
`
	// validate the config and check after the update
	_, err = CreateConfig(data)
	assert.ErrorContains(t, err, "should not set more than one wildcard group")
}

func TestLoadSchedulerConfigFromByteArray(t *testing.T) {
	invalidConf := `
partitions:
  -
    name: default
    placementrules:
      - name: tag
        value: namespace
        create: true
    queues:
      - name: root
        submitacl: '*'
        sample: value1
`
	mixedSpacesConf := `
partitions:
  -
    name: default
    placementrules:
      - name: tag
        value: namespace
        create: true
    queues:
      - name: root
        submitacl: '*'
        properties:
            application.sort.policy: stateaware
`
	testCases := []struct {
		name          string
		config        string
		errorExpected bool
	}{
		{"Valid config", validConf, false},
		{"Invalid checksum", invalidConf, true},
		{"Mixed space", mixedSpacesConf, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			schedulerConf, err := LoadSchedulerConfigFromByteArray([]byte(tc.config))
			if tc.errorExpected {
				assert.Assert(t, err != nil, "Error is expected")
				assert.Assert(t, strings.Contains(err.Error(), "unmarshal error"), "Unexpected error message")
			} else {
				assert.NilError(t, err, "No error is expected")
				assert.Assert(t, schedulerConf != nil, "Returned conf should not be nil")
			}
		})
	}
}

func TestGetConfigurationString(t *testing.T) {
	configBytes := []byte(validConf)
	checksum := "checksum: " + fmt.Sprintf("%X", sha256.Sum256(configBytes))
	testCases := []struct {
		name           string
		requestBytes   []byte
		expectedConfig string
	}{
		{"No checksum", configBytes, validConf},
		{"Checksum at the beginning", []byte(checksum + validConf), validConf},
		{"Checksum at the end", []byte(validConf + checksum), validConf},
		{"Checksum in the middle", []byte(validConf + checksum + "extra config"), validConf + "extra config"},
		{"Empty config and checksum", []byte(""), ""},
		{"Empty checksum", []byte(validConf + "checksum: "), validConf},
		{"Empty config", []byte("" + checksum), ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.DeepEqual(t, tc.expectedConfig, GetConfigurationString(tc.requestBytes))
		})
	}
}
