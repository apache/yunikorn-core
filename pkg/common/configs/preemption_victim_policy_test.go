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
	"testing"

	"gotest.tools/v3/assert"
)

func TestPreemptionVictimPolicyValidation(t *testing.T) {
	for _, value := range []string{PreemptionVictimDefault, PreemptionVictimQueuePriority, "", "typo"} {
		t.Run(value, func(t *testing.T) {
			q := QueueConfig{Name: "root", Parent: true, Properties: map[string]string{PreemptionVictimPolicy: value}}
			err := checkQueues(&q, 0)
			if value == PreemptionVictimDefault || value == PreemptionVictimQueuePriority {
				assert.NilError(t, err)
			} else {
				assert.ErrorContains(t, err, PreemptionVictimPolicy)
			}
		})
	}
	q := QueueConfig{Name: "root", Parent: true, Queues: []QueueConfig{{Name: "child", Properties: map[string]string{PreemptionVictimPolicy: "typo"}}}}
	assert.ErrorContains(t, checkQueues(&q, 0), PreemptionVictimPolicy)
	q.Queues = nil
	assert.NilError(t, checkQueues(&q, 0))
}
