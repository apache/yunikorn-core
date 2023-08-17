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

package placement

import (
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/placement/types"
)

type recoveryRule struct {
	basicRule
}

// A rule to place an application into the recovery queue if no other rules matched and application submission is forced.
// This rule will be run implicitly after all other placement rules are evaluated to ensure that an application
// corresponding to an already-executing workload can be accepted successfully.
func (rr *recoveryRule) getName() string {
	return types.Recovery
}

func (rr *recoveryRule) initialise(conf configs.PlacementRule) error {
	// no configuration needed for the recovery rule
	return nil
}

func (rr *recoveryRule) placeApplication(app *objects.Application, _ func(string) *objects.Queue) (string, bool, error) {
	// only forced applications should resolve to the recovery queue
	if !app.IsCreateForced() {
		return "", false, nil
	}

	queueName := common.RecoveryQueueFull
	log.Log(log.Config).Info("Recovery rule application placed",
		zap.String("application", app.ApplicationID),
		zap.String("queue", queueName))
	return queueName, false, nil
}
