/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

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

import (
	"go.uber.org/zap"

	"github.com/cloudera/yunikorn-core/pkg/log"
)

type reservation struct {
	nodeID string
	appID  string
	ask    *SchedulingAllocationAsk
}

// The reservation inside the scheduler. A reservation object is never mutated and does not use locking.
// One of the node and app must be nil, not both or neither. The ask must always be set.
// The key depends on where the reservation was made (node or app).
func newReservation(node *schedulingNode, app *SchedulingApplication, ask *SchedulingAllocationAsk) *reservation {
	if ask == nil || (app == nil && node == nil) || (app != nil && node != nil){
		log.Logger().Warn("Illegal reservation requested",
			zap.Any("node", node),
			zap.Any("app", app),
			zap.Any("ask", ask))
		return nil
	}
	res := &reservation{
		ask: ask,
	}
	// since one is nil this is needed to prevent panic
	if node == nil {
		res.appID = app.ApplicationInfo.ApplicationID
	} else {
		res.nodeID = node.NodeID
	}
	return res
}

func reservationKey(node *schedulingNode, app *SchedulingApplication, ask *SchedulingAllocationAsk) string {
	if ask == nil || (app == nil && node == nil) || (app != nil && node != nil) {
		log.Logger().Warn("Illegal reservation key requested",
			zap.Any("node", node),
			zap.Any("app", app),
			zap.Any("ask", ask))
		return ""
	}
	if node == nil {
		return app.ApplicationInfo.ApplicationID+"|"+ask.AskProto.AllocationKey
	}
	return node.NodeID+"|"+ask.AskProto.AllocationKey
}

// Return the reservation key
func (r *reservation) getKey() string {
	if r.nodeID == "" {
		return r.appID+"|"+r.ask.AskProto.AllocationKey
	}
	return r.nodeID+"|"+r.ask.AskProto.AllocationKey
}

