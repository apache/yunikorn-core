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

package objects

import (
	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/log"
)

type reservation struct {
	nodeID string
	appID  string
	askKey string
	// these references must ONLY be used for ask, node and application removal otherwise
	// the reservations cannot be removed and scheduling might be impacted.
	app  *Application
	node *Node
	ask  *AllocationAsk
}

// The reservation inside the scheduler. A reservation object is never mutated and does not use locking.
// The key depends on where the reservation was made (node or app).
// appBased must be true for a reservation for an app and false for a reservation on a node
func newReservation(node *Node, app *Application, ask *AllocationAsk, appBased bool) *reservation {
	if ask == nil || app == nil || node == nil {
		log.Logger().Warn("Illegal reservation requested: one input is nil",
			zap.String("node", node.String()),
			zap.String("app", app.String()),
			zap.String("ask", ask.String()))
		return nil
	}
	res := &reservation{
		askKey: ask.AllocationKey,
		ask:    ask,
		app:    app,
		node:   node,
	}
	if appBased {
		res.nodeID = node.NodeID
	} else {
		res.appID = app.ApplicationID
	}
	return res
}

func reservationKey(node *Node, app *Application, ask *AllocationAsk) string {
	if ask == nil || (app == nil && node == nil) || (app != nil && node != nil) {
		log.Logger().Warn("Illegal reservation key requested",
			zap.Any("node", node),
			zap.Any("app", app),
			zap.Any("ask", ask))
		return ""
	}
	if node == nil {
		return app.ApplicationID + "|" + ask.AllocationKey
	}
	return node.NodeID + "|" + ask.AllocationKey
}

// Return the reservation key
func (r *reservation) getKey() string {
	if r.nodeID == "" {
		return r.appID + "|" + r.askKey
	}
	return r.nodeID + "|" + r.askKey
}

func (r *reservation) String() string {
	if r.nodeID == "" {
		return r.node.NodeID + " -> " + r.appID + "|" + r.askKey
	}
	return r.app.ApplicationID + " -> " + r.nodeID + "|" + r.askKey
}
