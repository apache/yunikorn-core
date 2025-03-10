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
	appID    string
	nodeID   string
	allocKey string
	// these references must ONLY be used for alloc, node and application removal otherwise
	// the reservations cannot be removed and scheduling might be impacted.
	app   *Application
	node  *Node
	alloc *Allocation
}

// The reservation inside the scheduler. A reservation object is never mutated and does not use locking.
// The key depends on where the reservation was made (node or app).
// appBased must be true for a reservation for an app and false for a reservation on a node
func newReservation(node *Node, app *Application, alloc *Allocation, appBased bool) *reservation {
	if alloc == nil || app == nil || node == nil {
		log.Log(log.SchedReservation).Warn("Illegal reservation requested: one input is nil",
			zap.Stringer("node", node),
			zap.Stringer("app", app),
			zap.Stringer("alloc", alloc))
		return nil
	}
	res := &reservation{
		allocKey: alloc.GetAllocationKey(),
		alloc:    alloc,
		app:      app,
		node:     node,
	}
	if appBased {
		res.nodeID = node.NodeID
	} else {
		res.appID = app.ApplicationID
	}
	return res
}

func (r *reservation) String() string {
	if r == nil {
		return "nil reservation"
	}
	if r.nodeID == "" {
		return r.node.NodeID + " -> " + r.appID + "|" + r.allocKey
	}
	return r.app.ApplicationID + " -> " + r.nodeID + "|" + r.allocKey
}

// GetObjects returns the objects that created the reservation.
// None of the returned values will be nil unless the reservation itself is nil
func (r *reservation) GetObjects() (*Node, *Application, *Allocation) {
	if r != nil {
		return r.node, r.app, r.alloc
	}
	return nil, nil, nil
}
