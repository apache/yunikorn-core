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
	"fmt"
	"testing"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
)

func TestGetOutstandingRequestsSelectedActiveAccounting(t *testing.T) {
	resource := func(amount resources.Quantity) *resources.Resource {
		return resources.NewResourceFromMap(map[string]resources.Quantity{"memory": amount})
	}
	for _, tc := range []struct {
		name                          string
		advertised                    bool
		budget                        resources.Quantity
		wantSelected                  resources.Quantity
		wantRequests, wantWithdrawals int
	}{
		{"new selected", false, 20, 12, 1, 0},
		{"advertised selected", true, 20, 12, 0, 0},
		{"advertised displaced", true, 8, 0, 0, 1},
		{"unadvertised displaced", false, 8, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := createRootQueue(nil)
			assert.NilError(t, err)
			leaf, err := createManagedQueue(root, "leaf", false, map[string]string{"memory": "20"})
			assert.NilError(t, err)
			app := &Application{ApplicationID: "app-Y", queue: leaf, queuePath: "root.leaf"}
			ask := newAllocationAsk("ask-Y", app.ApplicationID, resource(12))
			ask.SetSchedulingAttempted(true)
			ask.SetScaleUpTriggered(tc.advertised)
			app.sortedRequests.insert(ask)
			queueBudget, userBudget := resource(tc.budget), resource(20)
			var requests, withdrawals []*Allocation
			selectedTotal := app.getOutstandingRequests(queueBudget, userBudget, &requests, &withdrawals)
			assert.Check(t, resources.Equals(selectedTotal, resource(tc.wantSelected)), "selected total=%v, want %d", selectedTotal, tc.wantSelected)
			assert.Check(t, len(requests) == tc.wantRequests, "new requests=%d, want %d", len(requests), tc.wantRequests)
			assert.Check(t, len(withdrawals) == tc.wantWithdrawals, "withdrawals=%d, want %d", len(withdrawals), tc.wantWithdrawals)
			if len(requests) > 0 {
				assert.Assert(t, requests[0] == ask)
			}
			if len(withdrawals) > 0 {
				assert.Assert(t, withdrawals[0] == ask)
			}
			assert.Equal(t, ask.HasTriggeredScaleUp(), tc.advertised, "collection must not dispatch or change callback state")
			assert.Assert(t, leaf.getMaxHeadRoom().FitInMaxUndef(ask.GetAllocatedResource()), "displacement is not policy loss")
			assert.Assert(t, resources.Equals(queueBudget, resource(tc.budget)) && resources.Equals(userBudget, resource(20)))
			assert.Assert(t, resources.IsZero(leaf.GetAllocatedResource()))
		})
	}
}

func TestGetOutstandingRequestsSelectedActiveExclusions(t *testing.T) {
	resource := func(amount resources.Quantity) *resources.Resource {
		return resources.NewResourceFromMap(map[string]resources.Quantity{"memory": amount})
	}
	for _, kind := range []string{"required node", "replaceable", "allocated", "never attempted"} {
		for _, advertised := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/advertised=%t", kind, advertised), func(t *testing.T) {
				app := &Application{ApplicationID: "app-1"}
				special := newAllocationAsk("special", app.ApplicationID, resource(12))
				special.priority = 1
				special.SetSchedulingAttempted(true)
				special.SetScaleUpTriggered(advertised)
				wantSelected := resources.Quantity(0)
				wantRequests, wantWithdrawals := 0, 0
				switch kind {
				case "required node":
					special.SetRequiredNode("node-1")
				case "replaceable":
					special.taskGroupName = testgroup
					app.addPlaceholderData(special)
					assert.Assert(t, app.canReplace(special))
				case "allocated":
					assert.Assert(t, special.allocate())
					wantSelected, wantRequests = 9, 1
				case "never attempted":
					special.SetSchedulingAttempted(false)
					wantSelected, wantRequests = 9, 1
				}
				if advertised && wantRequests == 0 {
					wantWithdrawals = 1
				}
				peer := newAllocationAsk("peer", app.ApplicationID, resource(9))
				peer.SetSchedulingAttempted(true)
				app.sortedRequests.insert(special)
				app.sortedRequests.insert(peer)
				var requests, withdrawals []*Allocation
				// A fitting required-node/replacement ask still deducts 12 locally,
				// leaving only 8 for the peer, but is never selected demand itself.
				// Allocated and never-attempted asks bypass even that deduction.
				selectedTotal := app.getOutstandingRequests(resource(20), resource(20), &requests, &withdrawals)
				assert.Assert(t, resources.Equals(selectedTotal, resource(wantSelected)))
				assert.Equal(t, len(requests), wantRequests)
				assert.Equal(t, len(withdrawals), wantWithdrawals)
				if len(requests) > 0 {
					assert.Assert(t, requests[0] == peer)
				}
				if len(withdrawals) > 0 {
					assert.Assert(t, withdrawals[0] == special)
				}
				assert.Equal(t, special.HasTriggeredScaleUp(), advertised, "collection must not change advertisement state")
			})
		}
	}
}
