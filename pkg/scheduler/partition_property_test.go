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

package scheduler

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-core/pkg/common/security"
	"github.com/apache/yunikorn-core/pkg/scheduler/objects"
	"github.com/apache/yunikorn-core/pkg/scheduler/ugm"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// This is a randomized property test for the resource accounting of a partition. It drives random
// sequences of the operations a resource manager triggers (node add and remove, application add and
// remove, ask add, scheduling cycles and allocation releases for every termination type) against a
// real PartitionContext and asserts the accounting invariants after every single operation. Nothing
// is re-implemented: the invariants are internal coherence checks over the partition, its queues,
// its applications, its nodes and the user group manager. The only model kept is a running total of
// the allocations that were placed minus the ones that were released.
//
// Placeholder (gang) allocations are deliberately out of scope, they are covered separately.
//
// The sequences are generated from a fixed set of seeds: a seed fixes the operations that are
// generated. The scheduler itself does not break every tie the same way twice, so the outcome of a
// scheduling cycle, and with it the exact state a sequence reaches, can differ between runs of the
// same seed. The failure report therefore carries the tail of the operation history next to the
// seed and the step number.

const (
	propNodeCount   = 3
	propMaxApps     = 8
	propSteps       = 800
	propHistoryTail = 25
	// propNodeMem and propNodeVCore are the capacity of every node of the pool. A node is small
	// compared to the queue maximums on purpose: an ask that fits in the queue but on none of the
	// nodes is the case that reserves a node.
	propNodeMem   = 4
	propNodeVCore = 4000
	// propMaxAskSize is the largest ask generated, in the same units as the node capacity: an ask
	// of a full node only fits on an empty one and is the ask that ends up reserving a node
	propMaxAskSize = 4
	// propRequiredNodeChance is the one in N chance that a generated request must run on a specific
	// node, the request type a daemon set pod maps to
	propRequiredNodeChance = 3
	// propSeedEnv holds an extra seed to run, for exploratory runs only: the fixed seeds always run
	propSeedEnv = "YUNIKORN_PARTITION_FUZZ_SEED"
)

// propSeeds are the seeds used for every run: sequences must be stable over time and over -count
var propSeeds = []int64{1, 7, 42, 1234, 20260817}

// the operations that are generated, see applyOp for their meaning
const (
	opAddNode = iota
	opRemoveNode
	opAddApp
	opRemoveApp
	opAddAsk
	opTryAllocate
	opTryReservedAllocate
	opRelease
)

// weights of the generated operations: the scheduling cycle and the ask additions are weighted up
// to keep the partition busy, the removals are frequent enough to churn the state
var propOpWeights = []struct {
	op     int
	weight int
}{
	{opAddNode, 6},
	{opRemoveNode, 6},
	{opAddApp, 8},
	{opRemoveApp, 4},
	{opAddAsk, 22},
	{opTryAllocate, 30},
	{opTryReservedAllocate, 10},
	{opRelease, 14},
}

// the leaf queues the applications are placed in: leaf-a and leaf-b have a maximum set, leaf-c does
// not, and leaf-b and leaf-c sit below an extra parent to get a deeper hierarchy to check
var propQueues = []string{"root.leaf-a", "root.parent.leaf-b", "root.parent.leaf-c"}

// the users the applications are submitted by, each with its own group: the tracked usage of a user
// and of a group is checked against the applications that user owns
var propUsers = []security.UserGroup{
	{User: "fuzz-user-1", Groups: []string{"fuzz-group-1"}},
	{User: "fuzz-user-2", Groups: []string{"fuzz-group-2"}},
}

// the termination types a release is generated for. PLACEHOLDER_REPLACED is not part of the list:
// it is only ever sent for a placeholder that has a real allocation linked to it and placeholders
// are out of scope for this fuzzer.
var propTerminationTypes = []si.TerminationType{
	si.TerminationType_UNKNOWN_TERMINATION_TYPE,
	si.TerminationType_STOPPED_BY_RM,
	si.TerminationType_TIMEOUT,
	si.TerminationType_PREEMPTED_BY_SCHEDULER,
	si.TerminationType_SCHEDULING_FAILED_ON_RM,
}

// newPropertyPartition creates the partition the fuzzer runs against:
// root -> leaf-a
//
//	-> parent -> leaf-b
//	          -> leaf-c
//
// leaf-a and leaf-b have a maximum resource set, the root has a user and group limit set.
func newPropertyPartition() (*PartitionContext, error) {
	conf := configs.PartitionConfig{
		Name: "test",
		Queues: []configs.QueueConfig{
			{
				Name:      "root",
				Parent:    true,
				SubmitACL: "*",
				Queues: []configs.QueueConfig{
					{
						Name:   "leaf-a",
						Parent: false,
						Resources: configs.Resources{
							Max: map[string]string{
								"memory": "10",
								"vcore":  "10",
							},
						},
					},
					{
						Name:   "parent",
						Parent: true,
						Queues: []configs.QueueConfig{
							{
								Name:   "leaf-b",
								Parent: false,
								Resources: configs.Resources{
									Max: map[string]string{
										"memory": "6",
										"vcore":  "6",
									},
								},
							},
							{
								Name:   "leaf-c",
								Parent: false,
								Queues: nil,
							},
						},
					},
				},
				Limits: []configs.Limit{
					{
						Limit:  "property fuzz limit",
						Users:  []string{propUsers[0].User, propUsers[1].User},
						Groups: []string{propUsers[0].Groups[0], propUsers[1].Groups[0]},
						MaxResources: map[string]string{
							"memory": "12",
							"vcore":  "12",
						},
						MaxApplications: 10,
					},
				},
			},
		},
		PlacementRules: nil,
		Limits:         nil,
		NodeSortPolicy: configs.NodeSortingPolicy{},
	}
	return newPartitionContext(conf, rmID, nil, false)
}

// propAlloc is the model entry of a single allocation that was placed by the scheduler
type propAlloc struct {
	appID string
	res   *resources.Resource
}

type partitionFuzzer struct {
	t         *testing.T
	seed      int64
	rng       *rand.Rand
	partition *PartitionContext
	nodes     map[string]bool       // the nodes currently registered on the partition
	apps      map[string]bool       // the applications currently added to the partition
	allocated map[string]*propAlloc // the allocations the fuzzer knows are placed
	stats     map[string]int        // what the sequence covered, logged at the end of a run
	appSeq    int
	askSeq    int
	history   []string
	step      int
}

func newPartitionFuzzer(t *testing.T, seed int64) *partitionFuzzer {
	t.Helper()
	setupUGM()
	partition, err := newPropertyPartition()
	assert.NilError(t, err, "test partition create failed with error")
	t.Cleanup(partition.userGroupCache.Stop)
	// an ask is only reserved once it has been around for the reservation delay: without this the
	// reservation part of the allocation flow is never reached in a test
	objects.SetReservationDelay(10 * time.Nanosecond)
	t.Cleanup(func() { objects.SetReservationDelay(2 * time.Second) })
	return &partitionFuzzer{
		t:         t,
		seed:      seed,
		rng:       rand.New(rand.NewSource(seed)), //nolint:gosec
		partition: partition,
		nodes:     make(map[string]bool),
		apps:      make(map[string]bool),
		allocated: make(map[string]*propAlloc),
		stats:     make(map[string]int),
	}
}

// count tracks what the generated sequence covered: a sequence which stops allocating or never
// removes a node with live allocations on it is not testing what it is meant to test
func (f *partitionFuzzer) count(name string) {
	f.stats[name]++
}

func propNodeID(idx int) string {
	return fmt.Sprintf("fuzz-node-%d", idx)
}

// askResource returns an ask sized between 1 and propMaxAskSize node units
func (f *partitionFuzzer) askResource() *resources.Resource {
	size := int64(f.rng.Intn(propMaxAskSize) + 1)
	return resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": resources.Quantity(size),
		"vcore":  resources.Quantity(size * 1000),
	})
}

func (f *partitionFuzzer) record(format string, args ...interface{}) {
	f.history = append(f.history, fmt.Sprintf("step %d: %s", f.step, fmt.Sprintf(format, args...)))
}

// pickOp returns the next operation to run based on the configured weights
func (f *partitionFuzzer) pickOp() int {
	total := 0
	for _, entry := range propOpWeights {
		total += entry.weight
	}
	pick := f.rng.Intn(total)
	for _, entry := range propOpWeights {
		if pick < entry.weight {
			return entry.op
		}
		pick -= entry.weight
	}
	return opTryAllocate
}

// pick returns a random entry of the passed in list, the list must be sorted to keep the generated
// sequence stable for a seed
func (f *partitionFuzzer) pick(list []string) string {
	return list[f.rng.Intn(len(list))]
}

func (f *partitionFuzzer) applyOp(op int) bool {
	switch op {
	case opAddNode:
		return f.addNode()
	case opRemoveNode:
		return f.removeNode()
	case opAddApp:
		return f.addApp()
	case opRemoveApp:
		return f.removeApp()
	case opAddAsk:
		return f.addAsk()
	case opTryAllocate:
		return f.tryAllocate()
	case opTryReservedAllocate:
		return f.tryReservedAllocate()
	case opRelease:
		return f.release()
	}
	return false
}

// addNode registers a node of the pool that is not registered yet. A new node object is used for
// every registration: a node that was removed is never handed back to the partition.
func (f *partitionFuzzer) addNode() bool {
	nodeID := propNodeID(f.rng.Intn(propNodeCount))
	if f.nodes[nodeID] {
		return false
	}
	capacity := resources.NewResourceFromMap(map[string]resources.Quantity{
		"memory": propNodeMem,
		"vcore":  propNodeVCore,
	})
	err := f.partition.AddNode(newNodeMaxResource(nodeID, capacity))
	assert.NilError(f.t, err, "node %s add failed unexpected", nodeID)
	f.nodes[nodeID] = true
	f.record("AddNode(%s)", nodeID)
	return true
}

// removeNode removes a registered node. The node can still hold allocations: all of them are
// released as part of the removal.
func (f *partitionFuzzer) removeNode() bool {
	if len(f.nodes) == 0 {
		return false
	}
	nodeID := f.pick(propSortedKeys(f.nodes))
	released, confirmed := f.partition.removeNode(nodeID)
	delete(f.nodes, nodeID)
	for _, alloc := range released {
		delete(f.allocated, alloc.GetAllocationKey())
	}
	f.count("node removed")
	if len(released) > 0 {
		f.count("node removed with allocations")
	}
	f.record("removeNode(%s) released=%d confirmed=%d", nodeID, len(released), len(confirmed))
	return true
}

// addApp adds a new application to the partition. Every application gets a new ID: an application
// that was removed is never added back.
func (f *partitionFuzzer) addApp() bool {
	if len(f.apps) >= propMaxApps {
		return false
	}
	f.appSeq++
	appID := fmt.Sprintf("fuzz-app-%d", f.appSeq)
	queueName := propQueues[f.appSeq%len(propQueues)]
	user := propUsers[f.appSeq%len(propUsers)]
	err := f.partition.AddApplication(newApplicationWithUser(appID, f.partition.Name, queueName, user))
	assert.NilError(f.t, err, "application %s add failed unexpected", appID)
	f.apps[appID] = true
	f.record("AddApplication(%s, queue=%s, user=%s)", appID, queueName, user.User)
	return true
}

// removeApp removes an application from the partition, all its asks, reservations and allocations
// are removed with it
func (f *partitionFuzzer) removeApp() bool {
	if len(f.apps) == 0 {
		return false
	}
	appID := f.pick(propSortedKeys(f.apps))
	released := f.partition.removeApplication(appID)
	delete(f.apps, appID)
	for _, alloc := range released {
		delete(f.allocated, alloc.GetAllocationKey())
	}
	f.count("application removed")
	if len(released) > 0 {
		f.count("application removed with allocations")
	}
	f.record("removeApplication(%s) released=%d", appID, len(released))
	return true
}

// addAsk adds a new request for an application via the resource manager entry point: an allocation
// without a node is a request
func (f *partitionFuzzer) addAsk() bool {
	if len(f.apps) == 0 {
		return false
	}
	appID := f.pick(propSortedKeys(f.apps))
	f.askSeq++
	askKey := fmt.Sprintf("%s-ask-%d", appID, f.askSeq)
	res := f.askResource()
	ask := newAllocationAsk(askKey, appID, res)
	// a request that must run on a specific node reserves that node as soon as it does not fit on
	// it: it is the request type that drives the reservation flow without relying on the cluster
	// filling up first
	requiredNode := ""
	if len(f.nodes) > 0 && f.rng.Intn(propRequiredNodeChance) == 0 {
		requiredNode = f.pick(propSortedKeys(f.nodes))
		ask.SetRequiredNode(requiredNode)
	}
	requestCreated, _, err := f.partition.UpdateAllocation(ask)
	// the core can reject a request, for instance when the application is not in a state that
	// accepts new work: that is a legal outcome and just leaves the partition unchanged
	f.record("UpdateAllocation(%s, app=%s, res=%s, requiredNode=%q) created=%t err=%v", askKey, appID, res, requiredNode, requestCreated, err)
	return true
}

// tryAllocate runs a regular scheduling cycle and tracks the allocation it placed
func (f *partitionFuzzer) tryAllocate() bool {
	result := f.partition.tryAllocate()
	f.trackResult("tryAllocate", result)
	return true
}

// tryReservedAllocate runs a scheduling cycle over the reservations and tracks the allocation it
// placed. This is the path that cancels a reservation by satisfying it.
func (f *partitionFuzzer) tryReservedAllocate() bool {
	result := f.partition.tryReservedAllocate()
	f.trackResult("tryReservedAllocate", result)
	return true
}

// trackResult adds the allocation of a scheduling cycle to the model. A cycle that only made or
// cancelled a reservation does not return a result.
func (f *partitionFuzzer) trackResult(name string, result *objects.AllocationResult) {
	if result == nil || result.Request == nil {
		f.record("%s() no allocation", name)
		return
	}
	alloc := result.Request
	f.allocated[alloc.GetAllocationKey()] = &propAlloc{
		appID: alloc.GetApplicationID(),
		res:   alloc.GetAllocatedResource().Clone(),
	}
	f.count(name + " placed an allocation")
	f.record("%s() allocated %s for app %s on node %s", name, alloc.GetAllocationKey(), alloc.GetApplicationID(), result.NodeID)
}

// release releases a placed allocation with a random termination type
func (f *partitionFuzzer) release() bool {
	if len(f.allocated) == 0 {
		return false
	}
	allocKey := f.pick(propSortedKeys(f.allocated))
	entry := f.allocated[allocKey]
	app := f.partition.GetApplication(entry.appID)
	if app == nil {
		return false
	}
	termination := propTerminationTypes[f.rng.Intn(len(propTerminationTypes))]
	// a rollback is rejected unless the application is in a state that can re-schedule the ask:
	// the allocation is left in place in that case
	rollback := termination == si.TerminationType_SCHEDULING_FAILED_ON_RM
	removed := !rollback || app.IsAccepted() || app.IsRunning()
	released, confirmed := f.partition.removeAllocation(&si.AllocationRelease{
		PartitionName:   f.partition.Name,
		ApplicationID:   entry.appID,
		AllocationKey:   allocKey,
		TerminationType: termination,
	})
	if removed {
		delete(f.allocated, allocKey)
	}
	f.count("released " + termination.String())
	f.record("removeAllocation(%s, app=%s, type=%s) released=%d confirmed=%t", allocKey, entry.appID, termination, len(released), confirmed != nil)
	return true
}

// run generates and runs the operation sequence, checking all invariants after every operation
func (f *partitionFuzzer) run(steps int) {
	for f.step = 1; f.step <= steps; f.step++ {
		if !f.applyOp(f.pickOp()) {
			// the preconditions of the operation are not met, nothing was run
			continue
		}
		f.check()
	}
	for _, name := range propSortedKeys(f.stats) {
		f.t.Logf("seed %d covered: %s: %d", f.seed, name, f.stats[name])
	}
}

// check runs all invariants and fails the test with a replayable report if any of them is violated
func (f *partitionFuzzer) check() {
	violations := make([]string, 0)
	violations = append(violations, f.checkQueue(f.partition.GetQueue(configs.RootQueue))...)
	violations = append(violations, f.checkUserGroupUsage()...)
	violations = append(violations, f.checkNodes()...)
	violations = append(violations, f.checkReservations()...)
	violations = append(violations, f.checkModel()...)
	if len(violations) == 0 {
		return
	}

	history := f.history
	if len(history) > propHistoryTail {
		history = history[len(history)-propHistoryTail:]
	}
	f.t.Fatalf("partition accounting invariant violated\nseed: %d\nstep: %d\nviolations:\n  %s\nlast %d operations:\n  %s",
		f.seed, f.step, strings.Join(violations, "\n  "), len(history), strings.Join(history, "\n  "))
}

// checkQueue walks the queue hierarchy from the root down and verifies:
// Q1 the usage of a parent queue is the sum of the usage of its children
// Q2 the usage of a leaf queue is the sum of the usage of the applications it runs
// Q3 the usage of a queue is never negative and never over the maximum when one is set
// Q6 the pending resource of a queue is the sum of the pending resources below it
func (f *partitionFuzzer) checkQueue(queue *objects.Queue) []string {
	violations := make([]string, 0)
	path := queue.GetQueuePath()
	allocated := queue.GetAllocatedResource()
	pending := queue.GetPendingResource()

	if allocated.HasNegativeValue() {
		violations = append(violations, fmt.Sprintf("Q3 usage: queue %s has a negative usage %s", path, allocated))
	}
	if pending.HasNegativeValue() {
		violations = append(violations, fmt.Sprintf("Q3 usage: queue %s has a negative pending resource %s", path, pending))
	}
	// the maximum of a queue includes the limits of its parents, the root maximum is the size of
	// the cluster. Mirror the check the queue makes itself when it increases its usage.
	if maxRes := queue.GetMaxResource(); !maxRes.IsEmpty() {
		fits := maxRes.FitInMaxUndef(allocated)
		if path == configs.RootQueue {
			fits = maxRes.FitIn(allocated)
		}
		if !fits {
			violations = append(violations, fmt.Sprintf("Q3 usage: queue %s usage %s is over its maximum %s", path, allocated, maxRes))
		}
	}

	childAllocated := resources.NewResource()
	childPending := resources.NewResource()
	if queue.IsLeafQueue() {
		apps := queue.GetCopyOfApps()
		for _, appID := range propSortedKeys(apps) {
			app := apps[appID]
			childAllocated.AddTo(resources.Add(app.GetAllocatedResource(), app.GetPlaceholderResource()))
			childPending.AddTo(app.GetPendingResource())
		}
		if !resources.EqualsOrEmpty(allocated, childAllocated) {
			violations = append(violations, fmt.Sprintf("Q2 leaf usage: queue %s usage %s is not the sum of its %d applications %s",
				path, allocated, len(apps), childAllocated))
		}
	} else {
		children := queue.GetCopyOfChildren()
		for _, name := range propSortedKeys(children) {
			child := children[name]
			childAllocated.AddTo(child.GetAllocatedResource())
			childPending.AddTo(child.GetPendingResource())
			violations = append(violations, f.checkQueue(child)...)
		}
		if !resources.EqualsOrEmpty(allocated, childAllocated) {
			violations = append(violations, fmt.Sprintf("Q1 hierarchy: queue %s usage %s is not the sum of its %d children %s",
				path, allocated, len(children), childAllocated))
		}
	}
	if !resources.EqualsOrEmpty(pending, childPending) {
		violations = append(violations, fmt.Sprintf("Q6 pending: queue %s pending %s is not the sum below it %s",
			path, pending, childPending))
	}
	return violations
}

// checkUserGroupUsage verifies Q4: the usage the user group manager tracks for a user, and for the
// group of that user, is the sum of the usage of the applications that user owns
func (f *partitionFuzzer) checkUserGroupUsage() []string {
	violations := make([]string, 0)
	manager := ugm.GetUserManager()
	expected := make(map[string]*resources.Resource)
	for _, app := range f.partition.GetApplications() {
		user := app.GetUser().User
		expected[user] = resources.Add(expected[user], resources.Add(app.GetAllocatedResource(), app.GetPlaceholderResource()))
	}
	for _, user := range propUsers {
		want := expected[user.User]
		if got := manager.GetUserResources(user.User); !resources.EqualsOrEmpty(got, want) {
			violations = append(violations, fmt.Sprintf("Q4 user tracker: user %s tracks %s, applications hold %s", user.User, got, want))
		}
		group := user.Groups[0]
		if got := manager.GetGroupResources(group); !resources.EqualsOrEmpty(got, want) {
			violations = append(violations, fmt.Sprintf("Q4 group tracker: group %s tracks %s, applications hold %s", group, got, want))
		}
	}
	return violations
}

// checkNodes verifies Q5: the usage of a node is the sum of the allocations placed on it
func (f *partitionFuzzer) checkNodes() []string {
	violations := make([]string, 0)
	for _, nodeID := range propSortedKeys(f.nodes) {
		node := f.partition.GetNode(nodeID)
		if node == nil {
			violations = append(violations, fmt.Sprintf("Q5 node usage: node %s is not registered on the partition", nodeID))
			continue
		}
		total := resources.NewResource()
		for _, alloc := range node.GetYunikornAllocations() {
			total.AddTo(alloc.GetAllocatedResource())
		}
		for _, alloc := range node.GetForeignAllocations() {
			total.AddTo(alloc.GetAllocatedResource())
		}
		if !resources.EqualsOrEmpty(node.GetAllocatedResource(), total) {
			violations = append(violations, fmt.Sprintf("Q5 node usage: node %s usage %s is not the sum of its allocations %s",
				nodeID, node.GetAllocatedResource(), total))
		}
	}
	return violations
}

// checkReservations verifies R1: every reservation references an application, an ask and a node
// that are all still live on the partition. The check runs from both ends: a reservation held by a
// node must point at a live application and ask, and a reservation held by an application must
// point at a node that is still registered and that still holds the reservation.
func (f *partitionFuzzer) checkReservations() []string {
	violations := make([]string, 0)
	reserved := 0
	for _, nodeID := range propSortedKeys(f.nodes) {
		node := f.partition.GetNode(nodeID)
		if node == nil {
			continue
		}
		// the reservations of a node are not returned in a stable order, sort the violations of the
		// node to keep the failure report of a replay identical
		nodeViolations := make([]string, 0)
		for _, reservation := range node.GetReservations() {
			reserved++
			reservedNode, app, ask := reservation.GetObjects()
			if app == nil || ask == nil || reservedNode == nil {
				nodeViolations = append(nodeViolations, fmt.Sprintf("R1 reservation: node %s has an incomplete reservation", nodeID))
				continue
			}
			key := ask.GetAllocationKey()
			if f.partition.GetApplication(app.ApplicationID) != app {
				nodeViolations = append(nodeViolations, fmt.Sprintf("R1 reservation: node %s reservation %s references application %s that is not on the partition",
					nodeID, key, app.ApplicationID))
			}
			if app.GetAllocationAsk(key) != ask {
				nodeViolations = append(nodeViolations, fmt.Sprintf("R1 reservation: node %s reservation %s references an ask that application %s does not have",
					nodeID, key, app.ApplicationID))
			}
			if reservedNode != node || f.partition.GetNode(reservedNode.NodeID) != node {
				nodeViolations = append(nodeViolations, fmt.Sprintf("R1 reservation: node %s reservation %s references node %s",
					nodeID, key, reservedNode.NodeID))
			}
			if reserved := app.NodeReservedForAsk(key); reserved != nodeID {
				nodeViolations = append(nodeViolations, fmt.Sprintf("R1 reservation: node %s reservation %s is reserved on %q by application %s",
					nodeID, key, reserved, app.ApplicationID))
			}
		}
		sort.Strings(nodeViolations)
		violations = append(violations, nodeViolations...)
	}
	violations = append(violations, f.checkApplicationReservations()...)
	if reserved > 0 {
		f.count("steps with a reservation")
	}
	if reserved > f.stats["peak reservations"] {
		f.stats["peak reservations"] = reserved
	}
	return violations
}

// checkApplicationReservations is the application side of R1: a reservation an application holds
// must be for an ask it still has and for a node that is still registered and still reserved
func (f *partitionFuzzer) checkApplicationReservations() []string {
	violations := make([]string, 0)
	for _, appID := range propSortedKeys(f.apps) {
		app := f.partition.GetApplication(appID)
		if app == nil {
			continue
		}
		keys := app.GetReservations()
		sort.Strings(keys)
		for _, key := range keys {
			if app.GetAllocationAsk(key) == nil {
				violations = append(violations, fmt.Sprintf("R1 reservation: application %s has a reservation for ask %s it does not have", appID, key))
			}
			nodeID := app.NodeReservedForAsk(key)
			node := f.partition.GetNode(nodeID)
			if node == nil {
				violations = append(violations, fmt.Sprintf("R1 reservation: application %s reservation %s is on node %q that is not on the partition", appID, key, nodeID))
				continue
			}
			if !propContains(node.GetReservationKeys(), key) {
				violations = append(violations, fmt.Sprintf("R1 reservation: application %s reservation %s is not held by node %s", appID, key, nodeID))
			}
		}
	}
	return violations
}

// propContains returns true if the list holds the value
func propContains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// checkModel cross-checks the partition totals against the running total of the allocations that
// were placed minus the ones that were released
func (f *partitionFuzzer) checkModel() []string {
	violations := make([]string, 0)
	total := resources.NewResource()
	for _, key := range propSortedKeys(f.allocated) {
		total.AddTo(f.allocated[key].res)
	}
	if allocated := f.partition.GetAllocatedResource(); !resources.EqualsOrEmpty(allocated, total) {
		violations = append(violations, fmt.Sprintf("M1 model: partition usage %s is not the placed minus released total %s", allocated, total))
	}
	if count := f.partition.GetTotalAllocationCount(); count != len(f.allocated) {
		violations = append(violations, fmt.Sprintf("M1 model: partition allocation count %d is not the placed minus released count %d", count, len(f.allocated)))
	}
	return violations
}

// propSortedKeys returns the keys of the given map in a stable order: the generated sequence and
// the failure report of a replay must be identical
func propSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func TestPartitionProperty(t *testing.T) {
	for _, seed := range propSeedList(t) {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			newPartitionFuzzer(t, seed).run(propSteps)
		})
	}
}

// propSeedList returns the seeds to run: the fixed list, plus the seed of the seed environment
// variable when set for an exploratory run
func propSeedList(t *testing.T) []int64 {
	seeds := make([]int64, len(propSeeds))
	copy(seeds, propSeeds)
	value, ok := os.LookupEnv(propSeedEnv)
	if !ok {
		return seeds
	}
	seed, err := strconv.ParseInt(value, 10, 64)
	assert.NilError(t, err, "invalid %s value %q", propSeedEnv, value)
	return append(seeds, seed)
}
