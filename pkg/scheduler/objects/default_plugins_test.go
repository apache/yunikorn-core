package objects

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/plugins/defaults"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"

	"gotest.tools/assert"
)

const queueName = "leaf"

func TestSortAppsNoPending(t *testing.T) {
	// init queue
	leafQueue := createTestQueue(t, queueName)
	// init apps
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	for i := 0; i < 2; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", queueName)
		leafQueue.AddApplication(app)
		app.SetQueue(leafQueue)
	}

	// no apps with pending resources should come back empty
	leafQueue.sortType = policies.FairSortPolicy
	list := getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{}, "fair no pending")
	leafQueue.sortType = policies.FifoSortPolicy
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{}, "fifo no pending")
	leafQueue.sortType = policies.StateAwarePolicy
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{}, "state no pending")

	// set one app with pending
	appID := "app-1"
	ask := newAllocationAsk("ask-1", appID, res)
	leafQueue.applications.GetApplication(appID).(*Application).AddAllocationAsk(ask)
	leafQueue.sortType = policies.FairSortPolicy
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{appID}, "fair one pending")
	leafQueue.sortType = policies.FifoSortPolicy
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{appID}, "fifo one pending")
	leafQueue.sortType = policies.StateAwarePolicy
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{appID}, "state one pending")
}

func TestSortAppsFifo(t *testing.T) {
	// init queue
	leafQueue := createTestQueue(t, queueName)
	// init apps
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	input := make(map[string]interfaces.Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", queueName)
		input[appID] = app
		leafQueue.AddApplication(app)
		app.SetQueue(leafQueue)
		// add pending ask
		ask := newAllocationAsk("ask-1", appID, res)
		app.AddAllocationAsk(ask)
	}
	// map iteration is random so we do not need to check input
	// apps should come back in order created 0, 1, 2, 3
	leafQueue.sortType = policies.FifoSortPolicy
	list := getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{0, 1, 2, 3}, "fifo simple")
}

func TestSortAppsFair(t *testing.T) {
	// init queue
	leafQueue := createTestQueue(t, queueName)
	// init apps
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", queueName)
		leafQueue.AddApplication(app)
		app.SetQueue(leafQueue)
		// add pending ask
		ask := newAllocationAsk("ask-1", appID, res)
		app.AddAllocationAsk(ask)
		// add allocation
		alloc := newAllocation(appID, "uuid-1", nodeID1, queueName, resources.Multiply(res, int64(i+1)))
		app.AddAllocation(alloc)
	}

	leafQueue.sortType = policies.FairSortPolicy
	// nil resource: usage based sorting
	// apps should come back in order: 0, 1, 2, 3
	list := getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{0, 1, 2, 3}, "nil total")

	// apps should come back in order: 0, 1, 2, 3
	leafQueue.guaranteedResource = resources.Multiply(res, 0)
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{0, 1, 2, 3}, "zero total")

	// apps should come back in order: 0, 1, 2, 3
	leafQueue.guaranteedResource = resources.Multiply(res, 5)
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{0, 1, 2, 3}, "no alloc, set total")

	// update allocated resource for app-1
	leafQueue.applications.GetApplication("app-1").(*Application).allocatedResource = resources.Multiply(res, 10)
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	// apps should come back in order: 0, 2, 3, 1
	assertAppList(t, list, []int{0, 3, 1, 2}, "app-1 allocated")

	// update allocated resource for app-3 to negative (move to head of the list)
	leafQueue.applications.GetApplication("app-3").(*Application).allocatedResource = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{1, 3, 2, 0}, "app-1 & app-3 allocated")
}

func TestSortAppsStateAware(t *testing.T) {
	// init queue
	leafQueue := createTestQueue(t, queueName)
	// init apps
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup all apps with pending resources, all accepted state
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", queueName)
		leafQueue.AddApplication(app)
		app.SetQueue(leafQueue)
		// add pending ask
		ask := newAllocationAsk("ask-1", appID, res)
		app.AddAllocationAsk(ask)
		time.Sleep(time.Nanosecond * 5)
	}
	// only first app should be returned (all in accepted)
	leafQueue.sortType = policies.StateAwarePolicy
	list := getApps(leafQueue.GetApplications().SortForAllocation())
	appID0 := "app-0"
	assertAppListLength(t, list, []string{appID0}, "state all accepted")

	// set first app pending to zero, should get 2nd app back
	leafQueue.applications.GetApplication(appID0).(*Application).pending = resources.NewResource()
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	appID1 := "app-1"
	assertAppListLength(t, list, []string{appID1}, "state no pending")

	// move the first app to starting no pending resource should get nothing
	err := leafQueue.applications.GetApplication(appID0).(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-0")
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{}, "state starting no pending")

	// move first app to running (no pending resource) and 4th app to starting should get starting app
	err = leafQueue.applications.GetApplication(appID0).(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-0")
	appID3 := "app-3"
	err = leafQueue.applications.GetApplication(appID3).(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{appID3}, "state starting")

	// set pending for first app, should get back 1st and 4th in that order
	leafQueue.applications.GetApplication(appID0).(*Application).pending = res
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{appID0, appID3}, "state first pending")

	// move 4th to running should get back: 1st, 2nd and 4th in that order
	err = leafQueue.applications.GetApplication(appID3).(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppListLength(t, list, []string{appID0, appID1, appID3}, "state not app-2")
}

func TestSortAsks(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(1)})
	list := make([]interfaces.Request, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		ask := newAllocationAsk("ask-"+num, "app-1", res)
		ask.priority = int32(i)
		list[i] = ask
	}
	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAskList(t, list, []int{2, 3, 0, 1}, "moved 1")
	defaults.SortAskByPriority(list, true)
	// asks should come back in order: 0, 1, 2, 3
	assertAskList(t, list, []int{0, 1, 2, 3}, "ascending")
	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAskList(t, list, []int{2, 3, 0, 1}, "moved 2")
	defaults.SortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 1, 0
	assertAskList(t, list, []int{3, 2, 1, 0}, "descending")
	// make asks with same priority
	// ask-3 and ask-1 both with prio 1 do not change order
	// ask-3 must always be earlier in the list
	list[0].(*AllocationAsk).priority = 1
	defaults.SortAskByPriority(list, true)
	// asks should come back in order: 0, 2, 3, 1
	assertAskList(t, list, []int{0, 1, 3, 2}, "ascending same prio")
	defaults.SortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 0, 1
	assertAskList(t, list, []int{3, 1, 0, 2}, "descending same prio")
}

func BenchmarkIteratePendingRequests(b *testing.B) {
	tests := []struct {
		numPriorities          int
		numRequestsPerPriority int
		pendingPercentage      float32
	}{
		// expect the performance is only related to the number of pending requests,
		// won't be affected by other factors such as the number of priorities and pending percentage.

		// cases: 10,000 pending requests in total with different number of priorities
		{numPriorities: 1, numRequestsPerPriority: 10000, pendingPercentage: 1.0},
		{numPriorities: 2, numRequestsPerPriority: 5000, pendingPercentage: 1.0},
		{numPriorities: 5, numRequestsPerPriority: 2000, pendingPercentage: 1.0},
		{numPriorities: 10, numRequestsPerPriority: 1000, pendingPercentage: 1.0},

		// cases: 10,000 pending requests in total with only 1 priority and different pending percentage
		{numPriorities: 1, numRequestsPerPriority: 10000, pendingPercentage: 1.0},
		{numPriorities: 1, numRequestsPerPriority: 10000, pendingPercentage: 0.5},
		{numPriorities: 1, numRequestsPerPriority: 10000, pendingPercentage: 0.2},
		{numPriorities: 1, numRequestsPerPriority: 10000, pendingPercentage: 0.1},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%dPriorities/%dRequests/%fPendingPercentage",
			test.numPriorities, test.numRequestsPerPriority, test.pendingPercentage)
		b.Run(name, func(b *testing.B) {
			benchmarkIteratePendingRequests(b, test.numPriorities, test.numRequestsPerPriority,
				test.pendingPercentage)
		})
	}
}

func benchmarkIteratePendingRequests(b *testing.B, numPriorities, numRequestsPerPriority int, pendingPercentage float32) {
	// create the root and left queue
	root, err := createRootQueue(nil)
	assert.NilError(b, err, "queue create failed")
	var leaf *Queue
	leaf, err = createManagedQueue(root, queueName, true, nil)
	assert.NilError(b, err, "failed to create leaf queue: %v", err)
	// create an application
	app := newApplication(appID1, "default", queueName)
	app.queue = leaf
	leaf.AddApplication(app)
	// prepare asks for testing
	res := resources.NewResourceFromMap(map[string]resources.Quantity{"first": 1})
	pendingFlag := int(pendingPercentage * 100)
	expectedPendingCount := 0
	askIndex := 0
	for i := 0; i < numPriorities; i++ {
		for j := 0; j < numRequestsPerPriority; j++ {
			allocKey := "ask-" + strconv.Itoa(askIndex)
			ask := newAllocationAskRepeat(allocKey, appID1, res, 1)
			ask.priority = int32(i)
			err := app.AddAllocationAsk(ask)
			assert.NilError(b, err, "failed to add allocation ask", err)
			if rand.Intn(100) < pendingFlag {
				expectedPendingCount++
			} else {
				// set 0 repeat
				_, err = app.updateAskRepeat(allocKey, -1)
				if err != nil {
					b.Errorf("failed to update ask repeat: %v (err = %v)", app, err)
				}
			}
			askIndex++
		}
	}

	// check pending count
	pendingCount := 0
	reqIt := app.requests.SortForAllocation()
	for reqIt.HasNext() {
		reqIt.Next()
		pendingCount++
	}
	if pendingCount != expectedPendingCount {
		b.Fatalf("expected pending count: %v, but actually got: %v", expectedPendingCount, pendingCount)
	}

	// Reset timer for this benchmark
	startTime := time.Now()
	b.ResetTimer()

	// execute get all pending for 1000 times
	testTimes := 100
	for i := 0; i < testTimes; i++ {
		app.requests.SortForAllocation()
		//for reqIt.HasNext() {
		//	reqIt.Next()
		//}
	}

	// Stop timer and calculate duration
	b.StopTimer()
	duration := time.Since(startTime)
	b.Logf("Total time to iterate %d pending requests from %d request for %d times in %s, avg time: %s",
		expectedPendingCount, numPriorities*numRequestsPerPriority, testTimes, duration, duration/100)
}

func BenchmarkSortApplications(b *testing.B) {
	tests := []struct {
		numApplications int
		policyType      policies.SortPolicy
	}{
		{numApplications: 10, policyType: policies.FairSortPolicy},
		{numApplications: 10, policyType: policies.FifoSortPolicy},
		{numApplications: 100, policyType: policies.FairSortPolicy},
		{numApplications: 100, policyType: policies.FifoSortPolicy},
		{numApplications: 1000, policyType: policies.FairSortPolicy},
		{numApplications: 1000, policyType: policies.FifoSortPolicy},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%vApps/%s", test.numApplications, test.policyType)
		b.Run(name, func(b *testing.B) {
			benchmarkSortApplications(b, test.numApplications, test.policyType)
		})
	}
}

func benchmarkSortApplications(b *testing.B, numApplications int, sortPolicy policies.SortPolicy) {
	// create the root
	root, err := createRootQueue(nil)
	if err != nil {
		b.Fatalf("failed to create basic root queue: %v", err)
	}
	var leaf, parent *Queue
	// empty parent queue
	parent, err = createManagedQueue(root, "parent", true, nil)
	if err != nil {
		b.Fatalf("failed to create parent queue: %v", err)
	}
	// empty leaf queue
	leaf, err = createManagedQueue(parent, "leaf", false, nil)
	leaf.sortType = sortPolicy
	//leaf.appRequestSorter = newAppRequestSorter(sortPolicy)
	if err != nil {
		b.Fatalf("failed to create leaf queue: %v", err)
	}
	// add apps with a pending ask
	for i := 0; i < numApplications; i++ {
		appID := fmt.Sprintf("app-%d", i)
		app := newApplication(appID, "default", leaf.Name)
		app.queue = leaf
		leaf.AddApplication(app)
		res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
		if err != nil {
			b.Fatalf("failed to create basic resource: %v", err)
		}
		// add an ask app must be returned
		err = app.AddAllocationAsk(newAllocationAsk("alloc-1", appID, res))
		if err != nil {
			b.Errorf("failed to add allocation ask (err = %v)", err)
		}
	}

	// Reset  timer for this benchmark
	startTime := time.Now()
	b.ResetTimer()

	// execute sorting for 1000 times
	testTimes := 100
	for i := 0; i < testTimes; i++ {
		leaf.applications.SortForAllocation()
	}

	// Stop timer and calculate duration
	b.StopTimer()
	duration := time.Since(startTime)
	b.Logf("Total time to sort %d apps for %d times in %s, avg time: %s",
		numApplications, testTimes, duration, duration/100)
}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAppList(t *testing.T, list []interfaces.Application, place []int, name string) {
	assert.Equal(t, "app-0", list[place[0]].GetApplicationID(), "test name: %s", name)
	assert.Equal(t, "app-1", list[place[1]].GetApplicationID(), "test name: %s", name)
	assert.Equal(t, "app-2", list[place[2]].GetApplicationID(), "test name: %s", name)
	assert.Equal(t, "app-3", list[place[3]].GetApplicationID(), "test name: %s", name)
}

// list of application and the location of the named applications inside that list
// place[0] defines the location of the app-0 in the list of applications
func assertAskList(t *testing.T, list []interfaces.Request, place []int, name string) {
	assert.Equal(t, "ask-0", list[place[0]].GetAllocationKey(), "test name: %s", name)
	assert.Equal(t, "ask-1", list[place[1]].GetAllocationKey(), "test name: %s", name)
	assert.Equal(t, "ask-2", list[place[2]].GetAllocationKey(), "test name: %s", name)
	assert.Equal(t, "ask-3", list[place[3]].GetAllocationKey(), "test name: %s", name)
}

func assertAppListLength(t *testing.T, list []interfaces.Application, apps []string, name string) {
	assert.Equal(t, len(apps), len(list), "length of list differs, test: %s", name)
	for i, app := range list {
		assert.Equal(t, apps[i], app.GetApplicationID(), "test name: %s", name)
	}
}

func createTestQueue(t *testing.T, queueName string) *Queue {
	// create the root
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	var leaf *Queue
	// empty parent queue
	leaf, err = createManagedQueue(root, queueName, true, nil)
	assert.NilError(t, err, "failed to create leaf queue: %v", err)
	return leaf
}

func getApps(appIt interfaces.AppIterator) []interfaces.Application {
	apps := make([]interfaces.Application, 0)
	for appIt.HasNext() {
		apps = append(apps, appIt.Next())
	}
	return apps
}
