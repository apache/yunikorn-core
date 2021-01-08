package objects

import (
	"strconv"
	"testing"
	"time"

	"github.com/apache/incubator-yunikorn-core/pkg/plugins/default_plugins_impl"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
	"github.com/apache/incubator-yunikorn-core/pkg/scheduler/policies"
	"gotest.tools/assert"
)

func TestSortAppsNoPending(t *testing.T) {
	// init queue
	queueName := "leaf"
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
	//list := default_plugins_impl.SortApplications(input, policies.FairSortPolicy, nil)
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
	queueName := "leaf"
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
	queueName := "leaf"
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
	//list := default_plugins_impl.SortApplications(input, policies.FairSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3}, "nil total")

	// apps should come back in order: 0, 1, 2, 3
	//list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 0))
	leafQueue.guaranteedResource = resources.Multiply(res, 0)
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{0, 1, 2, 3}, "zero total")

	// apps should come back in order: 0, 1, 2, 3
	//list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	leafQueue.guaranteedResource = resources.Multiply(res, 5)
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{0, 1, 2, 3}, "no alloc, set total")

	// update allocated resource for app-1
	leafQueue.applications.GetApplication("app-1").(*Application).allocatedResource = resources.Multiply(res, 10)
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	// apps should come back in order: 0, 2, 3, 1
	//list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 3, 1, 2}, "app-1 allocated")

	// update allocated resource for app-3 to negative (move to head of the list)
	leafQueue.applications.GetApplication("app-3").(*Application).allocatedResource = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	list = getApps(leafQueue.GetApplications().SortForAllocation())
	assertAppList(t, list, []int{1, 3, 2, 0}, "app-1 & app-3 allocated")
}

func TestSortAppsStateAware(t *testing.T) {
	// init queue
	queueName := "leaf"
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
	default_plugins_impl.SortAskByPriority(list, true)
	// asks should come back in order: 0, 1, 2, 3
	assertAskList(t, list, []int{0, 1, 2, 3}, "ascending")
	// move things around
	list[0], list[2] = list[2], list[0]
	list[1], list[3] = list[3], list[1]
	assertAskList(t, list, []int{2, 3, 0, 1}, "moved 2")
	default_plugins_impl.SortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 1, 0
	assertAskList(t, list, []int{3, 2, 1, 0}, "descending")
	// make asks with same priority
	// ask-3 and ask-1 both with prio 1 do not change order
	// ask-3 must always be earlier in the list
	list[0].(*AllocationAsk).priority = 1
	default_plugins_impl.SortAskByPriority(list, true)
	// asks should come back in order: 0, 2, 3, 1
	assertAskList(t, list, []int{0, 1, 3, 2}, "ascending same prio")
	default_plugins_impl.SortAskByPriority(list, false)
	// asks should come back in order: 3, 2, 0, 1
	assertAskList(t, list, []int{3, 1, 0, 2}, "descending same prio")
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
