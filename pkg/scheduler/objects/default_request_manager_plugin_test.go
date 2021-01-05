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
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	input := make(map[string]interfaces.Application, 4)
	for i := 0; i < 2; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		input[appID] = app
	}

	// no apps with pending resources should come back empty
	list := default_plugins_impl.SortApplications(input, policies.FairSortPolicy, nil)
	assertAppListLength(t, list, []string{}, "fair no pending")
	list = default_plugins_impl.SortApplications(input, policies.FifoSortPolicy, nil)
	assertAppListLength(t, list, []string{}, "fifo no pending")
	list = default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{}, "state no pending")

	// set one app with pending
	appID := "app-1"
	input[appID].(*Application).pending = res
	list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, nil)
	assertAppListLength(t, list, []string{appID}, "fair one pending")
	list = default_plugins_impl.SortApplications(input, policies.FifoSortPolicy, nil)
	assertAppListLength(t, list, []string{appID}, "fifo one pending")
	list = default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID}, "state one pending")
}

func TestSortAppsFifo(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]interfaces.Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.pending = res
		input[appID] = app
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}
	// map iteration is random so we do not need to check input
	// apps should come back in order created 0, 1, 2, 3
	list := default_plugins_impl.SortApplications(input, policies.FifoSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3}, "fifo simple")
}

func TestSortAppsFair(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup to sort descending: all apps have pending resources
	input := make(map[string]interfaces.Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.allocatedResource = resources.Multiply(res, int64(i+1))
		app.pending = res
		input[appID] = app
	}
	// nil resource: usage based sorting
	// apps should come back in order: 0, 1, 2, 3
	list := default_plugins_impl.SortApplications(input, policies.FairSortPolicy, nil)
	assertAppList(t, list, []int{0, 1, 2, 3}, "nil total")

	// apps should come back in order: 0, 1, 2, 3
	list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 0))
	assertAppList(t, list, []int{0, 1, 2, 3}, "zero total")

	// apps should come back in order: 0, 1, 2, 3
	list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 1, 2, 3}, "no alloc, set total")

	// update allocated resource for app-1
	input["app-1"].(*Application).allocatedResource = resources.Multiply(res, 10)
	// apps should come back in order: 0, 2, 3, 1
	list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{0, 3, 1, 2}, "app-1 allocated")

	// update allocated resource for app-3 to negative (move to head of the list)
	input["app-3"].(*Application).allocatedResource = resources.Multiply(res, -10)
	// apps should come back in order: 3, 0, 2, 1
	list = default_plugins_impl.SortApplications(input, policies.FairSortPolicy, resources.Multiply(res, 5))
	assertAppList(t, list, []int{1, 3, 2, 0}, "app-1 & app-3 allocated")
}

func TestSortAppsStateAware(t *testing.T) {
	// stable sort is used so equal values stay where they were
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"first": resources.Quantity(100)})
	// setup all apps with pending resources, all accepted state
	input := make(map[string]interfaces.Application, 4)
	for i := 0; i < 4; i++ {
		num := strconv.Itoa(i)
		appID := "app-" + num
		app := newApplication(appID, "partition", "queue")
		app.pending = res
		input[appID] = app
		err := app.HandleApplicationEvent(runApplication)
		assert.NilError(t, err, "state change failed for app %v", appID)
		// make sure the time stamps differ at least a bit (tracking in nano seconds)
		time.Sleep(time.Nanosecond * 5)
	}
	// only first app should be returned (all in accepted)
	list := default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
	appID0 := "app-0"
	assertAppListLength(t, list, []string{appID0}, "state all accepted")

	// set first app pending to zero, should get 2nd app back
	input[appID0].(*Application).pending = resources.NewResource()
	list = default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
	appID1 := "app-1"
	assertAppListLength(t, list, []string{appID1}, "state no pending")

	// move the first app to starting no pending resource should get nothing
	err := input[appID0].(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-0")
	list = default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{}, "state starting no pending")

	// move first app to running (no pending resource) and 4th app to starting should get starting app
	err = input[appID0].(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-0")
	appID3 := "app-3"
	err = input[appID3].(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID3}, "state starting")

	// set pending for first app, should get back 1st and 4th in that order
	input[appID0].(*Application).pending = res
	list = default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
	assertAppListLength(t, list, []string{appID0, appID3}, "state first pending")

	// move 4th to running should get back: 1st, 2nd and 4th in that order
	err = input[appID3].(*Application).HandleApplicationEvent(runApplication)
	assert.NilError(t, err, "state change failed for app-3")
	list = default_plugins_impl.SortApplications(input, policies.StateAwarePolicy, nil)
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
