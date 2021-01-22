package defaults

import (
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/interfaces"
)

type fakeRequest struct {
	allocationKey    string
	pendingAskRepeat int32
	priority         int32
	createTime       time.Time
}

func newFakeRequest(allocationKey string, pendingAskRepeat int32, priority int32) *fakeRequest {
	return &fakeRequest{
		allocationKey:    allocationKey,
		pendingAskRepeat: pendingAskRepeat,
		priority:         priority,
		createTime:       time.Now(),
	}
}

func (fr *fakeRequest) GetAllocationKey() string {
	return fr.allocationKey
}

func (fr *fakeRequest) GetPendingAskRepeat() int32 {
	return fr.pendingAskRepeat
}

func (fr *fakeRequest) GetPriority() int32 {
	return fr.priority
}

func (fr *fakeRequest) GetCreateTime() time.Time {
	return fr.createTime
}

// Test adding/updating/removing requests with different priorities,
// to verify the correctness of all/pending priority groups and requests.
func TestRequests(t *testing.T, requests interfaces.Requests) {
	// ignore nil request
	requests.AddOrUpdateRequest(nil)
	assert.Equal(t, requests.Size(), 0)

	// add request-1 with priority 1
	reqKey1 := "req-1"
	req1 := newFakeRequest(reqKey1, 1, 1)
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey1}, []string{reqKey1})

	// update request-1 to non-pending state
	req1.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey1}, []string{})

	// update request-1 to pending state
	req1.pendingAskRepeat = 2
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey1}, []string{reqKey1})

	// add request-2 with priority 1
	reqKey2 := "req-2"
	req2 := newFakeRequest(reqKey2, 1, 1)
	requests.AddOrUpdateRequest(req2)
	checkRequests(t, requests, []string{reqKey1, reqKey2}, []string{reqKey1, reqKey2})

	// update request-1 to non-pending state
	req2.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req2)
	checkRequests(t, requests, []string{reqKey1, reqKey2}, []string{reqKey1})

	// add request-3 with priority 1
	reqKey3 := "req-3"
	req3 := newFakeRequest(reqKey3, 1, 1)
	requests.AddOrUpdateRequest(req3)
	checkRequests(t, requests, []string{reqKey1, reqKey2, reqKey3}, []string{reqKey1, reqKey3})

	// add request-4 with priority 3
	reqKey4 := "req-4"
	req4 := newFakeRequest(reqKey4, 1, 3)
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey1, reqKey3})

	// update request-4 to non-pending state
	req4.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey1, reqKey2, reqKey3}, []string{reqKey1, reqKey3})

	// update request-4 to pending state
	req4.pendingAskRepeat = 2
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey1, reqKey3})

	// add request-5 with priority 2
	reqKey5 := "req-5"
	req5 := newFakeRequest(reqKey5, 1, 2)
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey1, reqKey3})

	// add request-0 with priority 1 and create time is less than request-1
	// it should be placed before request-1
	reqKey0 := "req-0"
	req0 := newFakeRequest(reqKey0, 1, 1)
	req0.createTime = req1.createTime.Add(-1 * time.Millisecond)
	requests.AddOrUpdateRequest(req0)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey0, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey0, reqKey1, reqKey3})

	// remove request-0
	requests.RemoveRequest(reqKey0)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey1, reqKey3})

	// update request-5 to non-pending state
	req5.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey1, reqKey3})

	// update request-1 to non-pending state
	req1.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey3})

	// update request-3 to non-pending state
	req3.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req3)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4})

	// update request-4 to non-pending state
	req4.pendingAskRepeat = 0
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{})

	// update request-5 to pending state
	req5.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey5})

	// update request-4 to pending state
	req4.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req4)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey5})

	// update request-3 to pending state
	req3.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req3)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey4, reqKey5, reqKey3})

	// update request-2 to pending state
	req2.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req2)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey2, reqKey3})

	// update request-1 to pending state
	req1.pendingAskRepeat = 1
	requests.AddOrUpdateRequest(req1)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3},
		[]string{reqKey4, reqKey5, reqKey1, reqKey2, reqKey3})

	// remove request-4
	requests.RemoveRequest(reqKey4)
	checkRequests(t, requests, []string{reqKey5, reqKey1, reqKey2, reqKey3}, []string{reqKey5, reqKey1, reqKey2, reqKey3})

	// remove request-3
	requests.RemoveRequest(reqKey3)
	checkRequests(t, requests, []string{reqKey5, reqKey1, reqKey2}, []string{reqKey5, reqKey1, reqKey2})

	// remove request-5
	requests.RemoveRequest(reqKey5)
	checkRequests(t, requests, []string{reqKey1, reqKey2}, []string{reqKey1, reqKey2})

	// remove request-2
	requests.RemoveRequest(reqKey2)
	checkRequests(t, requests, []string{reqKey1}, []string{reqKey1})

	// remove request-1
	requests.RemoveRequest(reqKey1)
	checkRequests(t, requests, []string{}, []string{})

	// add request-1/request-2/request-4/request-5
	requests.AddOrUpdateRequest(req1)
	requests.AddOrUpdateRequest(req2)
	requests.AddOrUpdateRequest(req4)
	requests.AddOrUpdateRequest(req5)
	checkRequests(t, requests, []string{reqKey4, reqKey5, reqKey1, reqKey2}, []string{reqKey4, reqKey5, reqKey1, reqKey2})

	// reset
	requests.Reset()
	checkRequests(t, requests, []string{}, []string{})
}

// Check the correctness of all/pending requests
func checkRequests(t *testing.T, requests interfaces.Requests, expectedRequestKeys []string,
	expectedPendingRequestKeys []string) {
	// check length of all requests
	t.Logf("Check sorted requests: expectedRequestKeys=%v", expectedRequestKeys)
	assert.Equal(t, requests.Size(), len(expectedRequestKeys),
		"length of requests differs: expected=%v, actual=%v",
		len(expectedRequestKeys), requests.Size())
	// check length of pending requests
	reqIt := requests.SortForAllocation()
	assert.Equal(t, reqIt.Size(), len(expectedPendingRequestKeys),
		"length of pending requests differs: expected=%v, actual=%v",
		len(expectedPendingRequestKeys), reqIt.Size())

	reqIndex := 0
	for reqIt.HasNext() {
		assert.Equal(t, reqIt.Next().GetAllocationKey(), expectedPendingRequestKeys[reqIndex],
			"length of pending requests differs: expected=%v, actual=%v",
			expectedPendingRequestKeys[reqIndex])
		reqIndex++
	}
}
