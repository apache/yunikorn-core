package objects

import (
	"sync/atomic"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

const ALL_USER = "*"
const USER_MAX_APPLICATION_LIMIT_NOT_SET = -1

type User struct {
	name	string
	maxResources	*resources.Resource
	maxApplications	int32
	runningApplications	int32
	usedGroup	string
}

func NewUser(user string) *User {
	return &User{
		name:	user,
		runningApplications: 0,
		maxApplications: USER_MAX_APPLICATION_LIMIT_NOT_SET,
	}
}

func (u *User) GetName() string {
	return u.name
}

func (u *User) SetMaxApplications(maxApplications int32) {
	u.maxApplications = maxApplications
}

func (u *User) GetMaxApplications() int32 {
	return u.maxApplications
}

func (u *User) IncRunningApplications() {
	atomic.AddInt32(&u.runningApplications, 1)
}

func (u *User) DecRunningApplications() {
	atomic.AddInt32(&u.runningApplications, -1)
}

func (u *User) IsRunningAppsUnderLimit() bool {
	if atomic.LoadInt32(&u.runningApplications) < u.maxApplications {
		return true
	} else {
		return false
	}
}

func (u *User) IsMaxAppsLimitSet() bool {
	return u.GetMaxApplications() != USER_MAX_APPLICATION_LIMIT_NOT_SET
}

// SetUsedGroup A user may belong to more than one group. In case of any group changes for
// any user while running applications, without this usedGroup info might lead
// to confusion while doing any metrics calculation as and when user is done with
// their activities. Also, this info avoids parsing all mapped groups while doing calculations
func (u *User) SetUsedGroup(usedGroup string) {
	u.usedGroup = usedGroup
}

func (u *User) GetUsedGroup() string {
	return u.usedGroup
}