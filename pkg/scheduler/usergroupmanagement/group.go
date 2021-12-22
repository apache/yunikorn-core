package usergroupmanagement

import (
	"sync/atomic"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

type Group struct {
	name	string // Name of the group
	maxResources	*resources.Resource // Max Resource configured per group
	maxApplications	int32 // Max Applications configured per group
	runningApplications	*int32 // Running Applications
}

func NewGroup(group string) *Group {
	return &Group{
		name:	group,
	}
}

func (g *Group) GetName() string {
	return g.name
}

func (g *Group) SetMaxApplications(maxApplications int32) {
	g.maxApplications = maxApplications
}

func (g *Group) IncRunningApplications() {
	atomic.AddInt32(g.runningApplications, 1)
}

func (g *Group) DecRunningApplications() {
	atomic.AddInt32(g.runningApplications, -1)
}

func (g *Group) CanRun() bool {
	if atomic.LoadInt32(g.runningApplications) < g.maxApplications {
		return true
	} else {
		return false
	}
}