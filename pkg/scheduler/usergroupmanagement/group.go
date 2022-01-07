package usergroupmanagement

import (
	"sync/atomic"

	"github.com/apache/incubator-yunikorn-core/pkg/common/resources"
)

type Group struct {
	name	string
	maxResources	*resources.Resource
	maxApplications	int32
	runningApplications	*int32
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