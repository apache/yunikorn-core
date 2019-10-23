package common

import (
	"github.com/cloudera/yunikorn-core/pkg/scheduler"
	"math/rand"
)

// SortingIterator generates a list of nodes sorted as per defined policy
type SortingIterator interface {
	HasNext() (ok bool)
	Next() (node *scheduler.SchedulingNode)
}

type BinPackingSortingIterator struct {
	SortingIterator
	countIdx int
	nodes  []*scheduler.SchedulingNode
}

type FairSortingIterator struct {
	SortingIterator
	startIdx int
	countIdx int
	nodes  []*scheduler.SchedulingNode
}

func NewFairSortingIterator(schedulerNodes []*scheduler.SchedulingNode) *FairSortingIterator {
	return &FairSortingIterator{
		nodes : schedulerNodes,
		countIdx : 0,
	}
}

func NewBinPackingSortingIterator(schedulerNodes []*scheduler.SchedulingNode) *BinPackingSortingIterator {
	return &BinPackingSortingIterator{
		nodes : schedulerNodes,
		countIdx : 0,
	}
}

// Next advances to next element in array. Returns false on end of iteration.
func (i *BinPackingSortingIterator) Next() *scheduler.SchedulingNode {
	len := len(i.nodes)
	if (i.countIdx + 1) > len {
		return nil
	}

	value := i.nodes[i.countIdx]
	i.countIdx++
	return value
}

// Next advances to next element in array. Returns false on end of iteration.
func (i *BinPackingSortingIterator) HasNext() bool {
	len := len(i.nodes)
	if (i.countIdx + 1) > len {
		return false
	}
	return true
}

// Next advances to next element in array. Returns false on end of iteration.
func (i *FairSortingIterator) Next() *scheduler.SchedulingNode {
	len := len(i.nodes)

	// For the first time, initialize the rand seed based on number of nodes.
	if i.startIdx == -1 {
		i.startIdx = rand.Intn(len)
	}

	if (i.countIdx + 1) > len {
		// reset the rand value after one full iteration
		i.startIdx = -1
		return nil
	}

	idx := (i.countIdx + i.startIdx) % len
	value := i.nodes[idx]
	i.countIdx++
	return value
}

// Next advances to next element in array. Returns false on end of iteration.
func (i *FairSortingIterator) HasNext() bool {
	len := len(i.nodes)
	if (i.countIdx + 1) > len {
		// reset the rand value after one iteration
		i.startIdx = -1
		return false
	}
	return true
}