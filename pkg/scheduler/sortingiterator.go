package scheduler

import (
	"math/rand"
)

// SortingIterator generates a list of nodes sorted as per defined policy
type SortingIterator interface {
	HasNext() (ok bool)
	Next() (node *SchedulingNode)
}

type BinPackingSortingIterator struct {
	SortingIterator
	countIdx int
	nodes  []*SchedulingNode
}

type FairSortingIterator struct {
	SortingIterator
	startIdx int
	countIdx int
	nodes  []*SchedulingNode
}

func NewFairSortingIterator(schedulerNodes []*SchedulingNode) *FairSortingIterator {
	return &FairSortingIterator{
		nodes : schedulerNodes,
		countIdx : 0,
	}
}

func NewBinPackingSortingIterator(schedulerNodes []*SchedulingNode) *BinPackingSortingIterator {
	return &BinPackingSortingIterator{
		nodes : schedulerNodes,
		countIdx : 0,
	}
}

// Next returns the next element and advances to next element in array.
// Returns nil and reset the starting point at the end of iteration.
func (i *BinPackingSortingIterator) Next() *SchedulingNode {
	len := len(i.nodes)
	if (i.countIdx + 1) > len {
		return nil
	}

	value := i.nodes[i.countIdx]
	i.countIdx++
	return value
}

// HasNext retrurns true if there is a next element in the array.
// Returns false if there are no more elemeents.
func (i *BinPackingSortingIterator) HasNext() bool {
	len := len(i.nodes)
	if (i.countIdx + 1) > len {
		return false
	}
	return true
}

// Next returns the next element and advances to next element in array.
// Returns nil and reset the starting point at the end of iteration.
func (i *FairSortingIterator) Next() *SchedulingNode {
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

// HasNext retrurns true if there is a next element in the array.
// Returns false if there are no more elemeents.
func (i *FairSortingIterator) HasNext() bool {
	len := len(i.nodes)
	if (i.countIdx + 1) > len {
		// reset the rand value after one iteration
		i.startIdx = -1
		return false
	}
	return true
}
