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

package common

// This interface defines how to storing and managing data in forms of mapping.
type Map interface {
    // add or update a mapping,
    // return the previous value if present, otherwise return nil.
    Put(key interface{}, value interface{}) interface{}
    // remove the mapping with the specified key,
    // return the removed mapping if present, otherwise return nil.
    Remove(key interface{}) interface{}
    // get the value that matches the specified allocation key,
    // return the matched value if present, otherwise return nil.
    Get(key interface{}) interface{}
    // return iterator for all entries
    GetIterator() MapIterator
    // return the size of all entries
    Size() int
    // reset to a clean state
    Reset()
}

// This interface helps to iterate over all entries in mapping
type MapIterator interface {
    // return true if there are more entries to iterate over
    HasNext() (ok bool)
    // return the key and value from the iterator
    Next() (key interface{}, value interface{})
}

// This is an implementation of Map interface which leverages the hash and linked list algorithms
// to make data sorted by specified comparison and efficient to be found and updated.
// It's not thread-safe, must be called while holding the lock.
type SortableLinkedMap struct {
    // a map of entries
    entries map[interface{}]*SortableLinkedMapEntry
    // the head of linked entries
    head *SortableLinkedMapEntry
    // the tail of linked entries
    tail *SortableLinkedMapEntry
    // keep the first entry in linked entries that matches the specific
    // findFirstMatchFunc function.
    firstMatchedEntry *SortableLinkedMapEntry
    // keep linked entries sorted by the provided isPreFuc function,
    // if not provided, always put the new entry into the tail.
    isPreFunc func(i, j interface{}) bool
    // whether the order of linked entries is fixed for updating,
    // won't change the order when updating the value if set to true,
    // otherwise will try to reorder the updated entry.
    // If set to true, make sure that the update won't affect the sort policy
    // defined by isPreFunc function, otherwise it will break the correctness of first matched entry.
    isFixedOrder bool
    // function helps to locate the entry in specified condition.
    matchFunc func(value interface{}) bool
}

// This struct defines a doubly-linked entry
type SortableLinkedMapEntry struct {
    // key of this mapping
    key interface{}
    // value of this mapping
    value interface{}
    // the pre entry
    pre *SortableLinkedMapEntry
    // the next entry
    next *SortableLinkedMapEntry
    // whether this entry is matched to the specified condition
    isMatched bool
}

func NewSortableLinkedMap(isPreFunc func(i, j interface{}) bool, isFixedOrder bool,
    matchFunc func(value interface{}) bool) *SortableLinkedMap {
    return &SortableLinkedMap{
        entries:      make(map[interface{}]*SortableLinkedMapEntry),
        isPreFunc:    isPreFunc,
        isFixedOrder: isFixedOrder,
        matchFunc:    matchFunc,
    }
}

func (slm *SortableLinkedMap) getEntry(entry *SortableLinkedMapEntry) (interface{}, interface{}) {
   if entry != nil {
       return entry.key, entry.value
   }
   return nil, nil
}

func (slm *SortableLinkedMap) GetHead() (interface{}, interface{}) {
   return slm.getEntry(slm.head)
}

func (slm *SortableLinkedMap) GetTail() (interface{}, interface{}) {
   return slm.getEntry(slm.tail)
}

func (slm *SortableLinkedMap) GetIterator() MapIterator {
    return NewSortableLinkedMapIterator(slm, false)
}

func (slm *SortableLinkedMap) GetMatchedIterator() MapIterator {
    return NewSortableLinkedMapIterator(slm, true)
}

func (slm *SortableLinkedMap) GetFirstMatched() (interface{}, interface{}) {
    if slm.firstMatchedEntry == nil {
        return nil, nil
    }
    return slm.firstMatchedEntry.key, slm.firstMatchedEntry.value
}

func (slm *SortableLinkedMap) HasMatched() bool {
    return slm.firstMatchedEntry != nil
}

func (slm *SortableLinkedMap) findPreEntry(fromEntry, newEntry *SortableLinkedMapEntry,
    isForward bool) *SortableLinkedMapEntry {
    // always put the new entry into the tail if isPreFunc isn't defined
    if slm.isPreFunc == nil {
        return slm.tail
    }
    // find the entry
    lookupEntry := fromEntry
    for {
        if lookupEntry == nil {
            break
        } else {
            if isForward {
                if slm.isPreFunc(newEntry.value, lookupEntry.value) {
                    // move forward if newEntry is pre
                    lookupEntry = lookupEntry.pre
                } else {
                    // lookupEntry should be the pre
                    return lookupEntry
                }
            } else {
                if slm.isPreFunc(lookupEntry.value, newEntry.value) {
                    // move backward if lookupEntry is pre
                    lookupEntry = lookupEntry.next
                }  else {
                    // lookupEntry should be next to newEntry
                    return lookupEntry.pre
                }
            }
        }
    }
    if isForward {
        return nil
    } else {
        return slm.tail
    }
}

func (slm *SortableLinkedMap) putIntoLinkedList(preEntry, newEntry *SortableLinkedMapEntry) {
    // insert new entry into the linked list
    if preEntry != nil {
        // put after the specified pre entry
        newEntry.pre = preEntry
        newEntry.next = preEntry.next
        if preEntry.next != nil {
            preEntry.next.pre = newEntry
        }
        preEntry.next = newEntry
    } else {
        // put as the head
        if slm.head == nil {
            slm.head = newEntry
        } else {
            slm.head.pre = newEntry
            newEntry.next = slm.head
            slm.head = newEntry
            slm.head.pre = nil
        }
    }
    // update the tail if it's nil or pre entry
    if slm.tail == preEntry {
        slm.tail = newEntry
    }
}

func (slm *SortableLinkedMap) removeFromLinkedList(entry *SortableLinkedMapEntry) {
    // update the head and tail if necessary
    if entry == slm.head {
        slm.head = entry.next
    }
    if entry == slm.tail {
        slm.tail = entry.pre
    }
    // take out this entry
    if entry.pre != nil {
        entry.pre.next = entry.next
    }
    if entry.next != nil {
        entry.next.pre = entry.pre
    }
    // if first match entry is removed, find next
    if entry == slm.firstMatchedEntry {
        slm.firstMatchedEntry = slm.findNextMatchedEntry()
    }
}

// Reorder the specified entry: remove then add the entry if need to move forward/backward
func (slm *SortableLinkedMap) reorderEntry(entry *SortableLinkedMapEntry) {
    // need to move forward
    if entry.pre != nil {
        if !slm.isPreFunc(entry.pre.value, entry.value) {
            preEntry := slm.findPreEntry(entry.pre, entry, true)
            slm.removeFromLinkedList(entry)
            slm.putIntoLinkedList(preEntry, entry)
        }
    }
    // need to move backward
    if entry.next != nil {
        if !slm.isPreFunc(entry.value, entry.next.value) {
            preEntry := slm.findPreEntry(entry.next, entry, false)
            slm.removeFromLinkedList(entry)
            slm.putIntoLinkedList(preEntry, entry)
        }
    }
}

// Find next match entry after the first match entry
func (slm *SortableLinkedMap) findNextMatchedEntry() *SortableLinkedMapEntry {
    if slm.firstMatchedEntry == nil {
        return nil
    }
    lookupEntry := slm.firstMatchedEntry.next
    for {
        if lookupEntry == nil {
            return nil
        } else if lookupEntry.isMatched {
            return lookupEntry
        }
        lookupEntry = lookupEntry.next
    }
}

// Update isMatched state for entry and first matched entry for sortable linked map,
// this method must be called after the entry has already been updated in the linked map,
// required updates should be as below:
// 1. if updated entry is matched
//      1.1 if first matched entry is not present (no matter whether or not this map is sortable)
//          set firstMatchedEntry = updatedEntry
//      1.2 if this map is sortable and updated entry should be placed before first matched entry,
//          set firstMatchedEntry = updatedEntry
// 2. if update entry is not matched
//      2.1 if updated entry is first matched entry (no matter whether or not this map is sortable),
//          set firstMatchedEntry = nextMatchedEntry
func (slm *SortableLinkedMap) updateMatchedState(updatedEntry * SortableLinkedMapEntry) {
    updatedEntry.isMatched = slm.matchFunc(updatedEntry.value)
    if updatedEntry.isMatched {
        // if updated entry is matched
        if slm.firstMatchedEntry == nil {
            // set first matched entry if not present
            slm.firstMatchedEntry = updatedEntry
        } else if slm.isPreFunc != nil && slm.isPreFunc(updatedEntry.value, slm.firstMatchedEntry.value) {
            // if map is sortable and updated entry should be placed before first matched entry
            slm.firstMatchedEntry = updatedEntry
        }
    } else {
        // if updated entry is not matched
        if updatedEntry == slm.firstMatchedEntry {
            // if updated entry is first matched entry
            slm.firstMatchedEntry = slm.findNextMatchedEntry()
        }
    }
}

func (slm *SortableLinkedMap) Put(key interface{}, value interface{}) interface{} {
    // update the value if entry exists
    if entry, ok := slm.entries[key]; ok {
        updatedValue := entry.value
        entry.value = value
        // reorder this entry if isPreFunc is present and not fixed order
        if slm.isPreFunc != nil && !slm.isFixedOrder {
            // reorder entry
            slm.reorderEntry(entry)
        }
        // update matched state
        if slm.matchFunc != nil {
            slm.updateMatchedState(entry)
        }
        return updatedValue
    }
    // create a new entry
    newEntry := &SortableLinkedMapEntry{
        key:    key,
        value:  value,
    }
    // find the pre entry
    preEntry := slm.findPreEntry(slm.tail, newEntry, true)
    // insert into the linked list
    slm.putIntoLinkedList(preEntry, newEntry)
    // update matched state
    if slm.matchFunc != nil {
        slm.updateMatchedState(newEntry)
    }
    // update entry map
    slm.entries[key] = newEntry
    return newEntry.value
}

func (slm *SortableLinkedMap) Remove(key interface{}) interface{} {
    if entry := slm.entries[key]; entry != nil {
        // remove from linked list
        slm.removeFromLinkedList(entry)
        // delete from map
        delete(slm.entries, key)
        return entry.value
    }
    return nil
}

func (slm *SortableLinkedMap) Get(key interface{}) interface{} {
    if entry := slm.entries[key]; entry != nil {
        return entry.value
    }
    return nil
}

func (slm *SortableLinkedMap) Size() int {
    return len(slm.entries)
}

func (slm *SortableLinkedMap) getEntryItems(getItemFunc func(*SortableLinkedMapEntry) interface{}) []interface{} {
    results := make([]interface{}, 0)
    lookupEntry := slm.head
    for {
        if lookupEntry != nil {
            results = append(results, getItemFunc(lookupEntry))
            lookupEntry = lookupEntry.next
        } else {
            break
        }
    }
    return results
}

func (slm *SortableLinkedMap) Values() []interface{} {
    return slm.getEntryItems(func(entry *SortableLinkedMapEntry) interface{} {
        return entry.value
    })
}

func (slm *SortableLinkedMap) Keys() []interface{} {
    return slm.getEntryItems(func(entry *SortableLinkedMapEntry) interface{} {
        return entry.key
    })
}

func (slm *SortableLinkedMap) Reset() {
    slm.entries = make(map[interface{}]*SortableLinkedMapEntry)
    slm.head = nil
    slm.tail = nil
    slm.firstMatchedEntry = nil
}

type SortableLinkedMapIterator struct {
    findMatched bool
    indexEntry *SortableLinkedMapEntry
}

func NewSortableLinkedMapIterator(linkedMap *SortableLinkedMap, findMatched bool) *SortableLinkedMapIterator {
    if linkedMap == nil {
        return &SortableLinkedMapIterator{}
    }
    return &SortableLinkedMapIterator{
        findMatched: findMatched,
        indexEntry:  linkedMap.head,
    }
}

func (slmi *SortableLinkedMapIterator) findNextMatchedEntry() {
    lookupEntry := slmi.indexEntry
    for {
        if lookupEntry == nil {
            slmi.indexEntry = nil
            break
        } else if lookupEntry.isMatched {
            slmi.indexEntry = lookupEntry
            break
        }
        lookupEntry = lookupEntry.next
    }
}

func (slmi *SortableLinkedMapIterator) HasNext() bool {
    if slmi.findMatched {
        slmi.findNextMatchedEntry()
    }
    return slmi.indexEntry != nil
}

func (slmi *SortableLinkedMapIterator) Next() (key interface{}, value interface{}) {
    if slmi.findMatched {
        slmi.findNextMatchedEntry()
    }
    if slmi.indexEntry == nil {
        return nil, nil
    }
    k, v := slmi.indexEntry.key, slmi.indexEntry.value
    slmi.indexEntry = slmi.indexEntry.next
    return k, v
}
