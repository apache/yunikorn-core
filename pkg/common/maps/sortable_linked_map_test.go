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

package maps

import (
    "fmt"
    "gotest.tools/assert"
    "math/rand"
    "sort"
    "testing"
)

func checkIterator(t *testing.T, iterator MapIterator, expectedKeys []interface{}, expectedValues []interface{}) {
    errMsg := fmt.Sprintf("Check failed for iterator with expectedKeys: %v and expectedValues:%v",
        expectedKeys, expectedValues)
    mapIt := iterator
    index := 0
    for mapIt.HasNext() {
        itKey, itValue := mapIt.Next()
        assert.Equal(t, itKey, expectedKeys[index], errMsg)
        assert.Equal(t, itValue, expectedValues[index], errMsg)
        index++
    }
    assert.Equal(t, index, len(expectedValues), errMsg)
}

func checkEmptyMap(t *testing.T, testMap *SortableLinkedMap) {
    headKey, headValue := testMap.GetHead()
    assert.Equal(t, headKey, nil)
    assert.Equal(t, headValue, nil)
    tailKey, tailValue := testMap.GetTail()
    assert.Equal(t, tailKey, nil)
    assert.Equal(t, tailValue, nil)
    assert.Equal(t, testMap.Size(), 0)
    assert.Equal(t, len(testMap.Keys()), 0)
    assert.Equal(t, len(testMap.Values()), 0)
    assert.Equal(t, testMap.Get(1), nil)
    assert.Equal(t, testMap.Get(""), nil)
    assert.Assert(t, testMap.firstMatchedEntry == nil)
    assert.Equal(t, len(testMap.entries), 0)
}

func checkMap(t *testing.T, testMap *SortableLinkedMap, expectedKeys []interface{}, expectedValues []interface{},
    expectedFirstMatchedValue interface{}) {
    if len(expectedKeys) == 0 {
        checkEmptyMap(t, testMap)
        return
    }
    // check head & tail
    t.Logf("Check SortableLinkedMap: expectedKeys=%v, expectedValues=%v",
        expectedKeys, expectedValues)
    assert.Assert(t, testMap.head.pre == nil)
    assert.Assert(t, testMap.tail.next == nil)
    headKey, headValue := testMap.GetHead()
    assert.Equal(t, headKey, expectedKeys[0])
    assert.Equal(t, headValue, expectedValues[0])
    tailKey, tailValue := testMap.GetTail()
    assert.Equal(t, tailKey, expectedKeys[len(expectedKeys)-1])
    assert.Equal(t, tailValue, expectedValues[len(expectedValues)-1])
    // check size
    assert.Equal(t, len(expectedKeys), len(expectedValues))
    assert.Equal(t, len(testMap.entries), len(expectedValues))
    assert.Equal(t, testMap.Size(), len(expectedValues))
    // check keys & values
    keys := testMap.Keys()
    values := testMap.Values()
    for index, key := range keys {
        assert.Equal(t, key, expectedKeys[index])
        value := testMap.Get(key)
        assert.Equal(t, value, expectedValues[index])
        assert.Equal(t, value, values[index])
    }
    // check first matched entry
    if expectedFirstMatchedValue == nil {
        assert.Assert(t, testMap.firstMatchedEntry == nil)
    } else {
        assert.Equal(t, testMap.firstMatchedEntry.value, expectedFirstMatchedValue)
    }
    // check iterator
    checkIterator(t, testMap.GetIterator(), expectedKeys, expectedValues)
}

// Test simple linked map without sorting
func TestSimpleLinkedMap(t *testing.T) {
    testMap := NewSortableLinkedMap(nil, nil)
    // empty map
    checkEmptyMap(t, testMap)

    // put
    result := testMap.Put(1, 1)
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1}, []interface{}{1}, nil)

    result = testMap.Put("2", "2")
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1, "2"}, []interface{}{1, "2"}, nil)

    result = testMap.Put("3", 3)
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1, "2", "3"}, []interface{}{1, "2", 3}, nil)

    result = testMap.Put(4, "4")
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4}, []interface{}{1, "2", 3, "4"}, nil)

    result = testMap.Put(5, 5)
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5}, []interface{}{1, "2", 3, "4", 5}, nil)

    result = testMap.Put(6, nil)
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6}, []interface{}{1, "2", 3, "4", 5, nil}, nil)

    result = testMap.Put(nil, 7)
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{1, "2", 3, "4", 5, nil, 7}, nil)

    // replace
    result = testMap.Put(1, 11)
    assert.Equal(t, result, 1)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{11, "2", 3, "4", 5, nil, 7}, nil)

    result = testMap.Put("2", "22")
    assert.Equal(t, result, "2")
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{11, "22", 3, "4", 5, nil, 7}, nil)

    result = testMap.Put("3", "33")
    assert.Equal(t, result, 3)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{11, "22", "33", "4", 5, nil, 7}, nil)

    result = testMap.Put(4, 44)
    assert.Equal(t, result, "4")
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{11, "22", "33", 44, 5, nil, 7}, nil)

    result = testMap.Put(5, 55)
    assert.Equal(t, result, 5)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{11, "22", "33", 44, 55, nil, 7}, nil)

    result = testMap.Put(6, "66")
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{11, "22", "33", 44, 55, "66", 7}, nil)

    result = testMap.Put(nil, nil)
    assert.Equal(t, result, 7)
    checkMap(t, testMap, []interface{}{1, "2", "3", 4, 5, 6, nil}, []interface{}{11, "22", "33", 44, 55, "66", nil}, nil)

    // remove first entry
    result = testMap.Remove(1)
    assert.Equal(t, result, 11)
    checkMap(t, testMap, []interface{}{"2", "3", 4, 5, 6, nil}, []interface{}{"22", "33", 44, 55, "66", nil}, nil)

    // remove last entry
    result = testMap.Remove(nil)
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{"2", "3", 4, 5, 6}, []interface{}{"22", "33", 44, 55, "66"}, nil)

    // remove middle entry
    result = testMap.Remove("3")
    assert.Equal(t, result, "33")
    checkMap(t, testMap, []interface{}{"2", 4, 5, 6}, []interface{}{"22", 44, 55, "66"}, nil)

    // remove not exist entry
    result = testMap.Remove("X")
    assert.Equal(t, result, nil)
    checkMap(t, testMap, []interface{}{"2", 4, 5, 6}, []interface{}{"22", 44, 55, "66"}, nil)

    result = testMap.Remove(4)
    assert.Equal(t, result, 44)
    checkMap(t, testMap, []interface{}{"2", 5, 6}, []interface{}{"22", 55, "66"}, nil)

    result = testMap.Remove("2")
    assert.Equal(t, result, "22")
    checkMap(t, testMap, []interface{}{5, 6}, []interface{}{55, "66"}, nil)

    result = testMap.Remove(6)
    assert.Equal(t, result, "66")
    checkMap(t, testMap, []interface{}{5}, []interface{}{55}, nil)

    result = testMap.Remove(5)
    assert.Equal(t, result, 55)
    checkEmptyMap(t, testMap)
}

// Test sortable linked map
func TestSortableLinkedMap(t *testing.T) {
    testMap := NewSortableLinkedMap(func(i, j interface{}) bool {
        return i.(int) > j.(int)
    }, func(value interface{}) bool {
        return value.(int) % 2 == 0
    })
    // empty map
    checkEmptyMap(t, testMap)

    // put
    testMap.Put(1, 1)
    checkMap(t, testMap, []interface{}{1}, []interface{}{1}, nil)

    testMap.Put(2, 2)
    checkMap(t, testMap, []interface{}{2, 1}, []interface{}{2, 1}, 2)

    testMap.Put(3, 3)
    checkMap(t, testMap, []interface{}{3, 2, 1}, []interface{}{3, 2, 1}, 2)

    testMap.Put(4, 4)
    checkMap(t, testMap, []interface{}{4, 3, 2, 1}, []interface{}{4, 3, 2, 1}, 4)

    testMap.Put(5, 5)
    checkMap(t, testMap, []interface{}{5, 4, 3, 2, 1}, []interface{}{5, 4, 3, 2, 1}, 4)

    testMap.Put(6, 6)
    checkMap(t, testMap, []interface{}{6, 5, 4, 3, 2, 1}, []interface{}{6, 5, 4, 3, 2, 1}, 6)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{6, 4, 2}, []interface{}{6, 4, 2})

    // replace
    testMap.Put(1, 11)
    checkMap(t, testMap, []interface{}{6, 5, 4, 3, 2, 1}, []interface{}{6, 5, 4, 3, 2, 11}, 6)

    testMap.Put(3, 0)
    checkMap(t, testMap, []interface{}{6, 5, 4, 3, 2, 1}, []interface{}{6, 5, 4, 0, 2, 11}, 6)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{6, 4, 3, 2}, []interface{}{6, 4, 0, 2})

    testMap.Put(6, 3)
    checkMap(t, testMap, []interface{}{6, 5, 4, 3, 2, 1}, []interface{}{3, 5, 4, 0, 2, 11}, 4)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{4, 3, 2}, []interface{}{4, 0, 2})

    testMap.Put(6, 6)
    checkMap(t, testMap, []interface{}{6, 5, 4, 3, 2, 1}, []interface{}{6, 5, 4, 0, 2, 11}, 6)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{6, 4, 3, 2}, []interface{}{6, 4, 0, 2})

    // remove
    testMap.Remove(6)
    checkMap(t, testMap, []interface{}{5, 4, 3, 2, 1}, []interface{}{5, 4, 0, 2, 11}, 4)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{4, 3, 2}, []interface{}{4, 0, 2})

    testMap.Remove(4)
    checkMap(t, testMap, []interface{}{5, 3, 2, 1}, []interface{}{5, 0, 2, 11}, 0)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{3, 2}, []interface{}{0, 2})

    testMap.Remove(3)
    checkMap(t, testMap, []interface{}{5, 2, 1}, []interface{}{5, 2, 11}, 2)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{2}, []interface{}{2})

    testMap.Remove(1)
    checkMap(t, testMap, []interface{}{5, 2}, []interface{}{5, 2}, 2)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{2}, []interface{}{2})

    testMap.Remove(2)
    checkMap(t, testMap, []interface{}{5}, []interface{}{5}, nil)
    checkIterator(t, testMap.GetMatchedIterator(), []interface{}{}, []interface{}{})

    testMap.Remove(5)
    checkEmptyMap(t, testMap)
}

// Test updating sortable linked map randomly
func TestUpdatingSortableLinkedMapRandomly(t *testing.T) {
    testMap := NewSortableLinkedMap(func(i, j interface{}) bool {
        return i.(int) > j.(int)
    }, func(value interface{}) bool {
        return value.(int) % 2 == 0
    })
    // empty map
    checkEmptyMap(t, testMap)

    // add random value in the interval [0, 1000)
    randomInts := rand.Perm(1000)
    expectedKVs := make(map[int]int, 1000)
    for i, randomInt := range randomInts {
        testMap.Put(randomInt, randomInt)
        expectedKVs[i] = i
    }
    sortedKeyValues, expectedFirstMatchedValue := getExpectedResults(expectedKVs)
    checkMap(t, testMap, sortedKeyValues, sortedKeyValues, expectedFirstMatchedValue)

    // remove random value in the interval [0, 1000)
    randomInts = rand.Perm(1000)
    for i, randomInt := range randomInts {
        testMap.Remove(randomInt)
        delete(expectedKVs, randomInt)
        if i % 10 == 0 {
            sortedKeyValues, expectedFirstMatchedValue := getExpectedResults(expectedKVs)
            checkMap(t, testMap, sortedKeyValues, sortedKeyValues, expectedFirstMatchedValue)
        }
    }
    checkEmptyMap(t, testMap)

    // random add or remove value
    maxRandNum := 100
    for i := 0; i < 1000; i++ {
        randomOpt := rand.Intn(2)
        if randomOpt == 0 {
            // remove random value
            if len(expectedKVs) > 0 {
                randomKV := getExistingRandomKey(expectedKVs)
                delete(expectedKVs, randomKV)
                removedValue := testMap.Remove(randomKV)
                assert.Equal(t, removedValue, randomKV)
                sortedKeyValues, expectedFirstMatchedValue := getExpectedResults(expectedKVs)
                checkMap(t, testMap, sortedKeyValues, sortedKeyValues, expectedFirstMatchedValue)
            }
        } else {
            // add or update random value
            randomKV := getNonExistingRandomKey(expectedKVs, maxRandNum)
            expectedKVs[randomKV] = randomKV
            oldValue := testMap.Put(randomKV, randomKV)
            assert.Assert(t, oldValue == nil)
            // check
            sortedKeyValues, expectedFirstMatchedValue := getExpectedResults(expectedKVs)
            checkMap(t, testMap, sortedKeyValues, sortedKeyValues, expectedFirstMatchedValue)
        }
    }
}

func getExistingRandomKey(kvMap map[int]int) int {
    keys := make([]int, len(kvMap))
    i := 0
    for k := range kvMap {
        keys[i] = k
        i++
    }
    return keys[rand.Intn(len(keys))]
}

func getNonExistingRandomKey(kvMap map[int]int, maxRandNum int) int {
    if len(kvMap) == maxRandNum {
        return -1
    }
    randInts := rand.Perm(maxRandNum)
    for _, randKV := range randInts {
        if _, ok := kvMap[randKV]; !ok {
            return randKV
        }
    }
    return -1
}

func getExpectedResults(kvMap map[int]int) ([]interface{}, interface{}) {
    keyValues := make([]int, len(kvMap))
    i := 0
    for k := range kvMap {
        keyValues[i] = k
        i++
    }
    sort.Sort(sort.Reverse(sort.IntSlice(keyValues)))
    expectedKeyValues := make([]interface{}, len(keyValues))
    var expectedFirstMatchedValue interface{}
    for i, v := range keyValues {
        expectedKeyValues[i] = v
        if expectedFirstMatchedValue == nil && v % 2 == 0 {
            expectedFirstMatchedValue = v
        }
    }
    return expectedKeyValues, expectedFirstMatchedValue
}
