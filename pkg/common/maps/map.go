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
