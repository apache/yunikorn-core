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

package locking

// The methods below simply forward to the wrapped go-deadlock lock. They exist so that the
// gVisor checklocks analysis (see the "vetlock" make target) tracks the locking.Mutex and
// locking.RWMutex fields of a struct directly. Calling a promoted method of the wrapped lock
// instead makes the analysis attribute the acquisition to the inner field (i.e. "lock.mu"
// rather than "lock") and every "+checklocks:" field annotation fails to match.
//
// The go-deadlock lock is held in an unexported named field rather than embedded on purpose.
// Embedding promotes the methods this file does not forward, TryLock on both types plus
// TryRLock and RLocker on RWMutex. Those would silently bypass the forwarding again: the
// analysis would attribute them to the inner field, and the sync.Locker returned by RLocker
// is not tracked at all. With a named field any use of them is a compile error until a
// forwarder is added here deliberately.
//
// The forwarders need nothing of their own from the analysis: the wrapper types declare
// themselves lock primitives (see locking.go), so a call to one is intercepted at the call
// site exactly as a call to the wrapped lock would be and the body is not analysed, a lock
// implementation not being a critical section. Until that declaration existed every forwarder
// carried a "+checklocksignore" instead, to silence the balance error a method that takes a
// lock and returns holding it produces. Those ignores are gone, and with them the checking
// they cost: an ignore is read at every call site, not only in the function that carries it,
// so a single ignore per forwarder suppressed the per call site "already locked" and "unlock
// without lock" diagnostics for every wrapper lock in the code base. That class is checked
// again, statically as well as by the go-deadlock detection at runtime. The rest of the lock
// state tracking never depended on this: guarded field access, the lock requirements of
// annotated functions and the return balance of the calling function were all still checked,
// the last of which is what reports an unlock-relock gap.
//
// The forwarders shift the go-deadlock reports by one stack frame: the "<<<<<" marker points
// at the forwarder in this file and the calling code is one frame further down. They are
// fully inlined so there is no runtime cost.

func (m *Mutex) Lock() {
	m.mu.Lock()
}

func (m *Mutex) Unlock() {
	m.mu.Unlock()
}

func (m *RWMutex) Lock() {
	m.mu.Lock()
}

func (m *RWMutex) Unlock() {
	m.mu.Unlock()
}

func (m *RWMutex) RLock() {
	m.mu.RLock()
}

func (m *RWMutex) RUnlock() {
	m.mu.RUnlock()
}
