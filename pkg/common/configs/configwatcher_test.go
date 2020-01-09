/*
Copyright 2020 Cloudera, Inc.  All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package configs

import (
	"fmt"
	"testing"
	"time"

	"gotest.tools/assert"

	"github.com/cloudera/yunikorn-core/pkg/common"
)

// test singleton
func TestGetConfigWatcher(t *testing.T) {
	cw0 := GetInstance()
	cw1 := GetInstance()
	assert.Equal(t, cw0, cw1)
}

type FakeConfigReloader struct {
	timesOfReload int
}

func (r *FakeConfigReloader) DoReloadConfiguration() error {
	r.timesOfReload++
	fmt.Printf("reload configuration")
	return nil
}

// this test simulates a file stays same for some time and then changes,
// it verifies the callback is not triggered until file state changes.
func TestTriggerCallback(t *testing.T) {
	var timesOfChecksum int
	// init context
	ConfigContext.Set("p-group", &SchedulerConfig{Checksum: []byte("abc")})
	SchedulerConfigLoader = func(policyGroup string) (config *SchedulerConfig, e error) {
		timesOfChecksum++
		return &SchedulerConfig{Checksum: []byte("abc")}, nil
	}

	// the original Checksum
	cw := CreateConfigWatcher("rm-id", "p-group", 3*time.Second)
	reloader := &FakeConfigReloader{}
	cw.RegisterCallback(reloader)

	// verify initial fields are correct
	assert.Equal(t, cw.rmID, "rm-id")
	assert.Equal(t, cw.policyGroup, "p-group")
	assert.Assert(t, cw.reloader != nil)

	// only run once
	cw.runOnce()

	// verify version is not changed
	assert.Equal(t, timesOfChecksum, 1)
	assert.Equal(t, reloader.timesOfReload, 0)

	// simulate file state changes
	SchedulerConfigLoader = func(policyGroup string) (config *SchedulerConfig, e error) {
		timesOfChecksum++
		return &SchedulerConfig{Checksum: []byte("bcd")}, nil
	}

	cw.runOnce()

	// verify when config state is changed,
	// callback is called and version is updated in config watcher
	assert.Equal(t, timesOfChecksum, 2)
	assert.Equal(t, reloader.timesOfReload, 1)
}

func TestRegister(t *testing.T) {
	SchedulerConfigLoader = func(policyGroup string) (config *SchedulerConfig, e error) {
		return nil, fmt.Errorf("error")
	}

	cw := CreateConfigWatcher("rm-id", "p-group", 3*time.Second)
	reloader := &FakeConfigReloader{}
	cw.RegisterCallback(reloader)

	assert.Equal(t, cw.reloader, reloader)
}

func TestChecksumFailure(t *testing.T) {
	// reset configWatcher before each test
	configWatcher = nil
	MockSchedulerConfigByData([]byte("abc"))

	cw := CreateConfigWatcher("rm-id", "p-group", 3*time.Second)
	reloader := &FakeConfigReloader{}
	cw.RegisterCallback(reloader)

	// verify initial fields are correct
	assert.Equal(t, cw.rmID, "rm-id")
	assert.Equal(t, cw.policyGroup, "p-group")
	assert.Assert(t, cw.reloader != nil)

	// simulate failed to parse configuration version
	SchedulerConfigLoader = func(policyGroup string) (config *SchedulerConfig, e error) {
		return nil, fmt.Errorf("error")
	}

	// verify callback is not called due to the failure
	assert.Equal(t, cw.runOnce(), false)
	assert.Equal(t, reloader.timesOfReload, 0)
}

func TestConfigWatcherExpiration(t *testing.T) {
	// init conf
	ConfigContext.Set("p-group", &SchedulerConfig{Checksum: []byte("abc")})
	// simulate configuration never changes
	SchedulerConfigLoader = func(policyGroup string) (config *SchedulerConfig, e error) {
		return &SchedulerConfig{Checksum: []byte("abc")}, nil
	}
	cw := CreateConfigWatcher("rm-id", "p-group", 2*time.Second)
	cw.Run()

	// short run, after 2 seconds, it should be stopped
	assert.Assert(t, waitForStopped(cw) == nil)

	// start again
	cw.Run()
	assert.Assert(t, waitForStarted(cw) == nil)
}

func waitForStarted(cw *ConfigWatcher) error {
	return common.WaitFor(1*time.Second, 5*time.Second, func() bool {
		// at most 1 element in solo chan means go routine is running
		return len(cw.soloChan) == 1
	})
}

func waitForStopped(cw *ConfigWatcher) error {
	return common.WaitFor(1*time.Second, 5*time.Second, func() bool {
		// no element in the solo chan means go routine is not running
		return len(cw.soloChan) == 0
	})
}
