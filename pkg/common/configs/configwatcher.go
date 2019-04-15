/*
Copyright 2019 The Unity Scheduler Authors

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
	"bytes"
	"errors"
	"github.com/golang/glog"
	"sync"
	"time"
)

var configWatcher *ConfigWatcher
var once sync.Once

// Config watcher watches on a configuration file,
// it is initiated with a certain expiration time, it will stop running either
// it detects configuration changes, or the expiration time reaches
type ConfigWatcher struct {
	currentCheckSum []byte
	filePath        string
	callbacks       chan func()
	expireTime      time.Duration
	stopChan        chan int
	lock            *sync.Mutex
}

func GetConfigWatcher(checkSum []byte, file string) *ConfigWatcher {
	// singleton
	once.Do(func() {
		newInstance(checkSum, file)
	})

	// update checksum if needed
	if !bytes.Equal(checkSum, configWatcher.currentCheckSum) {
		configWatcher.lock.Lock()
		defer configWatcher.lock.Unlock()
		configWatcher.currentCheckSum = checkSum
	}

	return configWatcher
}

// visible for testing
func newInstance(checkSum []byte, file string) *ConfigWatcher {
	configWatcher = &ConfigWatcher{
		currentCheckSum: checkSum,
		filePath:        file,
		callbacks:       make(chan func(), 100),
		expireTime:      60 * time.Second,
		lock:            &sync.Mutex{},
	}

	// start the watcher only during the initialization
	go configWatcher.run()
	return configWatcher
}

func (cw *ConfigWatcher) run() {
	for {
		select {
		case callback := <-configWatcher.callbacks:
			// for each callback, it ensures it gets processed before handling next
			if configWatcher.waitForStateChange() {
				// invoke callback once state changes
				callback()
			}
		case <-cw.stopChan:
			return
		}
	}
}

func (cw *ConfigWatcher) stop() {
	cw.stopChan <- 0
}

func (cw *ConfigWatcher) AddCallback(callback func()) error {
	cw.lock.Lock()
	defer cw.lock.Unlock()

	select {
	case cw.callbacks <- callback:
		return nil
	default:
		return errors.New("failed to add callback to configWatcher")
	}
}

func (cw *ConfigWatcher) waitForStateChange() bool {
	deadline := time.Now().Add(cw.expireTime)
	for {
		sum, err := FileCheckSummer(cw.filePath)
		if err != nil {
			// failed to calculate check sum, retry...
			glog.V(1).Infof("encounter error while calculating checksum for file %s," +
				" ignore reloading configuration to avoid loading malicious configuration",
				cw.filePath)
			return false
		}

		if time.Now().After(deadline) {
			glog.V(1).Infof("config watcher expired, configuration reload attempt is aborted")
			return false
		}

		// acquire the lock to avoid checksum changed externally
		cw.lock.Lock()
		same := bytes.Equal(sum, cw.currentCheckSum)
		cw.lock.Unlock()
		if same {
			// check sum equals, file not changed
			glog.V(1).Infof("configuration file state is not unchanged, checksum: %s",
				string(cw.currentCheckSum))
			time.Sleep(1 * time.Second)
			continue
		}

		return true
	}
}