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

package configs

import (
	"bytes"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/apache/incubator-yunikorn-core/pkg/log"
)

var configWatcher *ConfigWatcher
var once sync.Once

const (
	DefaultConfigWatcherDuration = 2 * time.Minute
)

// Config watcher watches on a configuration file,
// it is initiated with a certain expiration time, it will stop running either
// it detects configuration changes, or the expiration time reaches
type ConfigWatcher struct {
	rmID        string
	policyGroup string
	reloader    ConfigReloader
	expireTime  time.Duration
	soloChan    chan interface{}
	lock        *sync.Mutex
}

// interface for the actual reload function
type ConfigReloader interface {
	DoReloadConfiguration() error
}

func CreateConfigWatcher(rmID string, policyGroup string, expiration time.Duration) *ConfigWatcher {
	return &ConfigWatcher{
		rmID:        rmID,
		policyGroup: policyGroup,
		expireTime:  expiration,
		soloChan:    make(chan interface{}, 1),
		lock:        &sync.Mutex{},
	}
}

func GetInstance() *ConfigWatcher {
	// singleton
	once.Do(func() {
		configWatcher = &ConfigWatcher{
			expireTime: DefaultConfigWatcherDuration,
			lock:       &sync.Mutex{},
		}
	})

	return configWatcher
}

func (cw *ConfigWatcher) RegisterCallback(reloader ConfigReloader) {
	cw.lock.Lock()
	defer cw.lock.Unlock()
	cw.reloader = reloader
}

// returns true if config file state remains same,
// returns false if config file state changes
func (cw *ConfigWatcher) runOnce() bool {
	// acquire the lock to avoid Checksum changed externally
	cw.lock.Lock()
	defer cw.lock.Unlock()

	newConfig, err := SchedulerConfigLoader(cw.policyGroup)
	if err != nil {
		log.Logger().Warn("failed to calculate the checksum of configuration file for policyGroup,"+
			"ignore reloading configuration", zap.String("policyGroup", cw.policyGroup))
		return false
	}

	if bytes.Equal(newConfig.Checksum[:], ConfigContext.Get(cw.policyGroup).Checksum[:]) {
		// check sum equals, file not changed
		time.Sleep(1 * time.Second)
		return true
	}
	// when detect state changes, trigger the reload function
	log.Logger().Debug("configuration file changed")
	if err = cw.reloader.DoReloadConfiguration(); err == nil {
		log.Logger().Debug("configuration is successfully reloaded")
	}
	return false
}

var timer *time.Timer
// if configWatcher is not running, kick-off running it
// if configWatcher is already running, this is a noop
func (cw *ConfigWatcher) Run() {
	select {
	case cw.soloChan <- 0:
		ticker := time.NewTicker(1 * time.Second)
		quit := make(chan bool)
		go func() {
			for {
				select {
				case <-quit:
					<-cw.soloChan
					ticker.Stop()
					return
				default:
					break
				}
				select {
				case <-ticker.C:
					if !cw.runOnce() {
						<-cw.soloChan
						return
					}
				case <-quit:
					<-cw.soloChan
					ticker.Stop()
					return
				}
			}
		}()

		timer = time.AfterFunc(cw.expireTime, func() {
			log.Logger().Info("config watcher timed out")
			quit <- true
		})
	default:
		if timer != nil {
			log.Logger().Info("config watcher is already running. Extending config watcher duration")
			timer.Reset(cw.expireTime)
		} else {
			log.Logger().Warn("config watcher not initialized")
		}
	}
}
