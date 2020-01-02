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

package log

import (
	"fmt"
	"reflect"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var once sync.Once
var logger *zap.Logger
var config *zap.Config

func Logger() *zap.Logger {
	once.Do(func() {
		if logger = zap.L(); isNopLogger(logger) {
			// If a global logger is not found, this could be either scheduler-core
			// is running as a deployment mode, or running with another non-go code
			// shim. In this case, we need to create our own logger.
			// TODO support log options when a global logger is not there
			c := zap.NewDevelopmentConfig()
			config = &c
			newLogger, err := config.Build()
			// this should really not happen so just write to stdout
			if err != nil {
				fmt.Printf("Logger init error: %v", err)
			}
			logger = newLogger
		}
	})
	return logger
}

func IsDebugEnabled() bool {
	if logger == nil {
		// when under development mode
		return true
	}
	return logger.Core().Enabled(zapcore.DebugLevel)
}

// Returns true if the logger is a noop.
// Logger is a noop means the logger has not been initialized yet.
// This usually means a global logger is not set in the given context,
// see more at zap.ReplaceGlobals(). If a shim presets a global logger in
// the context, yunikorn-core can simply reuse it.
func isNopLogger(logger *zap.Logger) bool {
	return reflect.DeepEqual(zap.NewNop(), logger)
}

// Visible by tests
func InitAndSetLevel(level zapcore.Level) {
	if config == nil {
		Logger()
	}
	config.Level.SetLevel(level)
}
