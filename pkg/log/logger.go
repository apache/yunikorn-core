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
var aLevel *zap.AtomicLevel

type overriddenCore struct {
	aLevel *zap.AtomicLevel
	zapcore.Core
}

func (oc overriddenCore) Enabled(level zapcore.Level) bool {
	return oc.aLevel.Enabled(level)
}

func (oc overriddenCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if oc.aLevel.Enabled(ent.Level) {
		return ce.AddCore(ent, oc)
	}
	return ce
}

// workaround since no clean way to obtain a Level from a zap.Logger
func obtainLevel(core zapcore.Core) zapcore.Level {
	if core.Enabled(zapcore.DebugLevel) {
		return zapcore.DebugLevel
	} else if core.Enabled(zapcore.InfoLevel) {
		return zapcore.InfoLevel
	} else if core.Enabled(zapcore.WarnLevel) {
		return zapcore.WarnLevel
	} else if core.Enabled(zapcore.ErrorLevel) {
		return zapcore.ErrorLevel
	} else if core.Enabled(zapcore.DPanicLevel) {
		return zapcore.DPanicLevel
	} else if core.Enabled(zapcore.PanicLevel) {
		return zapcore.PanicLevel
	} else if core.Enabled(zapcore.FatalLevel) {
		return zapcore.FatalLevel
	} else {
		// TODO maybe panic here?
		return zapcore.InfoLevel
	}
}

func Logger() *zap.Logger {
	once.Do(func() {
		if logger = zap.L(); isNopLogger(logger) {
			// If a global logger is not found, this could be either scheduler-core
			// is running as a deployment mode, or running with another non-go code
			// shim. In this case, we need to create our own logger.
			// TODO support log options when a global logger is not there
			config = createConfig()
			var err error
			logger, err = config.Build()
			// this should really not happen so just write to stdout and set a Nop logger
			if err != nil {
				fmt.Printf("Logging disabled, logger init failed with error: %v\n", err)
				logger = zap.NewNop()
			}
		}

		// wrapping the logger core to have control over log level
		aLevelInstance := zap.NewAtomicLevelAt(obtainLevel(logger.Core()))
		aLevel = &aLevelInstance
		logger = logger.WithOptions(zap.WrapCore(
			func(core zapcore.Core) zapcore.Core {
				return overriddenCore{
					aLevel,
					core,
				}
			}))

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

func GetAtomicLevel() *zap.AtomicLevel {
	return aLevel
}

// Create a log config to keep full control over
// LogLevel set to DEBUG, Encodes for console, Writes to stderr,
// Enables development mode (DPanicLevel),
// Print stack traces for messages at WarnLevel and above
func createConfig() *zap.Config {
	return &zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.DebugLevel),
		Development: true,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:    "message",
			LevelKey:      "level",
			TimeKey:       "time",
			NameKey:       "name",
			CallerKey:     "caller",
			StacktraceKey: "stacktrace",
			LineEnding:    zapcore.DefaultLineEnding,
			// note: https://godoc.org/go.uber.org/zap/zapcore#EncoderConfig
			// only EncodeName is optional all others must be set
			EncodeLevel:    zapcore.CapitalLevelEncoder,
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
}
