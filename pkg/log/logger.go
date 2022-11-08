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
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var once sync.Once
var holderValue atomic.Value

type loggerHolder struct {
	logger     *zap.Logger
	zapConfigs *zap.Config
}

func Logger() *zap.Logger {
	once.Do(initLogger)
	return holderValue.Load().(loggerHolder).logger
}

func GetConfig() *zap.Config {
	once.Do(initLogger)
	return holderValue.Load().(loggerHolder).zapConfigs
}

func initLogger() {
	var logger *zap.Logger
	var config *zap.Config

	if logger = zap.L(); isNopLogger(logger) {
		// If a global logger is not found, this could be either scheduler-core
		// is running as a deployment mode, or running with another non-go code
		// shim. In this case, we need to create our own logger.
		config = createConfig()
		var err error
		logger, err = config.Build()
		// this should really not happen so just write to stdout and set a Nop logger
		if err != nil {
			fmt.Printf("Logging disabled, logger init failed with error: %v\n", err)
			logger = zap.NewNop()
		}
	}
	holderValue.Store(loggerHolder{
		logger:     logger,
		zapConfigs: config,
	})
}

func InitializeLogger(log *zap.Logger, zapConfig *zap.Config) {
	once.Do(initLogger)
	holderValue.Store(loggerHolder{
		logger:     log,
		zapConfigs: zapConfig,
	})
}

func IsDebugEnabled() bool {
	return Logger().Core().Enabled(zapcore.DebugLevel)
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
	GetConfig().Level.SetLevel(level)
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

func SetLogLevel(newLevel string) error {
	logger := Logger()
	config := GetConfig()

	oldLevel := config.Level.String()

	// noop if the input is the same as what is set
	if newLevel == oldLevel {
		return nil
	}

	logger.Info("Updating log level",
		zap.String("new level", newLevel))
	text := []byte(newLevel)
	if err := config.Level.UnmarshalText(text); err != nil {
		var errorMsg = "failed to change log level, old level active"
		logger.Error(errorMsg, zap.String("loglevel", oldLevel))
		return errors.New(errorMsg)
	}
	logger.Info("Log level updated", zap.String("old level", oldLevel),
		zap.String("new level", newLevel))
	return nil
}
