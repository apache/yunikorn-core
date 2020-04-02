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
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gotest.tools/assert"
)

// This test sets the global zap logger. This must be undone to make sure no side
// effects on other tests are caused by running this test.
func TestIsNopLogger(t *testing.T) {
	// reset the global vars and zap logger
	defer resetGlobals()

	testLogger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Dev logger init failed with error: %v", err)
	}
	assert.Equal(t, false, isNopLogger(testLogger))

	testLogger = zap.NewNop()
	assert.Equal(t, true, isNopLogger(testLogger))

	testLogger = zap.L()
	assert.Equal(t, true, isNopLogger(testLogger))

	testLogger, err = zap.NewProduction()
	if err != nil {
		t.Fatalf("Prod logger init failed with error: %v", err)
	}
	zap.ReplaceGlobals(testLogger)
	assert.Equal(t, false, isNopLogger(testLogger))
	assert.Equal(t, false, isNopLogger(zap.L()))
}

// Since we test the function IsDebugEnabled() we set the logger global var.
// It has not triggered the once.Do() so we just need to make sure we clean up the
// global var.
func TestIsDebugEnabled(t *testing.T) {
	// reset the global vars and zap logger
	defer resetGlobals()

	zapConfig := zap.Config{
		Level:    zap.NewAtomicLevelAt(zapcore.DebugLevel),
		Encoding: "console",
	}
	var err error
	logger, err = zapConfig.Build()
	assert.NilError(t, err, "debug level logger create failed")
	assert.Equal(t, true, IsDebugEnabled())

	zapConfig = zap.Config{
		Level:    zap.NewAtomicLevelAt(zapcore.InfoLevel),
		Encoding: "console",
	}
	logger, err = zapConfig.Build()
	assert.NilError(t, err, "info level logger create failed")
	assert.Equal(t, false, IsDebugEnabled())
}

// reset the global vars and the global logger in zap
func resetGlobals() {
	logger = nil
	config = nil
	zap.ReplaceGlobals(zap.NewNop())
}

// This test triggers the once.Do() and will have an impact on other tests in this file.
// resetGlobals() will not undo the impact this test has.
func TestCreateConfig(t *testing.T) {
	// direct call
	zapConfig := createConfig()
	localLogger, err := zapConfig.Build()
	assert.NilError(t, err, "default config logger create failed")
	assert.Equal(t, true, localLogger.Core().Enabled(zap.DebugLevel))

	// indirect call to init logger
	assert.Assert(t, logger == nil, "global logger should not have been set %v", logger)
	localLogger = Logger()
	assert.Assert(t, localLogger != nil, "returned logger should have been not nil")
	// default log level is debug
	assert.Equal(t, true, IsDebugEnabled())
	// change log level to info
	InitAndSetLevel(zap.InfoLevel)
	assert.Equal(t, false, IsDebugEnabled())
}
