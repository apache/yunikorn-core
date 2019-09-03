/*
Copyright 2019 Cloudera, Inc.  All rights reserved.

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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"testing"
)

func TestIsNopLogger(t *testing.T) {
	var logger *zap.Logger

	logger, _ = zap.NewDevelopment()
	assert.Equal(t, false, isNopLogger(logger))

	logger = zap.NewNop()
	assert.Equal(t, true, isNopLogger(logger))

	logger = zap.L()
	assert.Equal(t, true, isNopLogger(logger))

	logger, _ = zap.NewProduction()
	zap.ReplaceGlobals(logger)
	assert.Equal(t, false, isNopLogger(logger))
	assert.Equal(t, false, isNopLogger(zap.L()))
}

func TestIsDebugEnabled(t *testing.T) {
	zapConfigs := zap.Config{
		Level: zap.NewAtomicLevelAt(zapcore.DebugLevel),
		Encoding: "console",
	}
	if newLogger, err := zapConfigs.Build(); err != nil {
		assert.Fail(t, err.Error())
	} else {
		logger = newLogger
		assert.Equal(t, true, IsDebugEnabled())
	}

	zapConfigs = zap.Config{
		Level: zap.NewAtomicLevelAt(zapcore.InfoLevel),
		Encoding: "console",
	}
	if newLogger, err := zapConfigs.Build(); err != nil {
		assert.Fail(t, err.Error())
	} else {
		logger = newLogger
		assert.Equal(t, false, IsDebugEnabled())
	}
}