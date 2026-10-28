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
	"go.uber.org/zap/zaptest/observer"
	"gotest.tools/v3/assert"
)

func TestFilteredCoreWith(t *testing.T) {
	inner, logs := observer.New(zapcore.DebugLevel)
	core := filteredCore{level: zapcore.WarnLevel, inner: inner}

	child := core.With([]zapcore.Field{zap.String("key", "value")})
	assert.Assert(t, !child.Enabled(zapcore.InfoLevel), "child core must keep the filter level")
	assert.Assert(t, child.Enabled(zapcore.WarnLevel), "child core must allow the filter level")
	assert.Assert(t, child.Check(zapcore.Entry{Level: zapcore.InfoLevel}, nil) == nil, "child core must filter entries below the level")
	assert.Assert(t, child.Check(zapcore.Entry{Level: zapcore.WarnLevel}, nil) != nil, "child core must pass entries at the level")

	log := zap.New(core).With(zap.String("key", "value"))
	log.Info("filtered")
	assert.Equal(t, 0, logs.Len(), "info message should be filtered")
	log.Warn("logged")
	assert.Equal(t, 1, logs.Len(), "warn message should be logged")
	entry := logs.All()[0]
	assert.Equal(t, "logged", entry.Message)
	assert.Equal(t, "value", entry.ContextMap()["key"])
}

func TestFilteredCoreWrite(t *testing.T) {
	inner, logs := observer.New(zapcore.DebugLevel)
	core := filteredCore{level: zapcore.WarnLevel, inner: inner}

	err := core.Write(zapcore.Entry{Level: zapcore.WarnLevel, Message: "message"}, []zapcore.Field{zap.String("key", "value")})
	assert.NilError(t, err, "write failed")
	assert.Equal(t, 1, logs.Len(), "message should be written to the inner core")
	entry := logs.All()[0]
	assert.Equal(t, "message", entry.Message)
	assert.Equal(t, "value", entry.ContextMap()["key"])
	assert.NilError(t, core.Sync(), "sync failed")
}
