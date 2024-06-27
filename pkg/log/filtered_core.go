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

import "go.uber.org/zap/zapcore"

type filteredCore struct {
	level zapcore.Level
	inner zapcore.Core
}

var _ zapcore.Core = filteredCore{}

func (f filteredCore) Enabled(level zapcore.Level) bool {
	if level < f.level {
		return false
	}
	return f.inner.Enabled(level)
}

func (f filteredCore) With(fields []zapcore.Field) zapcore.Core {
	return f.inner.With(fields)
}

func (f filteredCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if entry.Level < f.level {
		return ce
	}
	return f.inner.Check(entry, ce)
}

func (f filteredCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return f.inner.Write(entry, fields)
}

func (f filteredCore) Sync() error {
	return f.inner.Sync()
}
