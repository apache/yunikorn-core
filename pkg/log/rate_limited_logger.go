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
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type rateLimitedLogger struct {
	logger  *zap.Logger
	limiter *rate.Limiter
}

// RateLimitedLogger provides a logger that only logs once within a specified duration
func RateLimitedLog(handle *LoggerHandle, every time.Duration) *rateLimitedLogger {
	return &rateLimitedLogger{
		logger:  Log(handle),
		limiter: rate.NewLimiter(rate.Every(every), 1),
	}
}

func (rl *rateLimitedLogger) Debug(msg string, fields ...zap.Field) {
	if rl.limiter.Allow() {
		rl.logger.Debug(msg, fields...)
	}
}

func (rl *rateLimitedLogger) Info(msg string, fields ...zap.Field) {
	if rl.limiter.Allow() {
		rl.logger.Info(msg, fields...)
	}
}

func (rl *rateLimitedLogger) Warn(msg string, fields ...zap.Field) {
	if rl.limiter.Allow() {
		rl.logger.Warn(msg, fields...)
	}
}

func (rl *rateLimitedLogger) Error(msg string, fields ...zap.Field) {
	if rl.limiter.Allow() {
		rl.logger.Error(msg, fields...)
	}
}

func (rl *rateLimitedLogger) DPanic(msg string, fields ...zap.Field) {
	if rl.limiter.Allow() {
		rl.logger.DPanic(msg, fields...)
	}
}

func (rl *rateLimitedLogger) Panic(msg string, fields ...zap.Field) {
	if rl.limiter.Allow() {
		rl.logger.Panic(msg, fields...)
	}
}

func (rl *rateLimitedLogger) Fatal(msg string, fields ...zap.Field) {
	if rl.limiter.Allow() {
		rl.logger.Fatal(msg, fields...)
	}
}
