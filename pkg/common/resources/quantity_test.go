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

package resources

import (
	"testing"

	"gotest.tools/assert"
)

func TestParseQuantity(t *testing.T) {
	tests := map[string]struct {
		input string
		qty   Quantity
		err   string
	}{
		"0":          {input: "0", qty: 0},
		"1":          {input: "1", qty: 1},
		"-1":         {input: "-1", qty: -1},
		"spaces":     {input: " 25k ", qty: 25 * 1000},
		"min":        {input: "-9223372036854775808", qty: -9223372036854775808},
		"max":        {input: "9223372036854775807", qty: 9223372036854775807},
		"overflow":   {input: "9223372036854775808", qty: 0, err: "overflow"},
		"underflow":  {input: "-9223372036854775809", qty: 0, err: "overflow"},
		"overflow2":  {input: "1000E", qty: 0, err: "overflow"},
		"underflow2": {input: "-1000E", qty: 0, err: "overflow"},
		"wrong unit": {input: "5X", qty: 0, err: "invalid"},
		"milli":      {input: "500m", qty: 0, err: "invalid"},
		"2k":         {input: "2k", qty: 2 * 1000},
		"-2k":        {input: "-2k", qty: -2 * 1000},
		"3M":         {input: "3M", qty: 3 * 1000 * 1000},
		"-3M":        {input: "-3M", qty: -3 * 1000 * 1000},
		"4G":         {input: "4G", qty: 4 * 1000 * 1000 * 1000},
		"-4G":        {input: "-4G", qty: -4 * 1000 * 1000 * 1000},
		"5T":         {input: "5T", qty: 5 * 1000 * 1000 * 1000 * 1000},
		"-5T":        {input: "-5T", qty: -5 * 1000 * 1000 * 1000 * 1000},
		"6P":         {input: "6P", qty: 6 * 1000 * 1000 * 1000 * 1000 * 1000},
		"-6P":        {input: "-6P", qty: -6 * 1000 * 1000 * 1000 * 1000 * 1000},
		"7E":         {input: "7E", qty: 7 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000},
		"-7E":        {input: "-7E", qty: -7 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000},
		"2Ki":        {input: "2Ki", qty: 2 * 1024},
		"-2Ki":       {input: "-2Ki", qty: -2 * 1024},
		"3Mi":        {input: "3Mi", qty: 3 * 1024 * 1024},
		"-3Mi":       {input: "-3Mi", qty: -3 * 1024 * 1024},
		"4Gi":        {input: "4Gi", qty: 4 * 1024 * 1024 * 1024},
		"-4Gi":       {input: "-4Gi", qty: -4 * 1024 * 1024 * 1024},
		"5Ti":        {input: "5Ti", qty: 5 * 1024 * 1024 * 1024 * 1024},
		"-5Ti":       {input: "-5Ti", qty: -5 * 1024 * 1024 * 1024 * 1024},
		"6Pi":        {input: "6Pi", qty: 6 * 1024 * 1024 * 1024 * 1024 * 1024},
		"-6Pi":       {input: "-6Pi", qty: -6 * 1024 * 1024 * 1024 * 1024 * 1024},
		"7Ei":        {input: "7Ei", qty: 7 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024},
		"-7Ei":       {input: "-7Ei", qty: -7 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := ParseQuantity(test.input)
			if test.err != "" {
				assert.ErrorContains(t, err, test.err, "error expected")
			} else {
				assert.NilError(t, err, "no error expected")
				assert.Equal(t, result, test.qty, "wrong result")
			}
		})
	}
}
func TestParseVCore(t *testing.T) {
	tests := map[string]struct {
		input string
		qty   Quantity
		err   string
	}{
		"0":          {input: "0", qty: 0},
		"1":          {input: "1", qty: 1000},
		"-1":         {input: "-1", qty: -1000},
		"spaces":     {input: " 25k ", qty: 25 * 1000 * 1000},
		"min":        {input: "-9223372036854775", qty: -9223372036854775000},
		"max":        {input: "9223372036854775", qty: 9223372036854775000},
		"overflow":   {input: "9223372036854776", qty: 0, err: "overflow"},
		"underflow":  {input: "-9223372036854776", qty: 0, err: "overflow"},
		"overflow2":  {input: "1000P", qty: 0, err: "overflow"},
		"underflow2": {input: "-1000P", qty: 0, err: "overflow"},
		"wrong unit": {input: "5X", qty: 0, err: "invalid"},
		"500m":       {input: "500m", qty: 500},
		"2k":         {input: "2k", qty: 2 * 1000 * 1000},
		"-2k":        {input: "-2k", qty: -2 * 1000 * 1000},
		"3M":         {input: "3M", qty: 3 * 1000 * 1000 * 1000},
		"-3M":        {input: "-3M", qty: -3 * 1000 * 1000 * 1000},
		"4G":         {input: "4G", qty: 4 * 1000 * 1000 * 1000 * 1000},
		"-4G":        {input: "-4G", qty: -4 * 1000 * 1000 * 1000 * 1000},
		"5T":         {input: "5T", qty: 5 * 1000 * 1000 * 1000 * 1000 * 1000},
		"-5T":        {input: "-5T", qty: -5 * 1000 * 1000 * 1000 * 1000 * 1000},
		"6P":         {input: "6P", qty: 6 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000},
		"-6P":        {input: "-6P", qty: -6 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000},
		"7E":         {input: "7E", qty: 0, err: "overflow"},
		"-7E":        {input: "-7E", qty: 0, err: "overflow"},
		"2Ki":        {input: "2Ki", qty: 2 * 1000 * 1024},
		"-2Ki":       {input: "-2Ki", qty: -2 * 1000 * 1024},
		"3Mi":        {input: "3Mi", qty: 3 * 1000 * 1024 * 1024},
		"-3Mi":       {input: "-3Mi", qty: -3 * 1000 * 1024 * 1024},
		"4Gi":        {input: "4Gi", qty: 4 * 1000 * 1024 * 1024 * 1024},
		"-4Gi":       {input: "-4Gi", qty: -4 * 1000 * 1024 * 1024 * 1024},
		"5Ti":        {input: "5Ti", qty: 5 * 1000 * 1024 * 1024 * 1024 * 1024},
		"-5Ti":       {input: "-5Ti", qty: -5 * 1000 * 1024 * 1024 * 1024 * 1024},
		"6Pi":        {input: "6Pi", qty: 6 * 1000 * 1024 * 1024 * 1024 * 1024 * 1024},
		"-6Pi":       {input: "-6Pi", qty: -6 * 1000 * 1024 * 1024 * 1024 * 1024 * 1024},
		"7Ei":        {input: "7Ei", qty: 0, err: "overflow"},
		"-7Ei":       {input: "-7Ei", qty: 0, err: "overflow"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := ParseVCore(test.input)
			if test.err != "" {
				assert.ErrorContains(t, err, test.err, "error expected")
			} else {
				assert.NilError(t, err, "no error expected")
				assert.Equal(t, result, test.qty, "wrong result")
			}
		})
	}
}
