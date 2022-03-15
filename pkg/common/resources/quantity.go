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
	"errors"
	"math/big"
	"regexp"
	"strconv"
)

// This code handles parsing of SI units in quantities:
// <quantity>     ::= <signedNumber><suffix>
// <signedNumber> ::= <digits> | <sign><digits>
// <sign>         ::= "+" | "-"
// <digit>        ::= 0 | 1 | ... | 9
// <digits>       ::= <digit> | <digit><digits>
// <suffix>       ::= <binarySI> | <decimalSI>
// <binarySI>     ::= Ki | Mi | Gi | Ti | Pi | Ei
// <decimalSI>    ::= "" | k | K | M | G | T | P | E
// Additionally, ParseVCore supports decimalSI of 'm' to indicate millicore.

var whitespace = regexp.MustCompile(`\s+`)
var legal = regexp.MustCompile(`^(?P<Number>[+-]?[0-9]+)(?P<Suffix>[A-Za-z]*)$`)

var multipliers = map[string]int64{
	"":   1,
	"m":  1, // special handling if milli is in use
	"k":  1e3,
	"M":  1e6,
	"G":  1e9,
	"T":  1e12,
	"P":  1e15,
	"E":  1e18,
	"Ki": 1 << 10,
	"Mi": 1 << 20,
	"Gi": 1 << 30,
	"Ti": 1 << 40,
	"Pi": 1 << 50,
	"Ei": 1 << 60,
}

// ParseQuantity is used to parse user-provided values into int64 quantities.
func ParseQuantity(value string) (Quantity, error) {
	return parse(value, false)
}

// ParseVCore is similar to ParseQuantity but allows the 'm' suffix. Additionally, the base unit returned is a
// millicore, so values without units will be converted to milliCPUs (i.e. '10' will result in 10000, and '500m' will
// result in 500).
func ParseVCore(value string) (Quantity, error) {
	return parse(value, true)
}

func parse(value string, milli bool) (Quantity, error) {
	value = whitespace.ReplaceAllLiteralString(value, "")

	parts := legal.FindStringSubmatch(value)
	if parts == nil || len(parts) != 3 {
		return 0, errors.New("invalid quantity")
	}
	number := parts[1]
	suffix := parts[2]

	result, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return 0, errors.New("invalid quantity: overflow")
	}

	scale, ok := multipliers[suffix]
	if !ok || (suffix == "m" && !milli) {
		return 0, errors.New("invalid suffix")
	}

	bigResult := big.NewInt(result)
	bigScale := big.NewInt(scale)
	bigResult = bigResult.Mul(bigResult, bigScale)
	if milli && suffix != "m" {
		bigResult.Mul(bigResult, big.NewInt(1000))
	}
	if !bigResult.IsInt64() {
		return 0, errors.New("invalid quantity: overflow")
	}
	result = bigResult.Int64()

	return Quantity(result), nil
}
