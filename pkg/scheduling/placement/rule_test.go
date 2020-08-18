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

package placement

import (
	"testing"

	"gotest.tools/assert"

	"github.com/apache/incubator-yunikorn-core/pkg/cache"
	"github.com/apache/incubator-yunikorn-core/pkg/common/configs"
)

func TestNewRule(t *testing.T) {
	conf := configs.PlacementRule{
		Name: "bogus",
	}
	nr, err := newRule(conf)
	if err == nil || nr != nil {
		t.Errorf("new newRule create did not fail with bogus newRule name, err 'nil' , newRule: %v, ", nr)
	}

	conf = configs.PlacementRule{
		Name: "test",
	}
	nr, err = newRule(conf)
	if err != nil || nr == nil {
		t.Errorf("new newRule build failed which should not, newRule 'nil' , err: %v, ", err)
	}

	// test normalise newRule name
	conf = configs.PlacementRule{
		Name: "TeSt",
	}
	nr, err = newRule(conf)
	if err != nil || nr == nil {
		t.Errorf("new normalised newRule build failed which should not, newRule 'nil' , err: %v, ", err)
	}
}

// Test for a basic test rule.
func TestPlaceApp(t *testing.T) {
	conf := configs.PlacementRule{
		Name: "test",
	}
	nr, err := newRule(conf)
	assert.NilError(t, err, "unexpected rule initialisation error")
	// place application that should fail
	_, err = nr.placeApplication(nil, nil)
	if err == nil {
		t.Error("test rule place application did not fail as expected")
	}
	var queue string
	// place application that should not fail and return "test"
	queue, err = nr.placeApplication(&cache.ApplicationInfo{}, nil)
	if err != nil || queue != "test" {
		t.Errorf("test rule place application did not fail, err: %v, ", err)
	}
	// place application that should not fail and return the queue in the object
	queue, err = nr.placeApplication(&cache.ApplicationInfo{QueueName: "passedin"}, nil)
	if err != nil || queue != "passedin" {
		t.Errorf("test rule place application did not fail, err: %v, ", err)
	}
	// place application that should not fail and return the queue in the object
	queue, err = nr.placeApplication(&cache.ApplicationInfo{QueueName: "user.name"}, nil)
	if err != nil || queue != "user_dot_name" {
		t.Errorf("test rule place application did not fail, err: %v, ", err)
	}
}

func TestReplaceDot(t *testing.T) {
	name := replaceDot("name.name")
	if name != "name_dot_name" {
		t.Errorf("replace dot failed, name: %s, ", name)
	}
	name = replaceDot("name...name")
	if name != "name_dot__dot__dot_name" {
		t.Errorf("replace consecutive dots failed, name: %s, ", name)
	}
	name = replaceDot("name.name.name")
	if name != "name_dot_name_dot_name" {
		t.Errorf("replace multi dots failed, name: %s, ", name)
	}
	name = replaceDot(".name.")
	if name != "_dot_name_dot_" {
		t.Errorf("replace start or end dots failed, name: %s, ", name)
	}
}

func CreatePartitionInfo(data []byte) (*cache.PartitionInfo, error) {
	return cache.CreatePartitionInfo(data)
}
