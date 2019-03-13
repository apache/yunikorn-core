/*
Copyright 2019 The Unity Scheduler Authors

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

package resources

func MockResource(nums ...Quantity) *Resource {
    res := NewResource()
    for i, num := range nums {
        if num == 0 {
            continue
        }
        res.Resources[string(97+i)] = num
    }
    return res
}

func CompareMockResource(left *Resource, nums ...Quantity) bool {
    right := MockResource(nums...)
    return Equals(left, right)
}
