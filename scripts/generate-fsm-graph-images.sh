#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

WORKDIR=$(pwd)/build/fsm

output_fsm() {
  # print digraph header
   head -n +1 "$1.dot"
  # add options
  echo "concentrate=true"
  # print rest of file, eliminating transitions from same state to same state
  # and cleaning up some verbose labels
  tail -n +2 "$1.dot" | \
	grep -E -v '"(\w+)" -> "\1"' | \
	grep -v AppAllocationAsk | \
	sed "s/Application/ App/g"
}

cd "${WORKDIR}"

for dot in *.dot; do
  # shellcheck disable=SC2001
  base=$(echo "${dot}" | sed "s/\.dot$//")
  
  output_fsm "${base}" | dot -Tpng > "${base}.png"
done

