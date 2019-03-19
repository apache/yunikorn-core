#
# opyright 2019 The Unity Scheduler Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

REPO=github.infra.cloudera.com/yunikorn/yunikorn-core/pkg

.PHONY: all simplescheduler

all: simplescheduler schedulerclient

test:
	go test ./... -cover
	go vet $(REPO)...

simplescheduler:
	if [ ! -d ./vendor ]; then dep ensure -vendor-only; fi
	CGO_ENABLED=0 GOOS=darwin go build -a -ldflags '-extldflags "-static"' -o _output/simplescheduler ./cmd/simplescheduler

schedulerclient:
	if [ ! -d ./vendor ]; then dep ensure -vendor-only; fi
	CGO_ENABLED=0 GOOS=darwin go build -a -ldflags '-extldflags "-static"' -o _output/schedulerclient ./cmd/schedulerclient

clean:
	go clean -r -x
	-rm -rf _output



