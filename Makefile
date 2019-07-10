#
# Copyright 2019 Cloudera, Inc.  All rights reserved.
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

# Check if this is at least GO 1.11 for Go Modules
GO_VERSION := $(shell go version | awk '$$3 ~ /go1.(10|0-9])/ {print $$3}')
ifdef GO_VERSION
$(error Build requires go 1.11 or later)
endif

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

REPO=github.com/cloudera/yunikorn-core/pkg
# when using the -race option must set CGO_ENBLED to 1
RACE=-race
CGO_ENABLED=1
# build on macos by default
GOOS=darwin

.PHONY: all simplescheduler

all: simplescheduler schedulerclient

test:
	go test ./... -cover $(RACE) -tags deadlock -v
	go vet $(REPO)...

simplescheduler:
	go build $(RACE) -a -ldflags '-extldflags "-static"' -o _output/simplescheduler ./cmd/simplescheduler

schedulerclient:
	go build $(RACE) -a -ldflags '-extldflags "-static"' -o _output/schedulerclient ./cmd/schedulerclient

clean:
	go clean -r -x ./...
	-rm -rf _output



