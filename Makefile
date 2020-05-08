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


# Check if this is at least GO 1.11 for Go Modules
GO_VERSION := $(shell go version | awk '$$3 ~ /go1.(10|0-9])/ {print $$3}')
ifdef GO_VERSION
$(error Build requires go 1.11 or later)
endif

# Make sure we are in the same directory as the Makefile
BASE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

REPO=github.com/apache/incubator-yunikorn-core/pkg
# when using the -race option CGO_ENABLED is set to 1 (automatically)
# it breaks cross compilation.
RACE=-race
# build commands on local os by default, uncomment for cross-compilation
#GOOS=darwin
#GOARCH=amd64

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

.PHONY: lint
lint:
	@echo "running golangci-lint"
	@lintBin=$$(go env GOPATH)/bin/golangci-lint ; \
	if [ ! -f "$${lintBin}" ]; then \
		lintBin=$$(echo ./bin/golangci-lint) ; \
		if [ ! -f "$${lintBin}" ]; then \
			echo "golangci-lint executable not found" ; \
			exit 1; \
		fi \
	fi ; \
	headSHA=$$(git rev-parse --short=12 origin/HEAD) ; \
	$${lintBin} run --new-from-rev=$${headSHA}

.PHONY: license-check
license-check:
	@echo "checking license header"
	@licRes=$$(grep -Lr --include=*.{go,sh,md,yaml,yml,mod} "Licensed to the Apache Software Foundation" .) ; \
	if [ -n "$${licRes}" ]; then \
		echo "following files have incorrect license header:\n$${licRes}" ; \
		exit 1; \
	fi

# Build the example binaries for dev and test
.PHONY: commands
commands:
	@echo "building examples"
	go build $(RACE) -a -ldflags '-extldflags "-static"' -o _output/simplescheduler ./cmd/simplescheduler
	go build $(RACE) -a -ldflags '-extldflags "-static"' -o _output/schedulerclient ./cmd/schedulerclient

# Build binaries for dev and test
.PHONY: build
build: commands

# Run the tests after building
.PHONY: test
test:
	@echo "running unit tests"
	go test -parallel 1 ./... -cover $(RACE) -tags deadlock
	go vet $(REPO)...

# Simple clean of generated files only (no local cleanup).
.PHONY: clean
clean:
	go clean -cache -r -x ./...
	-rm -rf _output
