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

# Check if this GO tools version used is at least the version of go specified in
# the go.mod file. The version in go.mod should be in sync with other repos.
GO_VERSION := $(shell go version | awk '{print substr($$3, 3, 10)}')
MOD_VERSION := $(shell cat .go_version) 

GM := $(word 1,$(subst ., ,$(GO_VERSION)))
MM := $(word 1,$(subst ., ,$(MOD_VERSION)))
FAIL := $(shell if [ $(GM) -lt $(MM) ]; then echo MAJOR; fi)
ifdef FAIL
$(error Build should be run with at least go $(MOD_VERSION) or later, found $(GO_VERSION))
endif
GM := $(word 2,$(subst ., ,$(GO_VERSION)))
MM := $(word 2,$(subst ., ,$(MOD_VERSION)))
FAIL := $(shell if [ $(GM) -lt $(MM) ]; then echo MINOR; fi)
ifdef FAIL
$(error Build should be run with at least go $(MOD_VERSION) or later, found $(GO_VERSION))
endif

# Make sure we are in the same directory as the Makefile
BASE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Force Go modules even when checked out inside GOPATH
GO111MODULE := on
export GO111MODULE

REPO=github.com/apache/yunikorn-core/pkg
# when using the -race option CGO_ENABLED is set to 1 (automatically)
# it breaks cross compilation.
RACE=-race
# build commands on local os by default, uncomment for cross-compilation
#GOOS=darwin
#GOARCH=amd64

ifeq ($(HOST_ARCH),)
HOST_ARCH := $(shell uname -m)
endif

# Kernel (OS) Name
OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')

all:
	$(MAKE) -C $(dir $(BASE_DIR)) build

LINTBASE := $(shell go env GOPATH)/bin
LINTBIN  := $(LINTBASE)/golangci-lint
$(LINTBIN):
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(LINTBASE) v1.51.2
	stat $@ > /dev/null 2>&1

.PHONY: lint
# Run lint against the previous commit for PR and branch build
# In dev setup look at all changes on top of master
lint: $(LINTBIN)
	@echo "running golangci-lint"
	git symbolic-ref -q HEAD && REV="origin/HEAD" || REV="HEAD^" ; \
	headSHA=$$(git rev-parse --short=12 $${REV}) ; \
	echo "checking against commit sha $${headSHA}" ; \
	${LINTBIN} run --new-from-rev=$${headSHA}

.PHONY: install_shellcheck
SHELLCHECK_PATH := "$(BASE_DIR)shellcheck"
SHELLCHECK_VERSION := "v0.8.0"
SHELLCHECK_ARCHIVE := "shellcheck-$(SHELLCHECK_VERSION).$(OS).$(HOST_ARCH).tar.xz"
install_shellcheck:
	@echo ${SHELLCHECK_PATH}
	@if command -v "shellcheck" &> /dev/null; then \
		exit 0 ; \
	elif [ -x ${SHELLCHECK_PATH} ]; then \
		exit 0 ; \
	elif [ "${HOST_ARCH}" = "arm64" ]; then \
		echo "Unsupported architecture 'arm64'" \
		exit 1 ; \
	else \
		curl -sSfL https://github.com/koalaman/shellcheck/releases/download/${SHELLCHECK_VERSION}/${SHELLCHECK_ARCHIVE} | tar -x -J --strip-components=1 shellcheck-${SHELLCHECK_VERSION}/shellcheck ; \
	fi

# Check scripts
.PHONY: check_scripts
ALLSCRIPTS := $(shell find . -name '*.sh')
check_scripts: install_shellcheck
	@echo "running shellcheck"
	@if command -v "shellcheck" &> /dev/null; then \
		shellcheck ${ALLSCRIPTS} ; \
	elif [ -x ${SHELLCHECK_PATH} ]; then \
		${SHELLCHECK_PATH} ${ALLSCRIPTS} ; \
	else \
		echo "shellcheck not found: failing target" \
		exit 1; \
	fi

.PHONY: license-check
# This is a bit convoluted but using a recursive grep on linux fails to write anything when run
# from the Makefile. That caused the pull-request license check run from the github action to
# always pass. The syntax for find is slightly different too but that at least works in a similar
# way on both Mac and Linux. Excluding all .git* files from the checks.
license-check:
	@echo "checking license headers:"
ifeq (darwin,$(OS))
	$(shell find -E . -not -path "./.git*" -regex ".*\.(go|sh|md|yaml|yml|mod)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > LICRES)
else
	$(shell find . -not -path "./.git*" -regex ".*\.\(go\|sh\|md\|yaml\|yml\|mod\)" -exec grep -L "Licensed to the Apache Software Foundation" {} \; > LICRES)
endif
	@if [ -s LICRES ]; then \
		echo "following files are missing license header:" ; \
		cat LICRES ; \
		rm -f LICRES ; \
		exit 1; \
	fi ; \
	rm -f LICRES
	@echo "  all OK"

# Check that we use pseudo versions in master
.PHONY: pseudo
BRANCH := $(shell git branch --show-current)
SI_REF := $(shell go list -m -f '{{ .Version }}' github.com/apache/yunikorn-scheduler-interface)
SI_MATCH := $(shell expr "${SI_REF}" : "v0.0.0-")
pseudo:
	@echo "pseudo version check"
	@if [ "${BRANCH}" = "master" ]; then \
		if [ ${SI_MATCH} -ne 7 ]; then \
			echo "YuniKorn references MUST all be pseudo versions:" ; \
			echo " SI ref: ${SI_REF}" ; \
			exit 1; \
		fi \
	fi
	@echo "  all OK"

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
test: clean
	@echo "running unit tests"
	go test ./... $(RACE) -tags deadlock -coverprofile=coverage.txt -covermode=atomic
	go vet $(REPO)...

# Generate FSM graphs (dot/png)
.PHONY: fsm_graph
fsm_graph: clean
	@echo "generating FSM graphs"
	go test -tags graphviz -run 'Test.*FsmGraph' ./pkg/scheduler/objects
	scripts/generate-fsm-graph-images.sh

# Simple clean of generated files only (no local cleanup).
.PHONY: clean
clean:
	@echo "cleaning up caches and output"
	go clean -cache -testcache -r
	-rm -rf _output
