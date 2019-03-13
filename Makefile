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



