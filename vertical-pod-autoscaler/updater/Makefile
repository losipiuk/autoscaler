all: build

TAG?=v0.0.1
REGISTRY?=k8s.gcr.io
FLAGS=
ENVVAR=
GOOS?=linux

deps:
	go get github.com/tools/godep

build: clean deps
	$(ENVVAR) GOOS=$(GOOS) godep go build ./...
	$(ENVVAR) GOOS=$(GOOS) godep go build -o updater

test-unit: clean deps build
	$(ENVVAR) godep go test --test.short -race ./... $(FLAGS)

docker:
ifndef REGISTRY
	ERR = $(error REGISTRY is undefined)
	$(ERR)
endif
	docker build --pull -t ${REGISTRY}/vpa-updater:${TAG} .
	gcloud docker -- push ${REGISTRY}/vpa-updater:${TAG}

release: build docker

clean:
	rm -f updater

format:
	test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -d {} + | tee /dev/stderr)" || \
	test -z "$$(find . -path ./vendor -prune -type f -o -name '*.go' -exec gofmt -s -w {} + | tee /dev/stderr)"

.PHONY: all deps build test-unit clean format release
