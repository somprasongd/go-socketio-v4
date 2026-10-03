.PHONY: build test race vet fmt check compliance integration

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

check: build test vet
	@echo "gofmt:" && test -z "$$(gofmt -l .)"

compliance:
	./compliance/run.sh

# Requires an owned disposable Redis 7.2+ endpoint, plus interop dependencies.
integration:
	@test -n "$(SOCKETIO_REDIS_ADDR)" || (echo "Set SOCKETIO_REDIS_ADDR to an owned disposable Redis endpoint"; exit 1)
	SOCKETIO_REDIS_ADDR="$(SOCKETIO_REDIS_ADDR)" go test -count=1 -v ./redisstreamsadapter -run 'TestDistributedJSRecovery|TestRealRedisDisconnectResumeAndFailClosed'
