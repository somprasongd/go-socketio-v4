.PHONY: build test race vet fmt check

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
