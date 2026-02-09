.PHONY: test fmt fmt-check vet lint check

test:
	go test ./...

fmt:
	gofmt -w $(shell find . -name '*.go' -type f)

fmt-check:
	@test -z "$(shell gofmt -l $(shell find . -name '*.go' -type f))" || \
		( echo 'gofmt check failed'; gofmt -l $(shell find . -name '*.go' -type f); exit 1 )

vet:
	go vet ./...

lint:
	golangci-lint run ./...

check: fmt-check vet test lint
