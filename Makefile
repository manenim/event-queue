.PHONY: run test cover vet fmt

run:
	go run ./cmd/demo

test:
	go test -race -count=1 ./...

cover:
	go test -race -coverprofile=coverage.out ./... && go tool cover -func=coverage.out

vet:
	go vet ./...

fmt:
	gofmt -l -w .
