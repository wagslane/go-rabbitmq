all: test vet staticcheck

test:
	go test -v -race ./...

vet:
	go vet ./...

staticcheck:
	staticcheck ./...
