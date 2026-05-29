all: test vet staticcheck

test:
	go test -v ./...

vet:
	go vet ./...

staticcheck:
	staticcheck ./...
