all: test vet staticcheck

test:
	go test ./...

vet:
	go vet ./...

staticcheck:
	staticcheck ./...
