all: test vet staticcheck


# Run all tests, including integration tests
test:
	go test -v -tags=integration ./...

# Run unit tests only
unit-test:
	go test -v ./...

vet:
	go vet ./...

staticcheck:
	staticcheck ./...
