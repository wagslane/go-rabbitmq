all: test vet lint staticcheck

test:
	go test ./...

vet:
	go vet ./...

lint:
	go list ./... | grep -v /vendor/ | xargs -L1 golint -set_exit_status

staticcheck:
	staticcheck ./...
