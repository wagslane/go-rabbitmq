all: test vet lint staticcheck

test:
	go test ./...

vet:
	go vet ./...

install-lint:
	GO111MODULE=off go get -u golang.org/x/lint/golint
	GO111MODULE=off go list -f {{.Target}} golang.org/x/lint/golint

lint:
	go list ./... | grep -v /vendor/ | xargs -L1 golint -set_exit_status

install-staticcheck:
	cd /tmp && GOPROXY="" go get honnef.co/go/tools/cmd/staticcheck

staticcheck:
	staticcheck ./...
