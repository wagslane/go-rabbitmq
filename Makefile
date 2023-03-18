all: prepare-integration test vet staticcheck clean-integration

test:
	go test -v ./...

vet:
	go vet ./...

staticcheck:
	staticcheck ./...

prepare-integration:
	docker compose up -d

clean-integration:
	docker compose down -v
