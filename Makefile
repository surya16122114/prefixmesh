GO ?= go
PROTO_DIR := api/proto
GEN_DIR := gen

.PHONY: all proto build test vet lint clean compose-up compose-down bench

all: proto build test

proto:
	protoc -I $(PROTO_DIR) \
		--go_out=. --go_opt=module=github.com/surya16122114/prefixmesh \
		--go-grpc_out=. --go-grpc_opt=module=github.com/surya16122114/prefixmesh \
		$(PROTO_DIR)/prefixmesh/v1/*.proto

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

bench:
	@echo "benchmark suite lands in M4 — see docs/BENCHMARKS.md"

compose-up:
	docker compose -f deploy/docker-compose.yml up -d --build

compose-down:
	docker compose -f deploy/docker-compose.yml down -v

clean:
	rm -rf bin
