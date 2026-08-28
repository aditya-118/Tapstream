SHELL := /bin/bash

# Go protoc plugins, version-pinned in server/go.mod via `go get -tool`.
GO_PLUGINS := google.golang.org/protobuf/cmd/protoc-gen-go \
               connectrpc.com/connect/cmd/protoc-gen-connect-go

.PHONY: tools proto-go lint proto-breaking clean-gen

# Build the pinned protoc plugins into server/bin so buf can find them on PATH.
tools:
	cd server && go build -o ./bin/ $(GO_PLUGINS)

proto-go: tools
	PATH="$(CURDIR)/server/bin:$$PATH" npx buf generate --template buf.gen.go.yaml

lint:
	npx buf lint

# Field numbers are the wire contract, not names. Run before changing a proto
# that already has clients.
proto-breaking:
	npx buf breaking --against '.git#branch=master'

clean-gen:
	rm -rf server/gen server/bin

# --- Kafka ---------------------------------------------------------------
KAFKA_BIN := docker compose exec -T kafka /opt/kafka/bin
BOOTSTRAP := localhost:9092
TOPICS := clickstream.events clickstream.orders

.PHONY: kafka-up kafka-down topics topics-list

kafka-up:
	docker compose up -d --wait

kafka-down:
	docker compose down

# Partitioned by category, so a category's events keep their relative order.
topics:
	@for t in $(TOPICS); do \
	  $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP) \
	    --create --if-not-exists --topic $$t --partitions 3 --replication-factor 1; \
	done

topics-list:
	@$(KAFKA_BIN)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP) --list

# --- Go ------------------------------------------------------------------
.PHONY: test build

test:
	cd server && go test -race ./...

build:
	cd server && go build ./...
