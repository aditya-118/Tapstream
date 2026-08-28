SHELL := /bin/bash

# Go protoc plugins, version-pinned in server/go.mod via `go get -tool`.
GO_PLUGINS := google.golang.org/protobuf/cmd/protoc-gen-go

.PHONY: tools proto-go lint clean-gen

# Build the pinned protoc plugins into server/bin so buf can find them on PATH.
tools:
	cd server && go build -o ./bin/ $(GO_PLUGINS)

proto-go: tools
	PATH="$(CURDIR)/server/bin:$$PATH" npx buf generate --template buf.gen.go.yaml

lint:
	npx buf lint

clean-gen:
	rm -rf server/gen server/bin
