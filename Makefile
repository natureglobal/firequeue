u := $(if $(update),-u)

export GO111MODULE=on

.PHONY: deps
deps:
	go get ${u} -d
	go mod tidy

.PHONY: devel-deps
devel-deps:
	go install golang.org/x/lint/golint@latest
	go install github.com/Songmu/godzil/cmd/godzil@latest

.PHONY: test
test:
	go test

.PHONY: lint
lint: devel-deps
	golint -set_exit_status

.PHONY: release
release: devel-deps
	godzil release
