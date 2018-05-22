BINARIES=exsrv excli

all: $(BINARIES)

exsrv: show-todos redrpc/*.go commands/exsrv/*.go
	./scripts/go build -o "$@" github.com/Codility/redis-rpc/go/commands/exsrv

excli: redrpc/*.go commands/excli/*.go
	./scripts/go build -o "$@" github.com/Codility/redis-rpc/go/commands/excli

.PHONY: clean
clean:
	rm -rf $(BINARIES) .gopath

.PHONY: test
test: show-todos goimports govet
	./scripts/go test -v github.com/Codility/redis-rpc/go/redrpc

.PHONY: govet
govet:
	find . -name '*.go' -and -not -path './vendor/*' -and -not -path './.gopath/*' | \
		while read f; do echo `dirname "$$f/*.go"`; done | uniq | xargs ./scripts/go tool vet

.PHONY: goimports
goimports:
	./scripts/go get golang.org/x/tools/cmd/goimports
	@echo "Running goimports..."
	@output=$$(find . -name '*.go' -and -not -path './vendor/*' -and -not -path './.gopath/*' | xargs ./.gopath/bin/goimports -d) && \
		if ! [ -z "$$output" ]; then \
			echo "$$output"; \
			echo "goimports failed!"; \
			exit 1; \
		fi
	@echo "goimports passed"

.PHONY: show-todos
show-todos:
	@echo "---------- TODOs ----------"
	@grep --line-number '// TODO' `find . -name \*.go | grep -v '\(/vendor/\|/.gopath/\)'` \
		| LC_ALL=C perl -ple 's|^([^:]+:[^:]+:)[^/]*|sprintf("%-40s", $$1)|e'
	@echo
