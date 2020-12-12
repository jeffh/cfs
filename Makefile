.PHONY: clean exportfs

GO := $(shell which go)
CMDS := $(shell ls cmd)

# Example goargs
# GOARGS=-race for race condition checking

all: $(CMDS)

deps:
	env 'GOPRIVATE=github.com/jeffh/*' go get github.com/jeffh/b2client 

# $(GO) build --ldflags="-s -w" $(GOARGS) -o ./bin/$@ ./cmd/$@
$(CMDS): $(find . -type '*.go')
	$(GO) build $(GOARGS) -o ./bin/$@ ./cmd/$@

compress: $(CMDS)
	echo $(CMDS) | xargs -n 1 -I'{}' upx './bin/{}'
	ls -lh bin

test:
	$(GO) test $(GOARGS) ./...

clean:
	rm -rf bin; true
