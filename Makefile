.PHONY: clean exportfs

GO := $(shell which go)
CMDS := $(shell ls cmd)

# Example goargs
# GOARGS=-race for race condition checking

all: deps $(CMDS)

deps:
	env 'GOPRIVATE=github.com/jeffh/*' go get github.com/jeffh/b2client 

$(CMDS): $(find . -type '*.go')
	$(GO) build --ldflags="-s -w" $(GOARGS) -o ./bin/$@ ./cmd/$@

compress: $(CMDS)
	echo $(CMDS) | xargs -n 1 -I'{}' upx './bin/{}'
	ls -lh bin

test:
	$(GO) test $(GOARGS) ./...

clean:
	rm -rf bin; true
