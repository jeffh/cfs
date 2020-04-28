.PHONY: clean exportfs

GO := $(shell which go)
CMDS := $(shell ls cmd)

# Example goargs
# GOARGS=-race for race condition checking

all: $(CMDS)

$(CMDS): $(find . -type '*.go')
	$(GO) build --ldflags="-s -w" $(GOARGS) -o ./bin/$@ ./cmd/$@

compress: $(CMDS)
	echo $(CMDS) | xargs -n 1 -I'{}' upx './bin/{}'
	ls -lh bin

test:
	$(GO) test $(GOARGS) ./...

clean:
	rm -rf bin; true
