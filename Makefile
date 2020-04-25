.PHONY: clean exportfs

CMDS := $(shell ls cmd)

# Example goargs
# GOARGS=-race for race condition checking

all: $(CMDS)

$(CMDS): $(find . -type '*.go')
	go build --ldflags="-s -w" $(GOARGS) -o ./bin/$@ ./cmd/$@

compress: $(CMDS)
	echo $(CMDS) | xargs -n 1 -I'{}' upx './bin/{}'
	ls -lh bin

test:
	go test $(GOARGS) ./...

clean:
	rm -rf bin; true
