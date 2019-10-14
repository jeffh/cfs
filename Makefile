.PHONY: clean

CMDS := $(shell ls cmd)

all: $(CMDS)

$(CMDS): $(find . -type '*.go')
	go build --ldflags="-s -w" $(GOARGS) -o ./bin/$@ ./cmd/$@

test:
	go test $(GOARGS) ./...

clean:
	rm -rf bin; true
