.PHONY: test all clean

CMDS := $(shell ls cmd)

all: $(CMDS)

$(CMDS):
	go build -o ./bin/$@ ./cmd/$@

test:
	go test ./...

clean:
	rm -rf bin; true
