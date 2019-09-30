.PHONY: test test_race all all_race clean

CMDS := $(shell ls cmd)

all: $(CMDS)

$(CMDS):
	go build $(GOARGS) -o ./bin/$@ ./cmd/$@

test:
	go test $(GOARGS) ./...

clean:
	rm -rf bin; true
