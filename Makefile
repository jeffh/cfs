.PHONY: clean exportfs

GO := $(shell which go)
CMDS := $(shell ls cmd)
OPS := $(shell which ops)

# Example goargs
# GOARGS=-race for race condition checking

all: $(CMDS)

deps:
	go get github.com/jeffh/b2client

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

.PHONY: rpi-einkfs
rpi-einkfs:
	env GOOS=linux GOARCH=arm GOARM=6 $(MAKE) einkfs

DEPLOY_TARGET=pi@rpiw-eink.local
deploy-rpi-einkfs: rpi-einkfs
	$(OPS) -ssh $(DEPLOY_TARGET) \
		file -content-from ./bin/einkfs -path einkfs -owner pi -group pi -perm 0755 \
		unit -file config/systemd/einkfs.service -restart