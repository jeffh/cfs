package main

import (
	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	cfg := &service.Config{
		Name:        "memfs",
		DisplayName: "InMemory File System Service",
		Description: "Provides a 9p file system that provides an in-memory file system",
	}
	cli.ServiceMain(cfg, func() ninep.FileSystem { return &fs.Mem{} })
}
