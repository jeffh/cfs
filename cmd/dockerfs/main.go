package main

import (
	"log"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/dockerfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	fs, err := dockerfs.NewFs()
	if err != nil {
		log.Fatalf("failed to create dockerfs: %s", err)
	}
	defer fs.Close()
	cfg := &service.Config{
		Name:        "dockerfs",
		DisplayName: "Docker API File System Service",
		Description: "Provides a 9p file system that gives full access to docker",
	}
	cli.ServiceMain(cfg, func() ninep.FileSystem { return fs })
}
