package main

import (
	"log"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/b2fs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	// cli.BasicServerMain(func() ninep.FileSystem {
	// 	fs, err := b2fs.NewFsFromEnv()
	// 	if err != nil {
	// 		log.Fatalf("error: %s", err)
	// 	}
	// 	return fs
	// })
	cfg := &service.Config{
		Name:        "b2fs",
		DisplayName: "B2 File System Service",
		Description: "Provides a 9p file system that connects to Backblaze's B2 service",
	}
	cli.ServiceMain(cfg, func() ninep.FileSystem {
		fs, err := b2fs.NewFsFromEnv()
		if err != nil {
			log.Fatalf("error: %s", err)
		}
		return fs
	})
}
