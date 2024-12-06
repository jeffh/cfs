package main

import (
	"log"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/dockerfs"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	cli.ServiceMain(func() ninep.FileSystem {
		fs, err := dockerfs.NewFs()
		if err != nil {
			log.Fatalf("failed to create dockerfs: %s", err)
		}
		return fs
	})
}
