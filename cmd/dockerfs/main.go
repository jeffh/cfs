package main

import (
	"log"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/dockerfs"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	fs, err := dockerfs.NewFs()
	if err != nil {
		log.Fatalf("failed to create dockerfs: %s", err)
	}
	defer fs.Close()
	cli.ServiceMain(func() ninep.FileSystem { return fs })
}
