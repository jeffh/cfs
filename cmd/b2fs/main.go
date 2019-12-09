package main

import (
	"log"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/b2fs"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	cli.BasicServerMain(func() ninep.FileSystem {
		fs, err := b2fs.NewFsFromEnv()
		if err != nil {
			log.Fatalf("error: %s", err)
		}
		return fs
	})
}
