package main

import (
	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/procfs"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	// cli.BasicServerMain(func() ninep.FileSystem { return &fs.Proc{} })
	cli.BasicServerMain(func() ninep.FileSystem { return procfs.NewFs() })
}
