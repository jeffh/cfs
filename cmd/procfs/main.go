package main

import (
	"time"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	cli.BasicServerMain(func() ninep.FileSystem { return &fs.Proc{CacheTTL: 0 * time.Second} })
}
