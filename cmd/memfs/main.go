package main

import (
	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	cli.ServiceMain(func() ninep.FileSystem { return &fs.Mem{} })
}
