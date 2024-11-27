package main

import (
	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	cli.ServiceMain(func() ninep.FileSystem { return &fs.Mem{} })
}
