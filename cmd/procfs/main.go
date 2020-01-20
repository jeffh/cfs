package main

import (
	"flag"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/procfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	var readOnly bool

	flag.BoolVar(&readOnly, "readonly", false, "Only allow reading processes")
	// cli.BasicServerMain(func() ninep.FileSystem { return &fs.Proc{} })
	cfg := &service.Config{
		Name:        "procfs",
		DisplayName: "Process File System Service",
		Description: "Provides a 9p file system that exposes local process metadata & controls",
	}
	cli.ServiceMain(cfg, func() ninep.FileSystem {
		if readOnly {
			return procfs.NewReadOnlyFs()
		} else {
			return procfs.NewFs()
		}
	})
}
