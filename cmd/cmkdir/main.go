package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "mkdir for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR/PATH\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		return mnt.FS.MakeDir(context.Background(), mnt.Prefix, 0700)
	})
}
