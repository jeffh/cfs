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
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH\n\n", os.Args[0])
		fmt.Fprintf(w, "mkdir for CFS\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		return mnt.FS.MakeDir(context.Background(), mnt.Prefix, 0700)
	})
}
