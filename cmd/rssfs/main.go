package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/rssfs"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "Exposes RSS feeds as a 9p file server.\n\n")
		_, _ = fmt.Fprintf(w, "Control file commands (write to /ctl):\n")
		_, _ = fmt.Fprintf(w, "  add_feed url=<url>    Add a new RSS feed to monitor. Creates a new directory with the feeds' results\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}
	cli.ServiceMain(func() ninep.FileSystem {
		fs := rssfs.NewFs()
		return fs
	})
}
