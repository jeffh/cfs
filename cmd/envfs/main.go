package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	var readOnly bool

	flag.BoolVar(&readOnly, "ro", false, "Serve the file system in read-only mode.")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "Exposes environment variables as a 9p file server.\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.ServiceMain(func() ninep.FileSystem {
		fsys := fs.Env()
		if readOnly {
			fsys = fs.ReadOnly(fsys)
			fmt.Fprintf(os.Stderr, "Serving in read-only mode\n")
		}
		return fsys
	})
}
