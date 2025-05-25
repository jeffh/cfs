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
	var root string
	var readOnly bool

	flag.StringVar(&root, "root", ".", "The root directory to serve files from. Defaults the current working directory.")
	flag.BoolVar(&readOnly, "ro", false, "Serve the file system in read-only mode.")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "Exposes a local file directory as a 9p file server.\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.ServiceMain(func() ninep.FileSystem {
		fmt.Printf("Serving: %v\n", root)
		var fsys ninep.FileSystem = fs.Dir(root)
		if readOnly {
			fsys = fs.ReadOnly(fsys)
			fmt.Fprintf(os.Stderr, "Serving in read-only mode\n")
		}
		return fsys
	})
}
