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
	var pattern string

	flag.StringVar(&root, "dir", "", "The base where the temp dir is located")
	flag.StringVar(&pattern, "pattern", "tmpfs-*", "The pattern which to create the temp directory name")

	flag.Usage = func() {
		out := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(out, "Usage: %s [options]\n", os.Args[0])
		_, _ = fmt.Fprintf(out, "Serves a temporary directory over 9p.\n")
		flag.PrintDefaults()
	}

	dir, err := os.MkdirTemp(root, pattern)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create temp directory: %s", err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to clean temp directory: %s: %s", dir, err)
		}
	}()

	cli.ServiceMain(func() ninep.FileSystem {
		fmt.Printf("Serving: %s\n", dir)
		return fs.Dir(dir)
	})
}
