package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var root string
	var pattern string

	flag.StringVar(&root, "dir", "", "The base where the temp dir is located")
	flag.StringVar(&pattern, "pattern", "tmpfs-*", "The pattern which to create the temp directory name")

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
		fmt.Printf("Serving: %v\n", dir)
		return fs.Dir(dir)
	})
}
