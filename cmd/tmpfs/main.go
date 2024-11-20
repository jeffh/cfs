package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	var root string
	var pattern string

	flag.StringVar(&root, "dir", "", "The base where the temp dir is located")
	flag.StringVar(&pattern, "pattern", "tmpfs-*", "The pattern which to create the temp directory name")

	cfg := &service.Config{
		Name:        "tmpfs",
		DisplayName: "Temp File System Service",
		Description: "Provides a 9p file system that exposes a local directory. Defaults to temp directory",
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

	cli.ServiceMain(cfg, func() ninep.FileSystem {
		fmt.Printf("Serving: %v\n", dir)
		return fs.Dir(dir)
	})
}
