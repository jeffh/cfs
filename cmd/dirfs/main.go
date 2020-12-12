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

	flag.StringVar(&root, "root", ".", "The root directory to serve files from. Defaults the current working directory.")

	cfg := &service.Config{
		Name:        "dirfs",
		DisplayName: "Dir File System Service",
		Description: "Provides a 9p file system that exposes a local directory",
	}

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(w, "Exposes a local file directory as a 9p file server.\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.ServiceMain(cfg, func() ninep.FileSystem {
		fmt.Printf("Serving: %v\n", root)
		return fs.Dir(root)
	})
}
