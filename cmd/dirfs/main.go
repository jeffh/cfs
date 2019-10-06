package main

import (
	"flag"
	"fmt"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var root string

	flag.StringVar(&root, "root", ".", "The root directory to serve files from. Defaults the current working directory.")

	cli.BasicServerMain(func() ninep.FileSystem {
		fmt.Printf("Serving: %v\n", root)
		return fs.Dir(root)
	})
}
