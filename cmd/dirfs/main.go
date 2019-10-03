package main

import (
	"flag"
	"fmt"

	"git.sr.ht/~jeffh/cfs/cli"
	"git.sr.ht/~jeffh/cfs/fs"
	"git.sr.ht/~jeffh/cfs/ninep"
)

func main() {
	var root string

	flag.StringVar(&root, "root", ".", "The root directory to serve files from. Defaults the current working directory.")

	cli.BasicServerMain(func() ninep.FileSystem {
		fmt.Printf("Serving: %v\n", root)
		return fs.Dir(root)
	})
}
