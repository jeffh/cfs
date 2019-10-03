package main

import (
	"git.sr.ht/~jeffh/cfs/cli"
	"git.sr.ht/~jeffh/cfs/fs"
	"git.sr.ht/~jeffh/cfs/ninep"
)

func main() {
	cli.BasicServerMain(func() ninep.FileSystem { return &fs.Mem{} })
}
