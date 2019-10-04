package main

import (
	"flag"
	"fmt"
	"os"

	"git.sr.ht/~jeffh/cfs/cli"
	"git.sr.ht/~jeffh/cfs/ninep"
)

func main() {
	var path string

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "ls for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR [PATH]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c *ninep.Client, fs ninep.FileSystem) error {
		if flag.NArg() == 1 {
			path = ""
		} else {
			path = flag.Arg(1)
		}

		path = flag.Arg(1)
		infos, err := fs.ListDir(path)
		if err != nil {
			return err
		}

		for _, info := range infos {
			fmt.Println(info.Name())
		}
		return nil
	})
}
