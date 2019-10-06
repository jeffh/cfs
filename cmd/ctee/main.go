package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"git.sr.ht/~jeffh/cfs/cli"
	"git.sr.ht/~jeffh/cfs/ninep"
)

func main() {
	var path string
	var numlines int

	flag.IntVar(&numlines, "n", 0, "number of lines to print")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "cat for CFS\n")
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
		h, err := fs.OpenFile(path, ninep.OREAD)
		if err != nil {
			return err
		}
		defer h.Close()

		rdr := ninep.Reader(h)
		io.Copy(os.Stdout, rdr)

		return nil
	})
}
