package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var (
		path     string
		nostdout bool
	)

	flag.BoolVar(&nostdout, "s", false, "Don't print to stdout, only write to file")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "tee for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR [PATH]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c *ninep.Client, fs *ninep.FileSystemProxy) error {
		if flag.NArg() == 1 {
			path = ""
		} else {
			path = flag.Arg(1)
		}

		var (
			h   ninep.FileHandle
			err error
		)

		path = flag.Arg(1)
		_, err = fs.Stat(path)
		if os.IsNotExist(err) {
			h, err = fs.CreateFile(path, ninep.OWRITE|ninep.OTRUNC, 0664)
		} else {
			h, err = fs.OpenFile(path, ninep.OWRITE|ninep.OTRUNC)
		}
		if err != nil {
			return err
		}
		defer h.Close()

		wtr := ninep.Writer(h)
		if nostdout {
			io.Copy(wtr, os.Stdin)
		} else {
			rdr := io.TeeReader(os.Stdin, wtr)
			io.Copy(os.Stdout, rdr)
		}
		return nil
	})
}
