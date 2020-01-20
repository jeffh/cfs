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
		read     bool
	)

	flag.BoolVar(&nostdout, "s", false, "Don't print to stdout, only write to file")
	flag.BoolVar(&read, "r", false, "Read file's contents after writing. Useful for ctl files")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "tee for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR [PATH]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c ninep.Client, fs *ninep.FileSystemProxy) error {
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
		var flags ninep.OpenMode
		if read {
			flags = ninep.ORDWR
		} else {
			flags = ninep.OWRITE
		}
		flags |= ninep.OTRUNC

		if os.IsNotExist(err) {
			h, err = fs.CreateFile(path, flags, 0664)
		} else {
			h, err = fs.OpenFile(path, flags)
		}
		if err != nil {
			return err
		}
		defer h.Close()

		wtr := ninep.Writer(h)
		if nostdout {
			_, err := io.Copy(wtr, os.Stdin)
			if err != nil && err != io.EOF {
				return err
			}
		} else {
			rdr := io.TeeReader(os.Stdin, os.Stdout)
			_, err := io.Copy(wtr, rdr)
			if err != nil && err != io.EOF {
				return err
			}
		}

		if read {
			fmt.Printf("\n=== READ ===\n")
			_, err := io.Copy(os.Stdout, ninep.Reader(h))
			if err != nil {
				fmt.Fprintf(os.Stderr, "[error] %s\n", err)
			}
		}
		return nil
	})
}
