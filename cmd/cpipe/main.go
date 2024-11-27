package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	var (
		mode           int
		append         bool
		readAfterWrite bool
	)
	flag.IntVar(&mode, "mode", 0644, "The mode to set the file that gets created")
	flag.BoolVar(&append, "append", false, "Append to the file instead of overwriting it")
	flag.BoolVar(&readAfterWrite, "res", false, "Read the file after writing it (for device files)")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH\n\n", os.Args[0])
		fmt.Fprintf(w, "Shell Pipe for CFS. Writes STDIN into a file.\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		var (
			h   ninep.FileHandle
			err error
		)

		ctx := context.Background()

		path := mnt.Prefix
		_, err = mnt.FS.Stat(ctx, path)
		flags := ninep.OpenMode(ninep.OWRITE)
		if readAfterWrite || append {
			flags = ninep.OpenMode(ninep.ORDWR)
		}
		if !append {
			flags |= ninep.OTRUNC
		}

		if os.IsNotExist(err) {
			h, err = mnt.FS.CreateFile(ctx, path, flags, ninep.Mode(mode))
		} else {
			h, err = mnt.FS.OpenFile(ctx, path, flags)
		}
		if err != nil {
			return err
		}
		defer h.Close()

		wtr := ninep.Writer(h)
		if append {
			_, err := wtr.Seek(0, io.SeekEnd)
			if err != nil {
				return err
			}
		}
		_, err = io.Copy(wtr, os.Stdin)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		if readAfterWrite {
			rdr := ninep.Reader(h)
			_, err = io.Copy(os.Stdout, rdr)
			if err != nil && err != io.EOF {
				return err
			}
		}

		return nil
	})
}
