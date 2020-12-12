package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var (
		mode int
	)
	flag.IntVar(&mode, "mode", 0644, "The mode to set the file that gets created")

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
		flags |= ninep.OTRUNC

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
		_, err = io.Copy(wtr, os.Stdin)
		if err != nil && err != io.EOF {
			return err
		}

		return nil
	})
}
