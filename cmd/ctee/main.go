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
	_ "go.uber.org/automaxprocs"
)

func main() {
	var (
		nostdout bool
		read     bool
	)

	flag.BoolVar(&nostdout, "s", false, "Don't print to stdout, only write to file")
	flag.BoolVar(&read, "r", false, "Read file's contents after writing. Useful for ctl files")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH\n\n", os.Args[0])
		fmt.Fprintf(w, "tee for CFS\n\n")
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
		var flags ninep.OpenMode
		if read {
			flags = ninep.ORDWR
		} else {
			flags = ninep.OWRITE
		}
		flags |= ninep.OTRUNC

		if os.IsNotExist(err) {
			h, err = mnt.FS.CreateFile(ctx, path, flags, 0664)
		} else {
			h, err = mnt.FS.OpenFile(ctx, path, flags)
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
