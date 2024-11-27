package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH NEW_ADDR/NEW_PATH\n\n", os.Args[0])
		fmt.Fprintf(w, "mv for CFS - move files and directories\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	exitCode := 0

	defer func() {
		os.Exit(exitCode)
	}()

	cli.MainClient(func(cfg *cli.ClientConfig, srcMnt proxy.FileSystemMount) error {
		ctx := context.Background()
		if flag.NArg() < 1 || flag.NArg() > 2 {
			flag.Usage()
			exitCode = 1
			runtime.Goexit()
		}

		dstMntCfg, ok := proxy.ParseMount(flag.Arg(1))
		if !ok {
			fmt.Fprintf(os.Stderr, "Invalid destination path: %v\n", flag.Arg(1))
			fmt.Fprintf(os.Stderr, "Format should be IP:PORT/PATH format.\n")
			exitCode = 1
			runtime.Goexit()
		}

		if dstMntCfg.Prefix == srcMnt.Prefix {
			// move in the same server
			err := srcMnt.FS.WriteStat(ctx, srcMnt.Prefix, ninep.SyncStatWithName(dstMntCfg.Prefix))
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("Path does not exist: %s", srcMnt.String())
			}
			return err
		} else {
			// move to another server
			dstMnt, err := cfg.FSMount(&dstMntCfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error connecting to destination fs: %s\n", err)
				exitCode = 2
				runtime.Goexit()
			}

			in, err := srcMnt.FS.OpenFile(ctx, srcMnt.Prefix, ninep.OREAD)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening file for reading: %s\n", srcMnt.String())
				exitCode = 1
				runtime.Goexit()
			}
			defer in.Close()

			out, err := dstMnt.FS.OpenFile(ctx, dstMnt.Prefix, ninep.OWRITE|ninep.OTRUNC)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error opening file for writing: %s\n", dstMnt.String())
				exitCode = 2
				runtime.Goexit()
			}

			var buf [4096]byte
			_, err = io.CopyBuffer(ninep.Writer(out), ninep.Reader(in), buf[:])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to move file to new fs: %s", err)
				if er := dstMnt.FS.Delete(ctx, dstMnt.Prefix); er != nil {
					fmt.Fprintf(os.Stderr, "Failed to deleted attempted copy of file: %s: %s", dstMnt.String(), er)
				}
				exitCode = 3
				runtime.Goexit()
			}

			err = srcMnt.FS.Delete(ctx, srcMnt.Prefix)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to delete old file: %s, but file has been copied to its new place", srcMnt.String())
				exitCode = 4
				runtime.Goexit()
			}

			return nil
		}
	})
}
