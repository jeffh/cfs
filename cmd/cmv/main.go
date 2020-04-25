package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "mv for CFS - move files and directories\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR PATH [NEW_ADDR] NEW_PATH\n", os.Args[0])
		flag.PrintDefaults()
	}

	exitCode := 0

	defer func() {
		os.Exit(exitCode)
	}()

	cli.MainClient(func(c ninep.Client, fs *ninep.FileSystemProxy) error {
		if flag.NArg() < 3 || flag.NArg() > 4 {
			flag.Usage()
			exitCode = 1
			runtime.Goexit()
		}

		if flag.NArg() == 4 {
			fmt.Fprintf(os.Stderr, "Unsupported\n")
			exitCode = 1
			runtime.Goexit()
		}

		path := flag.Args()[1]
		newPath := flag.Args()[2]

		err := fs.WriteStat(context.Background(), path, ninep.SyncStatWithName(newPath))
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("Path does not exist: %s", path)
		}
		return err
	})
}
