package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
)

func main() {
	var (
		files     []string
		recursive bool
	)

	flag.BoolVar(&recursive, "r", false, "Recursively delete directories")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "rm for CFS - will delete directories\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR/PATH [MORE_PATHS...]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		if flag.NArg() == 0 {
			flag.Usage()
			os.Exit(1)
		}

		files = []string{mnt.Prefix}
		files = append(files, flag.Args()[1:]...)

		for _, path := range files {
			node, err := mnt.FS.Traverse(path)
			if os.IsNotExist(err) {
				return fmt.Errorf("Path does not exist: %s", filepath.Join(mnt.Addr, path))
			}
			if err != nil {
				return err
			}

			if node.Type().IsDir() && !recursive {
				node.Close()
				return errors.New("Use -r to delete directories")
			}

			err = node.Delete()
			if err != nil {
				return err
			}
		}
		return nil
	})
}
