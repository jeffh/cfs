package main

import (
	"context"
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
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH [MORE_PATHS...]\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "rm for CFS - will delete directories\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	ctx := context.Background()

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		if flag.NArg() == 0 {
			flag.Usage()
			os.Exit(1)
		}

		files = []string{mnt.Prefix}
		files = append(files, flag.Args()[1:]...)

		for _, path := range files {
			node, err := mnt.FS.Traverse(ctx, path)
			if os.IsNotExist(err) {
				return fmt.Errorf("path does not exist: %s", filepath.Join(mnt.Addr, path))
			}
			if err != nil {
				return err
			}

			if node.Type().IsDir() && !recursive {
				_ = node.Close()
				return errors.New("use -r to delete directories")
			}

			err = node.Delete()
			if err != nil {
				return err
			}
		}
		return nil
	})
}
