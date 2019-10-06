package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var (
		files     []string
		recursive bool
	)

	flag.BoolVar(&recursive, "r", false, "Recursively delete directories")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "rm for CFS - will delete directories\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR PATH [MORE_PATHS...]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c *ninep.Client, fs ninep.FileSystem) error {
		if flag.NArg() == 1 {
			flag.Usage()
			os.Exit(1)
		}

		files = flag.Args()[1:]

		for _, path := range files {
			info, err := fs.Stat(path)
			if os.IsNotExist(err) {
				return fmt.Errorf("Path does not exist: %s", path)
			}
			if err != nil {
				return err
			}

			if info.IsDir() && !recursive {
				return errors.New("Use -r to delete directories")
			}

			err = fs.Delete(path)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
