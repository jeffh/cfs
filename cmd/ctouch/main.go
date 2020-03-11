package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var path string

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "touch for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR [PATH]\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(c ninep.Client, fs *ninep.FileSystemProxy) error {
		if flag.NArg() == 1 {
			path = ""
		} else {
			path = flag.Arg(1)
		}

		ctx := context.Background()

		path = flag.Arg(1)
		_, err := fs.Stat(ctx, path)

		if os.IsNotExist(err) {
			h, err := fs.CreateFile(ctx, path, ninep.OREAD, 0666)
			if err != nil {
				return err
			}
			h.Close()
		} else if err != nil {
			return err
		}

		stat := ninep.SyncStat()
		now := uint32(time.Now().Unix())
		stat.SetMtime(now)
		stat.SetAtime(now)

		err = fs.WriteStat(ctx, path, stat)
		return err
	})
}
