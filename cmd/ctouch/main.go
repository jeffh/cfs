package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var path string

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "touch for CFS\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [OPTIONS] ADDR/PATH\n", os.Args[0])
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		ctx := context.Background()

		path = mnt.Prefix
		_, err := mnt.FS.Stat(ctx, path)

		if os.IsNotExist(err) {
			h, err := mnt.FS.CreateFile(ctx, path, ninep.OREAD, 0666)
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

		err = mnt.FS.WriteStat(ctx, path, stat)
		return err
	})
}
