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
		w := flag.CommandLine.Output()
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS] ADDR/PATH\n\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "touch for CFS\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
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
			_ = h.Close()
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
