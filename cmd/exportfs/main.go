package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"bazil.org/fuse"
	"github.com/jeffh/cfs/cli"
	efuse "github.com/jeffh/cfs/exportfs/fuse"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var mountpoint string
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		s := make(chan os.Signal, 1)
		signal.Notify(s, os.Interrupt)
		<-s
		fmt.Printf("Received term signal\n")
		cancel()
	}()
	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		if flag.NArg() == 1 {
			return fmt.Errorf("Missing mountpoint")
		} else {
			mountpoint = flag.Arg(1)
		}

		var logger ninep.Logger = log.New(os.Stderr, "", log.LstdFlags)
		loggable := ninep.Loggable{
			ErrorLog: logger,
			TraceLog: logger,
		}

		return efuse.MountAndServeFS(
			ctx,
			mnt.FS,
			mnt.Prefix,
			loggable,
			mountpoint,
			fuse.FSName("9pfuse"),
			fuse.Subtype("9pfusefs"),
			fuse.VolumeName("exportfs"),
			fuse.NoBrowse(),
			fuse.NoAppleXattr(),
			fuse.NoAppleDouble(),
		)
	})
}
