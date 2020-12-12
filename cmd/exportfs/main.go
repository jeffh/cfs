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

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "mount for CFS\n")
		fmt.Fprintf(w, "Usage: %s [OPTIONS] LOCAL_MOUNT\n", os.Args[0])
		fmt.Fprintf(w, "Translate a 9p file server mount point into a locally mounted file system\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		if flag.NArg() == 0 {
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
