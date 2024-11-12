package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jeffh/cfs/cli"
	efuse "github.com/jeffh/cfs/exportfs/fuse"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var mountType string
	var mountpoint string
	var nfsAddr string

	flag.StringVar(&mountType, "type", "fuse", "Mount type to use, defaults to 'fuse', but can also be 'nfs'")
	flag.StringVar(&nfsAddr, "nfs-addr", "127.0.0.1:2049", "Address for NFS to listen on")
	flag.StringVar(&mountpoint, "mount", "/tmp/exportfs", "Directory to mount the 9p file system on")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "mount for CFS\n")
		fmt.Fprintf(w, "Usage: %s [OPTIONS] LOCAL_MOUNT\n", os.Args[0])
		fmt.Fprintf(w, "Translate a 9p file server mount point into a locally mounted file system\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cli.OnInterrupt(cancel)

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		var logger ninep.Logger = log.New(os.Stderr, "", log.LstdFlags)
		loggable := ninep.Loggable{
			ErrorLog: logger,
			TraceLog: logger,
		}

		switch mountType {
		case "fuse":
			oneSec := time.Second
			opts := fs.Options{
				MountOptions: fuse.MountOptions{
					FsName:        "9pfuse",
					Name:          "exportfs",
					DisableXAttrs: true,
					DirectMount:   true,
					Debug:         true,
				},
				EntryTimeout: &oneSec,
				AttrTimeout:  &oneSec,
				Logger:       log.New(os.Stdout, "[fuse] ", log.LstdFlags),
			}
			// fuse.FSName("9pfuse"),
			// fuse.Subtype("9pfusefs"),
			// fuse.VolumeName("exportfs"),
			// fuse.NoBrowse(),
			// fuse.NoAppleXattr(),
			// fuse.NoAppleDouble(),

			return efuse.MountAndServeFS(
				ctx,
				mnt.FS,
				mnt.Prefix,
				loggable,
				mountpoint,
				&opts,
			)
		case "nfs":
			return runServer(ctx, nfsAddr, mnt, mountpoint)
		default:
			return fmt.Errorf("Unknown mount type: got %#v. Expected 'fuse' or 'nfs'", mountType)
		}
	})
}
