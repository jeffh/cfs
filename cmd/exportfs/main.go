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
	_ "go.uber.org/automaxprocs"
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
		_, _ = fmt.Fprintf(w, "mount for CFS\n")
		_, _ = fmt.Fprintf(w, "Usage: %s [OPTIONS] LOCAL_MOUNT\n", os.Args[0])
		_, _ = fmt.Fprintf(w, "Translate a 9p file server mount point into a locally mounted file system\n\n")
		_, _ = fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cli.OnInterrupt(cancel)

	cli.MainClient(func(cfg *cli.ClientConfig, mnt proxy.FileSystemMount) error {
		switch mountType {
		case "fuse":
			timeout := time.Second
			opts := fs.Options{
				MountOptions: fuse.MountOptions{
					FsName:        "9pfuse",
					Name:          "exportfs",
					DisableXAttrs: true,
					DirectMount:   true,
					Debug:         true,
				},
				EntryTimeout: &timeout,
				AttrTimeout:  &timeout,
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
				cfg.LogLevel,
				nil,
				mountpoint,
				&opts,
			)
		case "nfs":
			return runServer(ctx, nfsAddr, mnt, mountpoint)
		default:
			return fmt.Errorf("unknown mount type: got %#v. Expected 'fuse' or 'nfs'", mountType)
		}
	})
}
