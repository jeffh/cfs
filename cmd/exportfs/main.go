package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/jeffh/cfs/cli"
	ebilly "github.com/jeffh/cfs/exportfs/billy"
	efuse "github.com/jeffh/cfs/exportfs/fuse"
	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	"github.com/willscott/go-nfs"
	"github.com/willscott/go-nfs/helpers"
)

func main() {
	var mountType string
	var mountpoint string
	var nfsAddr string

	flag.StringVar(&mountType, "type", "fuse", "Mount type to use, defaults to 'fuse', but can also be 'nfs'")
	flag.StringVar(&nfsAddr, "nfs-addr", "127.0.0.1:0", "Address for NFS to listen on")
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
			listener, err := net.Listen("tcp", nfsAddr)
			if err != nil {
				return err
			}
			cli.OnInterrupt(func() { listener.Close() })
			defer listener.Close()
			fmt.Printf("Listening on %s\n", listener.Addr())
			handler := helpers.NewCachingHandler(helpers.NewNullAuthHandler(ebilly.ToBillyFS(mnt)))
			srv := nfs.Server{Handler: handler}
			srv.Context = ctx
			err = srv.Serve(listener)
			if strings.Contains(err.Error(), "use of closed network connection") {
				err = nil
			}
			return err
		default:
			return fmt.Errorf("Unknown mount type: got %#v. Expected 'fuse' or 'nfs'", mountType)
		}
	})
}
