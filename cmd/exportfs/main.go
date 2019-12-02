package main

import (
	"flag"
	"fmt"

	"bazil.org/fuse"
	"github.com/jeffh/cfs/cli"
	efuse "github.com/jeffh/cfs/exportfs/fuse"
	"github.com/jeffh/cfs/ninep"
)

func main() {
	var mountpoint string
	cli.MainClient(func(c ninep.Client, fs *ninep.FileSystemProxy) error {
		if flag.NArg() == 1 {
			return fmt.Errorf("Missing mountpoint")
		} else {
			mountpoint = flag.Arg(1)
		}

		return efuse.MountAndServeFS(
			fs,
			mountpoint,
			fuse.FSName("9pfuse"),
			fuse.Subtype("9pfusefs"),
			fuse.VolumeName("9P Client File System"),
			fuse.NoAppleDouble(),
		)
	})
}
