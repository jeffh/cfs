package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/einkfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	cfg := &service.Config{
		Name:        "einkfs",
		DisplayName: "Eink File System Service",
		Description: "Provides a 9p file system that exposes an eink display",
	}

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(w, "Exposes an eink display as a 9p file server.\n\n")
		fmt.Fprintf(w, "The layout of the filesystem is:\n")
		fmt.Fprintf(w, "\t/display   a png image file of the current display that can be read or written to. Zeros /text on write.\n")
		fmt.Fprintf(w, "\t/text      a string file that is displayed on the display. Can be written to to change the text.\n")
		fmt.Fprintf(w, "\t/metadata  a file with metadata about the display\n")
		fmt.Fprintf(w, "\t/ctl       a device file to control settings of the display\n")
		fmt.Fprintf(w, "\n/ctl commands:\n")
		fmt.Fprintf(w, "\tclear      clear the display\n")
		fmt.Fprintf(w, "\trotate=n   set the rotation of the display to n (0, 1, 2, 3)\n")
		fmt.Fprintf(w, "\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}
	cli.ServiceMain(cfg, func() ninep.FileSystem {
		f, closer, err := einkfs.NewWaveshare2in13v2FS(log.Default())
		if err != nil {
			log.Fatal(err)
		}
		// we just leak memory here, but it's not a big deal?
		_ = closer
		return f
	})
}
