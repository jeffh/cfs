package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/rssfs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func main() {
	cfg := &service.Config{
		Name:        "rssfs",
		DisplayName: "RSS File System Service",
		Description: "Provides a 9p file system of a given rss feed",
	}

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS] URL\n\n", os.Args[0])
		fmt.Fprintf(w, "Exposes an RSS feed url as a 9p file server.\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}
	cli.ServiceMain(cfg, func() ninep.FileSystem {
		if len(os.Args) < 2 {
			flag.Usage()
			log.Fatalf("Expected to receive url of feed to load")
		}
		fs, err := rssfs.NewFsFromURL(os.Args[1])
		if err != nil {
			log.Fatalf("failed to create dockerfs: %s", err)
		}
		return fs
	})
}
