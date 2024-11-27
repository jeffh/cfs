package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs"
	"github.com/jeffh/cfs/fs/cachefs"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
)

func main() {
	var (
		fromAddr      string
		writeDelay    time.Duration
		dataCacheSize int
		dirCacheSize  int
		statCacheSize int
		delay         time.Duration
		asyncWrites   bool
	)

	cltCfg := cli.ClientConfig{PrintPrefix: "client"}
	srvCfg := cli.ServerConfig{PrintPrefix: "server"}

	cltCfg.SetFlags(nil)
	srvCfg.SetFlags(nil)

	flag.StringVar(&fromAddr, "fromfs", "", "Address of source file system to proxy")
	flag.DurationVar(&writeDelay, "write-delay", time.Second, "Delay before writing changes to source")
	flag.IntVar(&dataCacheSize, "data-cache-size", 1024, "Maximum data cache size roughly in number of 4kb pages")
	flag.IntVar(&dirCacheSize, "dir-cache-size", 256, "Maximum directories to cache")
	flag.IntVar(&statCacheSize, "stat-cache-size", 1024, "Maximum file stats to cache")
	flag.DurationVar(&delay, "delay", 0, "Delay before each operation from fromfs")
	flag.BoolVar(&asyncWrites, "async-writes", false, "Asynchronously writes to source")
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [OPTIONS]\n\n", os.Args[0])
		fmt.Fprintf(w, "Caches a 9p file system for faster access.\n\n")
		fmt.Fprintf(w, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	flag.Parse()
	cltCfg.LogLevel = srvCfg.LogLevel
	cltCfg.Logger = srvCfg.Logger

	if fromAddr == "" {
		fmt.Fprintf(os.Stderr, "Error: -fromfs is required\n")
		flag.Usage()
		os.Exit(1)
	}
	client, fsy, err := cltCfg.CreateFs(fromAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to source fs: %s\n", err)
		os.Exit(1)
	}

	defer client.Close()

	createcfg := func(stdout, stderr io.Writer) cli.ServerConfig {
		srvCfg.Stdout = stdout
		srvCfg.Stderr = stderr
		return srvCfg
	}

	createfs := func() ninep.FileSystem {
		logger := ninep.CreateLogger(srvCfg.LogLevel, srvCfg.PrintPrefix, srvCfg.Logger)

		opts := []cachefs.Options{
			cachefs.WithMaxDataEntries(dataCacheSize),
			cachefs.WithMaxDirsCached(dirCacheSize),
			cachefs.WithMaxStatCache(statCacheSize),
			cachefs.WithAsyncWrites(asyncWrites),
			cachefs.WithLogger(logger),
		}

		var fsys ninep.FileSystem
		fsys = fsy
		if delay > 0 {
			fsys = fs.NewDelayFS(fsys, delay)
		}

		cfs := cachefs.New(fsys, client, opts...)
		fmt.Printf("Caching enabled for %s (write delay: %v, cache size: %d bytes)\n",
			fromAddr, writeDelay, dataCacheSize)
		return cfs
	}
	cli.ServiceMainWithFactory(createcfg, createfs)
}
