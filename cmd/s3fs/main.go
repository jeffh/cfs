package main

import (
	"flag"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/s3fs"
	"github.com/jeffh/cfs/ninep"
	"github.com/kardianos/service"
)

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func main() {
	var (
		endpoint string
		flatten  bool
	)

	flag.BoolVar(&flatten, "flatten", false, "Truncate the directory listing to only show the first level of directories instead of key names")
	flag.StringVar(&endpoint, "endpoint", "", "The S3 endpoint to use, defaults to AWS S3's builtin endpoint.")

	cfg := &service.Config{
		Name:        "s3fs",
		DisplayName: "S3 File System Service",
		Description: "Provides a 9p file system that exposes buckets on AWS S3 (or compatible service)",
	}
	cli.ServiceMain(cfg, func() ninep.FileSystem {
		return s3fs.NewBasicFs(endpoint, !flatten)
	})
}
