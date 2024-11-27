package main

import (
	"flag"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/s3fs"
	"github.com/jeffh/cfs/ninep"
	_ "go.uber.org/automaxprocs"
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

	cli.ServiceMain(func() ninep.FileSystem {
		return s3fs.New(endpoint, flatten)
	})
}
