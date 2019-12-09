package main

import (
	"flag"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/s3fs"
	"github.com/jeffh/cfs/ninep"
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
	)

	flag.StringVar(&endpoint, "endpoint", "", "The S3 endpoint to use, defaults to AWS S3's builtin endpoint.")

	cli.BasicServerMain(func() ninep.FileSystem {
		return s3fs.NewBasicFs(endpoint)
	})
}
