package main

import (
	"context"
	"flag"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/config"
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
		endpoint         string
		flatten          bool
		forceS3PathStyle bool
	)

	flag.BoolVar(&flatten, "flatten", false, "Truncate the directory listing to only show the first level of directories instead of key names")
	flag.StringVar(&endpoint, "endpoint", "", "The S3 endpoint to use, defaults to AWS S3's builtin endpoint.")
	flag.BoolVar(&forceS3PathStyle, "s3-path-style", false, "If true, uses s3 path styles for buckets instead of domains; useful for some alternative s3 implementations")

	cli.ServiceMainWithLogger(func(L *slog.Logger) ninep.FileSystem {
		cfg, err := config.LoadDefaultConfig(
			context.Background(),
			config.WithBaseEndpoint(endpoint),
		)
		if err != nil {
			panic(err)
		}
		return s3fs.NewWithAwsConfig(cfg, s3fs.Options{
			Flatten:     flatten,
			Logger:      L,
			S3PathStyle: forceS3PathStyle,
		})
	})
}
