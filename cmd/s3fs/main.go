package main

import (
	"flag"

	"github.com/jeffh/cfs/cli"
	"github.com/jeffh/cfs/fs/s3fs"
	"github.com/jeffh/cfs/ninep"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
		cfg := &aws.Config{
			Endpoint: stringPtrOrNil(endpoint),
		}
		sess := session.Must(session.NewSession())
		svc := s3.New(sess, cfg)

		return s3fs.NewFs(svc)
	})
}
