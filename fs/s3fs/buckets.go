package s3fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

////////////////////////////////////////////////////////////////////////////////

func bucketCorsFile(ctx context.Context, s3c *S3Ctx, bucketName string) *ninep.SimpleFile {
	return &ninep.SimpleFile{
		FileInfo: &ninep.SimpleFileInfo{
			FIName: "cors",
			FIMode: 0444,
		},
		OpenFn: func(m ninep.OpenMode) (ninep.FileHandle, error) {
			if m.IsWriteOnly() {
				return nil, ninep.ErrWriteNotAllowed
			}

			rr, rw := io.Pipe()
			if m.IsReadable() {
				input := s3.GetBucketCorsInput{
					Bucket: aws.String(bucketName),
				}
				out, err := s3c.Client.GetBucketCors(ctx, &input)

				canRead := true
				var awsErr smithy.APIError
				if errors.As(err, &awsErr) {
					if awsErr.ErrorCode() == "NoSuchCORSConfiguration" {
						rw.Close()
						canRead = false
						err = nil
					}
				}

				if err != nil {
					return nil, mapAwsErrToNinep(err)
				}

				if canRead {
					go func() {
						defer rw.Close()
						for _, rule := range out.CORSRules {
							pairs := [][2]string{}
							pairs = append(pairs, [2]string{"max_age", fmt.Sprintf("%d", aws.ToInt32(rule.MaxAgeSeconds))})

							values := make([]string, 0, 64)
							{
								for _, h := range rule.AllowedHeaders {
									values = append(values, h)
								}
								pairs = append(pairs, [2]string{"allowed_headers", strings.Join(values, ",")})
								values = values[:0]
							}
							{
								for _, h := range rule.AllowedMethods {
									values = append(values, h)
								}
								pairs = append(pairs, [2]string{"allowed_methods", strings.Join(values, ",")})
								values = values[:0]
							}
							{
								for _, h := range rule.AllowedOrigins {
									values = append(values, h)
								}
								pairs = append(pairs, [2]string{"allowed_origins", strings.Join(values, ",")})
								values = values[:0]
							}
							{
								for _, h := range rule.ExposeHeaders {
									values = append(values, h)
								}
								pairs = append(pairs, [2]string{"expose_headers", strings.Join(values, ",")})
								values = values[:0]
							}

							fmt.Fprintf(rw, "%s\n", kvp.NonEmptyKeyPairs(pairs))
						}
					}()
				}
			}

			return &ninep.RWFileHandle{R: rr}, nil
		},
	}
}
