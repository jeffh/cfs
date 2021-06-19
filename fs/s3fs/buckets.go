package s3fs

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

////////////////////////////////

// Represents an s3 bucket. For speed, this attempts to minimize the number of s3 api calls.
//
// Directory tree:
//  /objects/data/<key>
//  /objects/metadata/<key>
//  /objects/presigned-download-urls/<key>
//  /objects/presigned-upload-urls/<key>
//  /acl
//  /cors

type buckets struct {
	ninep.SimpleFileInfo
	s3c *S3Ctx
}

func (b *buckets) Info() (os.FileInfo, error)     { return &b.SimpleFileInfo, nil }
func (b *buckets) WriteInfo(in os.FileInfo) error { return ninep.ErrUnsupported }
func (b *buckets) Delete(name string) error {
	_, err := b.s3c.Client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	return err
}
func (b *buckets) List() (ninep.NodeIterator, error) {
	resp, err := b.s3c.Client.ListBuckets(&s3.ListBucketsInput{})
	fmt.Printf("[S3.buckets.List] ListBuckets()\n")
	if err != nil {
		return nil, err
	}
	var uid string
	if owner := resp.Owner; owner != nil {
		if displayName := owner.DisplayName; displayName != nil {
			uid = *displayName
		}
	}
	now := time.Now()
	nodes := make([]ninep.Node, 0, len(resp.Buckets))
	for _, bucket := range resp.Buckets {
		if bucket != nil && bucket.Name != nil {
			nodes = append(nodes, &bucketNode{
				s3c:        b.s3c,
				bucketName: *bucket.Name,
				bucket:     bucket,
				now:        now,
				uid:        uid,
			})
		}
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}

func (b *buckets) Walk(subpath []string) ([]ninep.Node, error) {
	size := len(subpath)
	if size < 1 {
		return nil, nil
	}

	var (
		nodes []ninep.Node
		err   error
	)

	bucket := subpath[0]
	subpath = subpath[1:]

	bNode := &bucketNode{
		s3c:        b.s3c,
		bucketName: bucket,
	}

	nodes = append(nodes, bNode)
	if size > 1 {
		operation := subpath[0]
		subpath = subpath[1:]
		switch operation {
		case "objects":
			sNode := objectsRoot("objects", b.s3c, bucket)
			nodes = append(nodes, sNode)

			if size > 2 {
				staticDir := subpath[0]
				subpath = subpath[1:]

				objectTree := func(s3c *S3Ctx, subpath []string, dir, bucket string, op objectOperation, nodes []ninep.Node) ([]ninep.Node, error) {
					var err error
					var node ninep.Node = objectsForBucket(s3c, dir, bucket, op)
					nodes = append(nodes, node)
					if size > 3 && (subpath[0] != "." && subpath[0] != "") {
						var objNodes []ninep.Node
						objNodes, err = objectsOrObjectForBucket(s3c, bucket, subpath, op)
						if err == nil {
							nodes = append(nodes, objNodes...)
						}
					} else {
						// repeat if "." is at the end...
						for _, p := range subpath {
							if p == "." {
								nodes = append(nodes, node)
							} else {
								break
							}
						}
					}
					return nodes, err
				}

				switch staticDir {
				case dirObjectData:
					nodes, err = objectTree(b.s3c, subpath, dirObjectData, bucket, opData, nodes)
				case dirObjectPresignedDownloadUrls:
					nodes, err = objectTree(b.s3c, subpath, dirObjectPresignedDownloadUrls, bucket, opPresignedDownloadUrl, nodes)
				case dirObjectPresignedUploadUrl:
					nodes, err = objectTree(b.s3c, subpath, dirObjectPresignedUploadUrl, bucket, opPresignedUploadUrl, nodes)
				case dirObjectMetadata:
					nodes, err = objectTree(b.s3c, subpath, dirObjectMetadata, bucket, opMetadata, nodes)
				case ".", "":
					nodes = append(nodes, sNode)
				default:
					err = os.ErrNotExist
				}
			}
		case "acl":
			nodes = append(nodes, bucketAclFile(b.s3c, bucket))
		case "cors":
			nodes = append(nodes, bucketCorsFile(b.s3c, bucket))
		case ".", "":
			nodes = append(nodes, bNode)
		default:
			err = os.ErrNotExist
		}
	}

	if err != nil {
		return nil, err
	} else {
		return nodes, err
	}

}

func (b *buckets) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ErrUseMkDirToCreateBucket
}
func (b *buckets) CreateDir(name string, mode ninep.Mode) error {
	_, err := b.s3c.Client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(name),
	})
	fmt.Printf("[S3] CreateBucket(%#v) -> %v\n", name, err)
	return mapAwsErrToNinep(err)
}

////////////////////////////////////////////////////////////////////////////////

func bucketAclFile(s3c *S3Ctx, bucketName string) *ninep.SimpleFile {
	return &ninep.SimpleFile{
		FileInfo: &ninep.SimpleFileInfo{
			FIName: "acl",
			FIMode: 0666,
		},
		OpenFn: func(m ninep.OpenMode) (ninep.FileHandle, error) {
			wr, ww := io.Pipe()
			if m.IsWriteable() {
				go func() {
					defer wr.Close()
					in := bufio.NewReaderSize(wr, 4096)
					for {
						line, isPrefix, err := in.ReadLine()
						if err == io.EOF || err == io.ErrUnexpectedEOF {
							return
						}
						if err == nil {
							if isPrefix {
								// if too long, ignore that line
								for isPrefix {
									_, isPrefix, err = in.ReadLine()
									if err == io.EOF || err == io.ErrUnexpectedEOF {
										return
									}
								}
							} else {
								kv := kvp.ParseKeyValues(string(line))
								input := s3.PutBucketAclInput{
									Bucket: aws.String(bucketName),
								}
								if acl := kv.GetOne("acl"); acl != "" {
									input.ACL = aws.String(acl)
								}
								if grantFullControl := kv.GetOne("grant_full_control"); grantFullControl != "" {
									input.GrantFullControl = aws.String(grantFullControl)
								}
								if grantRead := kv.GetOne("grant_read"); grantRead != "" {
									input.GrantRead = aws.String(grantRead)
								}
								if grantReadACP := kv.GetOne("grant_read_acp"); grantReadACP != "" {
									input.GrantReadACP = aws.String(grantReadACP)
								}
								if grantWrite := kv.GetOne("grant_write"); grantWrite != "" {
									input.GrantWrite = aws.String(grantWrite)
								}
								if grantWriteACP := kv.GetOne("grant_write_acp"); grantWriteACP != "" {
									input.GrantWriteACP = aws.String(grantWriteACP)
								}

								_, err := s3c.Client.PutBucketAcl(&input)
								fmt.Printf("[S3] PutBucketAcl(%#v) -> %v\n", input, err)
								if err != nil {
									return
								}
							}
						}
					}
				}()
			}

			rr, rw := io.Pipe()
			if m.IsReadable() {
				input := s3.GetBucketAclInput{
					Bucket: aws.String(bucketName),
				}
				out, err := s3c.Client.GetBucketAcl(&input)
				if err != nil {
					return nil, mapAwsErrToNinep(err)
				}

				go func() {
					defer rw.Close()
					for _, grant := range out.Grants {
						pairs := [][2]string{}
						pairs = append(pairs, [2]string{"permission", aws.StringValue(grant.Permission)})

						if g := grant.Grantee; g != nil {
							pairs = append(pairs, [2]string{"id", aws.StringValue(g.ID)})
							pairs = append(pairs, [2]string{"name", aws.StringValue(g.DisplayName)})
							pairs = append(pairs, [2]string{"email", aws.StringValue(g.EmailAddress)})
							pairs = append(pairs, [2]string{"type", aws.StringValue(g.Type)})
							pairs = append(pairs, [2]string{"uri", aws.StringValue(g.URI)})
						}
						fmt.Fprintf(rw, "%s\n", kvp.NonEmptyKeyPairs(pairs))
					}
				}()
			}

			return &ninep.RWFileHandle{R: rr, W: ww}, nil
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func bucketCorsFile(s3c *S3Ctx, bucketName string) *ninep.SimpleFile {
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
				out, err := s3c.Client.GetBucketCors(&input)

				canRead := true
				if e, ok := err.(awserr.Error); ok {
					if e.Code() == "NoSuchCORSConfiguration" {
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
							if rule == nil {
								continue
							}
							pairs := [][2]string{}
							pairs = append(pairs, [2]string{"max_age", fmt.Sprintf("%d", aws.Int64Value(rule.MaxAgeSeconds))})

							values := make([]string, 0, 64)
							{
								for _, h := range rule.AllowedHeaders {
									values = append(values, aws.StringValue(h))
								}
								pairs = append(pairs, [2]string{"allowed_headers", strings.Join(values, ",")})
								values = values[:0]
							}
							{
								for _, h := range rule.AllowedMethods {
									values = append(values, aws.StringValue(h))
								}
								pairs = append(pairs, [2]string{"allowed_methods", strings.Join(values, ",")})
								values = values[:0]
							}
							{
								for _, h := range rule.AllowedOrigins {
									values = append(values, aws.StringValue(h))
								}
								pairs = append(pairs, [2]string{"allowed_origins", strings.Join(values, ",")})
								values = values[:0]
							}
							{
								for _, h := range rule.ExposeHeaders {
									values = append(values, aws.StringValue(h))
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

////////////////////////////////////////////////////////////////////////////////

type bucketNode struct {
	// required
	s3c        *S3Ctx
	bucketName string

	// optional
	bucket *s3.Bucket
	uid    string
	now    time.Time
}

func (b *bucketNode) Info() (os.FileInfo, error) {
	// if b.bucket == nil {
	// 	// we only have Listing all buckets to find the one we need...
	// 	resp, err := b.s3c.Client.ListBuckets(&s3.ListBucketsInput{})
	// 	fmt.Printf("[S3.bucket.Info] ListBuckets() | %v %p\n", b.bucketName, b)
	// 	if err != nil {
	// 		return nil, mapAwsErrToNinep(err)
	// 	}
	// 	var uid string
	// 	if owner := resp.Owner; owner != nil {
	// 		if displayName := owner.DisplayName; displayName != nil {
	// 			uid = *displayName
	// 		}
	// 	}
	// 	now := time.Now()
	// 	for _, bucket := range resp.Buckets {
	// 		if bucket != nil && bucket.Name != nil {
	// 			b.uid = uid
	// 			b.now = now
	// 			b.bucket = bucket
	// 			break
	// 		}
	// 	}
	// 	if b.bucket == nil {
	// 		return nil, os.ErrNotExist
	// 	}
	// }
	var date time.Time
	if b.bucket != nil && b.bucket.CreationDate != nil {
		date = *b.bucket.CreationDate
	} else {
		date = b.now
	}
	var info os.FileInfo = &ninep.SimpleFileInfo{
		FIName:    b.bucketName,
		FIMode:    os.ModeDir | 0777,
		FIModTime: date,
	}
	if b.uid != "" {
		info = ninep.FileInfoWithUsers(info, b.uid, "", "")
	}
	return info, nil
}

func (b *bucketNode) List() (ninep.NodeIterator, error) {
	nodes := []ninep.Node{
		objectsForBucket(b.s3c, "objects", b.bucketName, opData),
		bucketAclFile(b.s3c, b.bucketName),
		bucketCorsFile(b.s3c, b.bucketName),
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}

func (b *bucketNode) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrInvalidAccess
}

func (b *bucketNode) WriteInfo(in os.FileInfo) error               { return ninep.ErrInvalidAccess }
func (b *bucketNode) CreateDir(name string, mode ninep.Mode) error { return ninep.ErrInvalidAccess }
func (b *bucketNode) Delete(name string) error                     { return ninep.ErrInvalidAccess }
