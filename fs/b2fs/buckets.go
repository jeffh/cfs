package b2fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/kurin/blazer/b2"
)

// Represents an b2 bucket. For speed, this attempts to minimize the number of b2 api calls.
//
// Directory tree:
//  /<bucket_type>/ where <bucket_type> = "all" | "public" | "private" | "snapshot"
//  /<bucket_type>/<bucket>/objects/data/<key> -- b2_download_file_by_name | b2_upload_file | b2_copy_file (for renames)
//  /<bucket_type>/<bucket>/objects/metadata/<key> -- b2_get_file_info | b2_copy_file (for renames)
//  /<bucket_type>/<bucket>/metadata -- b2_update_bucket
type buckets struct {
	ninep.SimpleFileInfo
	b2c        *B2Ctx
	bucketType string

	m       sync.Mutex
	cache   []*b2.Bucket // mutation of slice is not allowed
	nodes   []ninep.Node
	expires time.Time
}

// blazer always calls listbuckets for all buckets without any caching. And since the api doesn't allow
// direct object access if the bucket is already known.
func (b *buckets) getBuckets(ignoreCache bool) ([]*b2.Bucket, error) {
	var err error
	now := time.Now()
	ctx := context.Background()

	b.m.Lock()
	if now.After(b.expires) || b.cache == nil || ignoreCache {
		var bkts []*b2.Bucket
		bkts, err = b.b2c.Client.ListBuckets(ctx)
		fmt.Printf("[B2.buckets.List] ListBuckets()\n")
		if err == nil {
			b.cache = bkts
		}
		b.nodes = nil
		b.expires = now.Add(b.b2c.Expiration)
	}
	b.m.Unlock()
	return b.cache, err
}

func (b *buckets) invalidateBucketsCache() {
	b.m.Lock()
	b.cache = nil
	b.m.Unlock()
}

func (b *buckets) getBucket(name string) (*b2.Bucket, error) {
	bkts, err := b.getBuckets(false)
	if err != nil {
		return nil, err
	}
	for _, bkt := range bkts {
		if bkt.Name() == name {
			return bkt, nil
		}
	}
	return nil, os.ErrNotExist
}

func (b *buckets) Info() (os.FileInfo, error)     { return &b.SimpleFileInfo, nil }
func (b *buckets) WriteInfo(in os.FileInfo) error { return ninep.ErrUnsupported }
func (b *buckets) Delete(name string) error {
	ctx := context.Background()
	bkt, err := b.b2c.Client.Bucket(ctx, name)
	if err != nil {
		return mapB2ErrToNinep(err)
	}
	return mapB2ErrToNinep(bkt.Delete(ctx))
}

func (b *buckets) List() (ninep.NodeIterator, error) {
	buckets, err := b.getBuckets(false) // TODO: set true once we implement walk
	if err != nil {
		return nil, err
	}
	b.m.Lock()
	defer b.m.Unlock()
	var nodes []ninep.Node
	if b.nodes == nil {
		now := time.Now()
		nodes = make([]ninep.Node, 0, len(buckets))
		for _, bucket := range buckets {
			if bucket != nil {
				nodes = append(nodes, &bucketNode{
					b2c:        b.b2c,
					bucketName: bucket.Name(),
					bucket:     bucket,
					now:        now,
				})
			}
		}
		if b.b2c.CacheMeta {
			b.nodes = nodes
		}
	} else {
		nodes = b.nodes
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}

func (b *buckets) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ErrUseMkDirToCreateBucket
}

func (b *buckets) CreateDir(name string, mode ninep.Mode) error {
	ctx := context.Background()
	bType := b2.BucketType(b.bucketType)
	if bType != "allPublic" {
		bType = "allPrivate"
	}
	_, err := b.b2c.Client.NewBucket(ctx, name, &b2.BucketAttrs{Type: bType})
	fmt.Printf("[S3] CreateBucket(%#v) -> %v\n", name, err)
	b.invalidateBucketsCache()
	return mapB2ErrToNinep(err)
}

func (b *buckets) Walk(subpath []string) ([]ninep.Node, error) {
	return ninep.WalkPassthrough(b, subpath)
}

////////////////////////////////////////////////////////////////////////////////

type bucketNode struct {
	b2c        *B2Ctx
	bucketName string

	// optional
	bucket *b2.Bucket
	now    time.Time

	// internal
	nodes []ninep.Node
}

func (b *bucketNode) Info() (os.FileInfo, error) {
	var info os.FileInfo = &ninep.SimpleFileInfo{
		FIName:    b.bucketName,
		FIMode:    os.ModeDir | 0755,
		FIModTime: b.now,
	}
	return info, nil
}

func bucketTypeString(t b2.BucketType) string {
	switch t {
	case b2.Private:
		return "private"
	case b2.Public:
		return "public"
	case b2.Snapshot:
		return "snapshot"
	case b2.UnknownType:
		fallthrough
	default:
		return "unknown"
	}
}

func parseBucketType(s string) b2.BucketType {
	switch s {
	case "private":
		return b2.Private
	case "public":
		return b2.Public
	case "snapshot":
		return b2.Snapshot
	case "unknown":
		fallthrough
	default:
		return b2.UnknownType
	}
}

func (b *bucketNode) List() (ninep.NodeIterator, error) {
	var nodes []ninep.Node
	if b.nodes == nil {
		nodes = []ninep.Node{
			ninep.StaticDir(
				"objects",
				objectsForBucket(b.b2c, "data", b.bucket, opData, b.now),
				objectsForBucket(b.b2c, "metadata", b.bucket, opMetadata, b.now),
				objectsForBucket(b.b2c, "presigned-download-urls", b.bucket, opPresignedDownloadUrl, b.now),
			),
			// doesn't seem to work
			// objectsForBucket(b.b2c, "versions", b.bucket, opVersions, b.now),
			// objectsForBucket(b.b2c, "unfinished-uploads", b.bucket, opUnfinishedUploads, b.now),
			// TODO: /metadata
			ninep.CtlFile("metadata", 755, b.now, func(m ninep.OpenMode, r io.Reader, w io.Writer) {
				ctx := context.Background()

				read := func(ctx context.Context, b *b2.Bucket, w io.Writer) {
					attrs, err := b.Attrs(ctx)
					if err != nil {
						fmt.Fprintf(w, "%s\n", ninep.KeyPair("error", err.Error()))
					} else {
						kp := [][2]string{{"type", bucketTypeString(attrs.Type)}}
						for k, v := range attrs.Info {
							kp = append(kp, [2]string{fmt.Sprintf("info-%s", k), v})
						}
						kp = append(kp, [2]string{"lifecycle-count", strconv.Itoa(len(attrs.LifecycleRules))})
						for _, rule := range attrs.LifecycleRules {
							kp = append(kp, [2]string{"lifecycle-prefix", rule.Prefix})
							kp = append(kp, [2]string{"lifecycle-days-new-until-hidden", strconv.Itoa(rule.DaysNewUntilHidden)})
							kp = append(kp, [2]string{"lifecycle-days-new-until-hidden", strconv.Itoa(rule.DaysHiddenUntilDeleted)})
						}
						fmt.Fprintf(w, "%s\n", ninep.KeyPairs(kp))
					}
				}

				if m.IsWriteable() {
					const maxLineSize = 8192
					ninep.ProcessKeyValuesLineLoop(r, maxLineSize, func(k ninep.KVMap, err error) bool {
						fmt.Printf("err: %s\n", err)
						if err != nil {
							fmt.Fprintf(w, "%s\n", ninep.KeyPair("error", err.Error()))
							return false
						}

						switch k.GetOne("op") {
						case "read":
							read(ctx, b.bucket, w)
						case "write":
							var attrs b2.BucketAttrs
							attrs.Type = parseBucketType(k.GetOne("type"))
							attrs.Info = k.GetOnePrefixOrNil("info-")
							if k.GetOneBool("empty-info") {
								attrs.Info = make(map[string]string)
							}
							if k.Has("lifecycle-count") {
								size := int(k.GetOneInt64("lifecycle-count"))
								attrs.LifecycleRules = make([]b2.LifecycleRule, 0, size)
								prefixes := k.GetAll("lifecycle-prefix")
								daysNewUntilHiddens := k.GetAllInts("lifecycle-days-new-until-hidden")
								daysHiddenUntilDeleted := k.GetAllInts("lifecycle-days-hidden-until-deleted")

								for i := 0; i < size; i++ {
									attrs.LifecycleRules = append(attrs.LifecycleRules, b2.LifecycleRule{
										Prefix:                 prefixes[i],
										DaysNewUntilHidden:     daysNewUntilHiddens[i],
										DaysHiddenUntilDeleted: daysHiddenUntilDeleted[i],
									})
								}
							}
							fmt.Printf("[B2] BucketUpload(%#v)\n", attrs)
							err := b.bucket.Update(ctx, &attrs)
							if err != nil {
								fmt.Fprintf(w, "%s\n", ninep.KeyPair("error", err.Error()))
							} else {
								fmt.Fprintf(w, "%s\n", ninep.KeyPair("success", "ok"))
							}
						case "close", "quit", "done":
							return false
						default:
							fmt.Fprintf(w, "%s\n", ninep.KeyPair("error", "invalid op, choose 'read' or 'write'"))
						}

						return true
					})
				} else if m.IsReadable() {
					read(ctx, b.bucket, w)
				}
			}),
		}
		if b.b2c.CacheMeta {
			b.nodes = nodes
		}
	} else {
		nodes = b.nodes
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}

func (b *bucketNode) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrInvalidAccess
}

func (b *bucketNode) WriteInfo(in os.FileInfo) error               { return ninep.ErrInvalidAccess }
func (b *bucketNode) CreateDir(name string, mode ninep.Mode) error { return ninep.ErrInvalidAccess }
func (b *bucketNode) Delete(name string) error                     { return ninep.ErrInvalidAccess }
