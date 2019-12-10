package b2fs

import (
	"context"
	"fmt"
	"os"
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
		b.expires = now.Add(15 * time.Minute)
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
	now := time.Now()
	nodes := make([]ninep.Node, 0, len(buckets))
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

////////////////////////////////////////////////////////////////////////////////

type bucketNode struct {
	b2c        *B2Ctx
	bucketName string

	// optional
	bucket *b2.Bucket
	now    time.Time
}

func (b *bucketNode) Info() (os.FileInfo, error) {
	var info os.FileInfo = &ninep.SimpleFileInfo{
		FIName:    b.bucketName,
		FIMode:    os.ModeDir | 0755,
		FIModTime: b.now,
	}
	return info, nil
}

func (b *bucketNode) List() (ninep.NodeIterator, error) {
	nodes := []ninep.Node{
		ninep.StaticDir(
			"objects",
			objectsForBucket(b.b2c, "data", b.bucket, opData, b.now),
			objectsForBucket(b.b2c, "metadata", b.bucket, opMetadata, b.now),
			objectsForBucket(b.b2c, "presigned-download-urls", b.bucket, opPresignedDownloadUrl, b.now),
		),
		objectsForBucket(b.b2c, "versions", b.bucket, opVersions, b.now),
		objectsForBucket(b.b2c, "unfinished-uploads", b.bucket, opUnfinishedUploads, b.now),
		// TODO: /metadata
	}
	return ninep.MakeNodeSliceIterator(nodes), nil
}

func (b *bucketNode) CreateFile(name string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrInvalidAccess
}

func (b *bucketNode) WriteInfo(in os.FileInfo) error               { return ninep.ErrInvalidAccess }
func (b *bucketNode) CreateDir(name string, mode ninep.Mode) error { return ninep.ErrInvalidAccess }
func (b *bucketNode) Delete(name string) error                     { return ninep.ErrInvalidAccess }
