// Implements a 9p file system that talks to Amazon's S3 Object Storage
// Service.
//
// Also supports any S3-compatible service as well.
package s3fs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

var mx = ninep.NewMux().
	Define().Path("/").As("root").
	Define().Path("/ctl").As("ctl").
	Define().Path("/buckets").TrailSlash().As("buckets").
	Define().Path("/buckets/{bucket}").TrailSlash().As("bucket").
	Define().Path("/buckets/{bucket}/ctl").As("bucketCtl").
	Define().Path("/buckets/{bucket}/objects").TrailSlash().As("objects").
	Define().Path("/buckets/{bucket}/objects/{key*}").Attr("dir", "objects").As("objectByKey").
	Define().Path("/buckets/{bucket}/metadata").TrailSlash().As("metadata").
	Define().Path("/buckets/{bucket}/metadata/{key*}").Attr("dir", "metadata").As("metadataByKey").
	Define().Path("/buckets/{bucket}/sign").TrailSlash().As("sign").
	Define().Path("/buckets/{bucket}/sign/expires").As("signExpires").
	Define().Path("/buckets/{bucket}/sign/download_url").TrailSlash().As("signDownload").
	Define().Path("/buckets/{bucket}/sign/download_url/{key*}").As("signDownloadByKey").
	Define().Path("/buckets/{bucket}/sign/upload").TrailSlash().As("signUpload").
	Define().Path("/buckets/{bucket}/sign/upload/key").As("signUploadKey").
	Define().Path("/buckets/{bucket}/sign/upload/url").As("signUploadUrl").
	Define().Path("/buckets/{bucket}/acl").As("bucketAcl")

func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type S3Ctx struct {
	Session *session.Session
	Client  *s3.S3
}

// Reasonable default configuration of NewFs(), an empty string of endpoint
// defaults to AWS' S3 service
//
// Parameters:
//   - endpoint defaults to AWS' S3 service if it is an empty string.
//   - flatten makes listing an object key prefix returns the full keys instead of
//     just the logical directory names. This can be more efficient to use S3
//     API at the cost of breaking some of the file system abstraction layer.
func New(endpoint string, flatten bool) ninep.FileSystem {
	cfg := &aws.Config{
		Endpoint: stringPtrOrNil(endpoint),
	}
	sess := session.Must(session.NewSession())
	svc := s3.New(sess, cfg)
	ctx := S3Ctx{
		Session: sess,
		Client:  svc,
	}

	return NewWithClient(&ctx, flatten)
}

func NewWithClient(s3c *S3Ctx, flatten bool) ninep.FileSystem {
	trace := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return newFs(s3c, fsysOptions{
		Logger:          trace,
		BucketCacheSize: -1,
		ObjectCacheSize: -1,
	})
}

////////////////////////////////////////////////////////////////////////////////

type fsys struct {
	s3c      *S3Ctx
	logger   *slog.Logger
	listKeys bool // set to true to list keys in directory hierarchies instead of all keys matching prefix

	mu        sync.Mutex
	expires   map[string]time.Duration
	uploadKey map[string]string

	bucketCache *expirable.LRU[string, fs.FileInfo]
	objCache    *expirable.LRU[string, fs.FileInfo]
}

type fsysOptions struct {
	Logger          *slog.Logger
	ListKeys        bool
	BucketCacheSize int
	ObjectCacheSize int
	BucketTTL       time.Duration
	ObjectTTL       time.Duration
}

func newFs(s3c *S3Ctx, opts fsysOptions) *fsys {
	if opts.BucketTTL == 0 {
		opts.BucketTTL = 24 * time.Hour
	}
	if opts.ObjectTTL == 0 {
		opts.ObjectTTL = 1 * time.Hour
	}
	if opts.BucketCacheSize < 0 {
		opts.BucketCacheSize = 32
	}
	if opts.ObjectCacheSize < 0 {
		opts.ObjectCacheSize = 512
	}
	return &fsys{
		s3c:         s3c,
		logger:      opts.Logger,
		listKeys:    opts.ListKeys,
		expires:     make(map[string]time.Duration),
		uploadKey:   make(map[string]string),
		bucketCache: expirable.NewLRU[string, fs.FileInfo](opts.BucketCacheSize, nil, opts.BucketTTL),
		objCache:    expirable.NewLRU[string, fs.FileInfo](opts.ObjectCacheSize, nil, opts.ObjectTTL),
	}
}

func (f *fsys) getUploadKey(bucket string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.uploadKey[bucket]
}

func (f *fsys) getSigningExpiry(bucket string) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.expires[bucket]
	if !ok {
		return 15 * time.Minute
	}
	return d
}

func (f *fsys) objectCacheKey(bucket, key string) string {
	key = strings.TrimPrefix(key, "/")
	return bucket + "/" + key
}

func (f *fsys) getCachedObject(bucket, key string) fs.FileInfo {
	key = f.objectCacheKey(bucket, key)
	if info, ok := f.objCache.Get(key); ok {
		return info
	}
	return nil
}

func (f *fsys) setCachedObject(bucket, key string, info fs.FileInfo) {
	key = f.objectCacheKey(bucket, key)
	f.objCache.Add(key, info)
}

func (f *fsys) evictCachedObject(bucket, key string) {
	key = f.objectCacheKey(bucket, key)
	f.objCache.Remove(key)
}

func (f *fsys) evictCachedObjectsForBucket(bucket string) {
	var keysToRemove []string
	prefix := bucket + "/"
	for _, key := range f.objCache.Keys() {
		if strings.HasPrefix(key, prefix) {
			keysToRemove = append(keysToRemove, key)
		}
	}
	for _, key := range keysToRemove {
		f.objCache.Remove(key)
	}
}

func (f *fsys) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return fs.ErrPermission
	}
	switch res.Id {
	case "root":
		return nil
	case "bucket":
		bucket := res.Vars[0]
		_, err := f.s3c.Client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		if f.logger != nil {
			f.logger.InfoContext(ctx, "S3.CreateBucket", slog.String("bucket", bucket), slog.Any("err", err))
		}
		return mapAwsErrToNinep(err)
	case "objectByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		if !strings.HasSuffix(key, "/") {
			key += "/"
		}
		_, err := f.s3c.Client.PutObject(&s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		return mapAwsErrToNinep(err)
	// case "metadataByKey":
	// 	return nil
	default:
		return ninep.ErrWriteNotAllowed
	}
}

func (f *fsys) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return nil, fs.ErrPermission
	}
	switch res.Id {
	case "bucket":
		return nil, ErrUseMkDirToCreateBucket
	case "objectByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		key = strings.TrimSuffix(key, "/")
		h := &ninep.RWFileHandle{}
		if flag.IsReadable() {
			r, w := io.Pipe()
			go func() {
				defer r.Close()
				resp, err := f.s3c.Client.GetObject(&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if f.logger != nil {
					f.logger.InfoContext(ctx, "S3.GetObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
				}
				if err != nil {
					if isNoSuchBucket(err) {
						f.bucketCache.Remove(bucket)
						f.evictCachedObjectsForBucket(bucket)
					}
					w.CloseWithError(mapAwsErrToNinep(err))
				} else {
					defer resp.Body.Close()
					_, err = io.Copy(w, resp.Body)
					w.CloseWithError(mapAwsErrToNinep(err))
				}
			}()
			h.R = r
		}
		if flag.IsWriteable() {
			r, w := io.Pipe()
			go func() {
				defer r.Close()
				uploader := s3manager.NewUploader(f.s3c.Session)
				_, err := uploader.Upload(&s3manager.UploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   r,
				})
				if f.logger != nil {
					f.logger.InfoContext(ctx, "S3.UploadObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
				}
				w.CloseWithError(mapAwsErrToNinep(err))
			}()
			h.W = w
		}
		return h, nil
	default:
		return nil, fs.ErrPermission
	}
}

func (f *fsys) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	switch res.Id {
	case "bucketCtl":
		bucket := res.Vars[0]
		return f.bucketCtlFile(ctx, bucket, flag)
	case "bucketAcl":
		bucket := res.Vars[0]
		return f.bucketAclFile(ctx, bucket, flag)
	case "signExpires":
		bucket := res.Vars[0]
		return f.signExpiresFile(ctx, bucket, flag)
	case "signUploadKey":
		bucket := res.Vars[0]
		return f.signUploadKeyFile(bucket, flag)
	case "signUploadUrl":
		bucket := res.Vars[0]
		return f.signUploadUrlFile(bucket, flag)
	case "objectByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		key = strings.TrimSuffix(key, "/")
		h := &ninep.RWFileHandle{}
		if flag.IsReadable() {
			r, w := io.Pipe()
			go func() {
				defer r.Close()
				resp, err := f.s3c.Client.GetObject(&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if f.logger != nil {
					f.logger.InfoContext(ctx, "S3.GetObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
				}
				if err != nil {
					if isNoSuchBucket(err) {
						f.bucketCache.Remove(bucket)
						f.evictCachedObjectsForBucket(bucket)
					}
					w.CloseWithError(mapAwsErrToNinep(err))
				} else {
					defer resp.Body.Close()
					_, err = io.Copy(w, resp.Body)
					w.CloseWithError(mapAwsErrToNinep(err))
				}
			}()
			h.R = r
		}
		if flag.IsWriteable() {
			r, w := io.Pipe()
			go func() {
				defer r.Close()
				uploader := s3manager.NewUploader(f.s3c.Session)
				_, err := uploader.Upload(&s3manager.UploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   r,
				})
				if f.logger != nil {
					f.logger.InfoContext(ctx, "S3.UploadObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
				}
				w.CloseWithError(mapAwsErrToNinep(err))
			}()
			h.W = w
		}
		return h, nil
	case "metadataByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		key = strings.TrimSuffix(key, "/")
		h := &ninep.RWFileHandle{}
		if flag.IsReadable() {
			r, w := io.Pipe()
			err := f.writeMetadata(ctx, bucket, key, w)
			if err != nil {
				r.Close()
				return nil, err
			}
			h.R = r
		}
		if flag.IsWriteable() {
			r, w := io.Pipe()
			go func() {
				buf, err := io.ReadAll(r)
				if err != nil {
					w.CloseWithError(mapAwsErrToNinep(err))
				} else {
					kv := kvp.MustParseKeyValues(string(buf))
					input := s3.PutObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(key),

						ACL:                       stringPtrIfNotEmpty(kv.GetOne("acl")),
						CacheControl:              stringPtrIfNotEmpty(kv.GetOne("cache-control")),
						ContentDisposition:        stringPtrIfNotEmpty(kv.GetOne("content-disposition")),
						ContentEncoding:           stringPtrIfNotEmpty(kv.GetOne("content-encoding")),
						ContentLanguage:           stringPtrIfNotEmpty(kv.GetOne("content-language")),
						ContentLength:             int64PtrIfNotEmpty(kv.GetOne("content-length")),
						ContentMD5:                stringPtrIfNotEmpty(kv.GetOne("content-md5")),
						ContentType:               stringPtrIfNotEmpty(kv.GetOne("content-type")),
						GrantFullControl:          stringPtrIfNotEmpty(kv.GetOne("grant-full-control")),
						GrantRead:                 stringPtrIfNotEmpty(kv.GetOne("grant-read")),
						GrantReadACP:              stringPtrIfNotEmpty(kv.GetOne("grant-read-acp")),
						GrantWriteACP:             stringPtrIfNotEmpty(kv.GetOne("grant-write-acp")),
						Metadata:                  mapPtrIfNotEmpty(kv.GetAllPrefix("metadata-")),
						ObjectLockLegalHoldStatus: stringPtrIfNotEmpty(kv.GetOne("object-lock-legal-hold-status")),
						ObjectLockMode:            stringPtrIfNotEmpty(kv.GetOne("object-lock-mode")),
						ObjectLockRetainUntilDate: timePtrIfNotEmpty(kv.GetOne("object-lock-retain-until-date")),
						SSECustomerAlgorithm:      stringPtrIfNotEmpty(kv.GetOne("sse-customer-algorithm")),
						SSECustomerKey:            stringPtrIfNotEmpty(kv.GetOne("sse-customer-key")),
						SSECustomerKeyMD5:         stringPtrIfNotEmpty(kv.GetOne("sse-customer-key-md5")),
						SSEKMSEncryptionContext:   stringPtrIfNotEmpty(kv.GetOne("sse-kms-encryption-context")),
						SSEKMSKeyId:               stringPtrIfNotEmpty(kv.GetOne("sse-kms-key-id")),
						ServerSideEncryption:      stringPtrIfNotEmpty(kv.GetOne("server-side-encryption")),
						StorageClass:              stringPtrIfNotEmpty(kv.GetOne("storage-class")),
						Tagging:                   stringPtrIfNotEmpty(kv.GetOne("tagging")),
						WebsiteRedirectLocation:   stringPtrIfNotEmpty(kv.GetOne("website-redirect-location")),
					}
					_, err = f.s3c.Client.PutObject(&input)
					if f.logger != nil {
						f.logger.InfoContext(ctx, "S3.PutObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("modified", kv.SortedKeys()), slog.Any("err", err))
					}
					w.CloseWithError(mapAwsErrToNinep(err))
				}
			}()
			h.W = w
		}
		return h, nil
	case "signDownloadByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		h, r, w := ninep.DeviceHandle(flag)
		if r != nil {
			r.Close()
		}
		if w != nil {
			go func() {
				url, err := f.signedDownloadURL(bucket, key, f.getSigningExpiry(bucket))
				if err != nil {
					w.CloseWithError(err)
				} else {
					_, err = io.WriteString(w, url)
					w.CloseWithError(err)
				}
			}()
		}
		return h, nil
	default:
		return nil, fs.ErrPermission
	}
}

func (f *fsys) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
	switch res.Id {
	case "root":
		infos := [...]fs.FileInfo{
			ninep.DirFileInfo("buckets"),
			ninep.DevFileInfo("ctl"),
		}
		return ninep.FileInfoSliceIterator(infos[:])
	case "ctl", "bucketCtl", "signUploadKey", "signUploadUrl":
		return ninep.FileInfoErrorIterator(ninep.ErrListingOnNonDir)
	case "buckets":
		return f.listBuckets(ctx)
	case "bucket":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return ninep.FileInfoErrorIterator(err)
		}
		infos := [...]fs.FileInfo{
			ninep.DevFileInfo("acl"),
			ninep.DevFileInfo("ctl"),
			ninep.DirFileInfo("metadata"),
			ninep.DirFileInfo("objects"),
			ninep.DirFileInfo("sign"),
		}
		return ninep.FileInfoSliceIterator(infos[:])
	case "sign":
		infos := [...]fs.FileInfo{
			ninep.DirFileInfo("download_url"),
			ninep.DirFileInfo("upload"),
			ninep.TempFileInfo("expires"),
		}
		return ninep.FileInfoSliceIterator(infos[:])
	case "objects":
		bucket := res.Vars[0]
		return f.listObjects(ctx, bucket, "", 0)
	case "metadata", "signDownload":
		bucket := res.Vars[0]
		return f.listObjects(ctx, bucket, "", fs.ModeDevice)
	case "signUpload":
		infos := [...]fs.FileInfo{
			ninep.TempFileInfo("key"),
			ninep.DevFileInfo("url"),
		}
		return ninep.FileInfoSliceIterator(infos[:])
	case "objectByKey":
		bucket := res.Vars[0]
		prefix := res.Vars[1]
		return f.listObjects(ctx, bucket, prefix, 0)
	case "metadataByKey", "signDownloadByKey":
		bucket := res.Vars[0]
		prefix := res.Vars[1]
		return f.listObjects(ctx, bucket, prefix, fs.ModeDevice)
	default:
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
}

func (f *fsys) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	switch res.Id {
	case "root":
		return ninep.DirFileInfo("."), nil
	case "ctl":
		return ninep.DevFileInfo("ctl"), nil
	case "buckets":
		return ninep.DirFileInfo("buckets"), nil
	case "bucket":
		bucket := res.Vars[0]
		return f.getBucketInfo(ctx, bucket)
	case "signExpires", "signUploadKey":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		}
		return ninep.TempFileInfo(filepath.Base(path)), nil
	case "bucketCtl", "bucketAcl", "signUploadUrl":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		}
		return ninep.DevFileInfo(filepath.Base(path)), nil
	case "objects", "metadata", "sign", "signDownload", "signUpload":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		}
		return ninep.DirFileInfo(filepath.Base(path)), nil
	case "objectByKey", "metadataByKey", "signDownloadByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		}
		info := f.getCachedObject(bucket, key)
		if info == nil {
			input := &s3.ListObjectsV2Input{
				Bucket:  aws.String(bucket),
				Prefix:  aws.String(key),
				MaxKeys: aws.Int64(1),
			}
			res, err := f.s3c.Client.ListObjectsV2(input)
			if f.logger != nil {
				f.logger.InfoContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", key), slog.Any("err", err))
			}
			if err != nil {
				return nil, mapAwsErrToNinep(err)
			}
			if len(res.Contents) == 0 {
				return nil, fs.ErrNotExist
			}
			var object *s3.Object
			if len(res.Contents) == 1 {
				obj := res.Contents[0]
				if keysMatch(obj.Key, key) {
					object = obj
				}
			}
			if object == nil {
				info = ninep.DirFileInfo(key)
			} else {
				info = objectInfo(filepath.Base(key), object, key)
			}
			f.setCachedObject(bucket, key, info)
		}
		if res.Id == "metadataByKey" {
			info = ninep.FileInfoWithMode(info, info.Mode()|fs.ModeDevice)
		}
		return info, nil
	default:
		return nil, fs.ErrNotExist
	}
}

func (f *fsys) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return fs.ErrNotExist
	}
	switch res.Id {
	case "root", "buckets", "objects", "metadata", "ctl", "bucketCtl", "sign", "signExpires", "signDownload", "signUpload", "signUploadKey", "signUploadUrl":
		return fs.ErrPermission
	case "objectByKey":
		bucket := res.Vars[0]
		// key := res.Vars[1]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return err
		}

		return ninep.ErrUnsupported
	case "metadataByKey", "signDownloadByKey":
		return ninep.ErrUnsupported
	default:
		return fs.ErrNotExist
	}
}

func (f *fsys) Delete(ctx context.Context, path string) error {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return fs.ErrNotExist
	}
	switch res.Id {
	case "root", "buckets", "objects", "metadata", "ctl", "bucketCtl", "signExpires", "signDownload", "signUpload", "signUploadKey", "signUploadUrl":
		return fs.ErrPermission
	case "bucket":
		bucket := res.Vars[0]
		return f.deleteBucket(ctx, bucket)
	case "objectByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		if strings.HasSuffix(key, "/") {
			return f.deleteObjectsByPrefix(ctx, bucket, key)
		}
		return f.deleteObject(ctx, bucket, key)
	case "metadataByKey":
		return ninep.ErrUnsupported
	case "signDownloadByKey":
		return ninep.ErrUnsupported
	default:
		return fs.ErrNotExist
	}
}

func (f *fsys) Walk(ctx context.Context, parts []string) ([]fs.FileInfo, error) {
	var res ninep.Match
	path := strings.Trim(strings.Join(parts, "/"), ".")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if !mx.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	infos := make([]fs.FileInfo, 0, len(parts))
	infos = append(infos, ninep.DirFileInfo("."))

	switch res.Id {
	case "root":
	case "ctl":
		infos = append(infos, ninep.DevFileInfo("ctl"))
	case "buckets":
		infos = append(infos, ninep.DirFileInfo("buckets"))
	case "bucket":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
	case "bucketAcl":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DevFileInfo("acl"))
	case "bucketCtl":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DevFileInfo("ctl"))
	case "objects":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("objects"))
	case "metadata":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("metadata"))
	case "sign":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("sign"))
	case "signExpires":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("sign"))
		infos = append(infos, ninep.DevFileInfo("expires"))
	case "signDownload":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("sign"))
		infos = append(infos, ninep.DirFileInfo("download_url"))
	case "signUpload":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("sign"))
		infos = append(infos, ninep.DirFileInfo("upload"))
	case "signUploadKey":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("sign"))
		infos = append(infos, ninep.DirFileInfo("upload"))
		infos = append(infos, ninep.TempFileInfo("key"))
	case "signUploadUrl":
		infos = append(infos, ninep.DirFileInfo("buckets"))
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("sign"))
		infos = append(infos, ninep.DirFileInfo("upload"))
		infos = append(infos, ninep.DevFileInfo("url"))
	case "objectByKey", "metadataByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		infos = append(infos, ninep.DirFileInfo("buckets"))
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		var em fs.FileMode
		infos = append(infos, ninep.DirFileInfo(res.Attrs["dir"]))
		if res.Id != "objectByKey" {
			em = fs.ModeDevice
		}

		found := false
		for info, err := range f.listObjects(ctx, bucket, key, em) {
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)
			found = true
			break
		}
		if !found {
			return infos, fs.ErrNotExist
		}
	case "signDownloadByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		infos = append(infos, ninep.DirFileInfo("buckets"))
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = append(infos, ninep.DirFileInfo("sign"))
		infos = append(infos, ninep.DirFileInfo("download_url"))

		found := false
		for info, err := range f.listObjects(ctx, bucket, key, fs.ModeDevice) {
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)
			found = true
			break
		}
		if !found {
			return infos, fs.ErrNotExist
		}
	default:
		return nil, fs.ErrNotExist
	}
	if parts[len(parts)-1] == "." && len(parts) > len(infos) && len(infos) > 0 {
		infos = append(infos, infos[len(infos)-1])
	}
	// for i, info := range infos {
	// 	fmt.Printf("INFO: %d %q\n", i, info.Name())
	// }
	// for i, part := range parts {
	// 	fmt.Printf("PART: %d %q\n", i, part)
	// }
	return infos, nil
}

func (f *fsys) deleteBucket(ctx context.Context, bucket string) error {
	_, err := f.s3c.Client.DeleteBucketWithContext(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if f.logger != nil {
		f.logger.InfoContext(ctx, "S3.DeleteBucket", slog.String("bucket", bucket), slog.Any("err", err))
	}
	if err == nil {
		f.bucketCache.Remove(bucket)
		f.evictCachedObjectsForBucket(bucket)
	}
	return mapAwsErrToNinep(err)
}

func (f *fsys) deleteObjectsByPrefix(ctx context.Context, bucket, prefix string) error {
	input := s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	for {
		resp, err := f.s3c.Client.ListObjectsV2WithContext(ctx, &input)
		if err != nil {
			if isNoSuchBucket(err) {
				f.bucketCache.Remove(bucket)
				f.evictCachedObjectsForBucket(bucket)
				return fs.ErrNotExist
			}
			return mapAwsErrToNinep(err)
		}
		if len(resp.Contents) == 0 {
			return nil
		}
		var objectKeys []*s3.ObjectIdentifier
		for _, obj := range resp.Contents {
			if obj.Key != nil {
				objectKeys = append(objectKeys, &s3.ObjectIdentifier{Key: obj.Key})
			}
		}
		_, err = f.s3c.Client.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &s3.Delete{
				Objects: objectKeys,
			},
		})
		if err != nil {
			return mapAwsErrToNinep(err)
		}
		if resp.IsTruncated != nil && !*resp.IsTruncated {
			break
		}
		input.ContinuationToken = resp.NextContinuationToken
	}
	return nil
}

func (f *fsys) deleteObject(ctx context.Context, bucket, key string) error {
	_, err := f.s3c.Client.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if f.logger != nil {
		f.logger.InfoContext(ctx, "S3.DeleteObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
	}
	if err == nil {
		f.evictCachedObject(bucket, key)
	}
	return mapAwsErrToNinep(err)
}

func (f *fsys) getBucketInfo(ctx context.Context, bucket string) (fs.FileInfo, error) {
	if info, ok := f.bucketCache.Get(bucket); ok {
		return info, nil
	}
	// check with s3 to see if the bucket exists
	// HeadBucket requires same permissions as ListBucket
	// this allows us to shortcut if the bucket doesn't exist without altering the cache.
	_, err := f.s3c.Client.HeadBucketWithContext(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if f.logger != nil {
		f.logger.InfoContext(ctx, "S3.HeadBucket", slog.String("bucket", bucket), slog.Any("err", err))
	}
	if err != nil {
		return nil, mapAwsErrToNinep(err)
	}
	for info, err := range f.listBuckets(ctx) {
		if err != nil {
			return nil, err
		}
		if info.Name() == bucket {
			return info, nil
		}
	}
	return nil, fs.ErrNotExist // we should never get here
}

func (f *fsys) listBuckets(ctx context.Context) iter.Seq2[fs.FileInfo, error] {
	resp, err := f.s3c.Client.ListBucketsWithContext(ctx, &s3.ListBucketsInput{})
	if f.logger != nil {
		f.logger.InfoContext(ctx, "S3.ListBuckets", slog.Int("count", len(resp.Buckets)), slog.Any("err", err))
	}
	if err != nil {
		return ninep.FileInfoErrorIterator(mapAwsErrToNinep(err))
	}
	var uid string
	if owner := resp.Owner; owner != nil {
		if displayName := owner.DisplayName; displayName != nil {
			uid = *displayName
		}
	}
	return func(yield func(fs.FileInfo, error) bool) {
		now := time.Now()
		for _, bucket := range resp.Buckets {
			if bucket != nil && bucket.Name != nil {
				var info fs.FileInfo = &ninep.SimpleFileInfo{
					FIName:    *bucket.Name,
					FIMode:    fs.ModeDir | 0777,
					FIModTime: now,
				}
				if uid != "" {
					info = ninep.FileInfoWithUsers(info, uid, "", "")
				}
				f.bucketCache.Add(*bucket.Name, info)
				if !yield(info, nil) {
					return
				}
			}
		}
	}
}

func (f *fsys) listObjects(ctx context.Context, bucket, prefix string, em fs.FileMode) iter.Seq2[fs.FileInfo, error] {
	input := s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}
	return func(yield func(fs.FileInfo, error) bool) {
		if f.listKeys {
			for {
				res, err := f.s3c.Client.ListObjectsV2(&input)
				if f.logger != nil {
					f.logger.InfoContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.Int("count", len(res.Contents)), slog.Any("err", err))
				}
				// TODO: handle retries?
				if err != nil {
					if isNoSuchBucket(err) {
						f.bucketCache.Remove(bucket)
						f.evictCachedObjectsForBucket(bucket)
					}
					yield(nil, mapAwsErrToNinep(err))
					return
				}
				input.ContinuationToken = res.NextContinuationToken

				for _, obj := range res.Contents {
					if obj == nil || obj.Key == nil {
						continue
					}
					key := *obj.Key
					if !strings.HasPrefix(key, "/") {
						key = "/" + key
					}
					info := objectInfo(filepath.Base(key), obj, key)
					f.setCachedObject(bucket, key, info)
					if em != 0 {
						info = ninep.FileInfoWithMode(info, info.Mode()|em)
					}
					if !yield(info, nil) {
						return
					}
				}

				if res.IsTruncated != nil && !*res.IsTruncated {
					return
				}
			}
		} else {
			seen := make(map[string]struct{})
			for {
				res, err := f.s3c.Client.ListObjectsV2(&input)
				if f.logger != nil {
					f.logger.InfoContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.Int("count", len(res.Contents)), slog.Any("err", err))
				}
				// TODO: handle retries?
				if err != nil {
					if isNoSuchBucket(err) {
						f.bucketCache.Remove(bucket)
						f.evictCachedObjectsForBucket(bucket)
					}
					yield(nil, mapAwsErrToNinep(err))
					return
				}
				input.ContinuationToken = res.NextContinuationToken

				for _, obj := range res.Contents {
					if obj == nil || obj.Key == nil {
						continue
					}
					key := *obj.Key
					// if !strings.HasPrefix(key, "/") {
					// 	key = "/" + key
					// }
					parts := ninep.PathSplit(key[len(prefix):])
					if len(parts) >= 1 && parts[0] == "" {
						parts = parts[1:]
					}
					switch len(parts) {
					case 0: // complete match
						prefixSize := len(prefix)
						if prefixSize == len(key) {
							idx := strings.LastIndex(key, "/")
							if idx != -1 {
								prefixSize = idx
							} else {
								prefixSize = 0
							}
						}
						info := objectInfo(key[prefixSize:], obj, key)
						f.setCachedObject(bucket, key, info)
						if em != 0 {
							info = ninep.FileInfoWithMode(info, info.Mode()|em)
						}
						if !yield(info, nil) {
							return
						}
						seen[key] = struct{}{}
					case 1: // object is a direct child of the prefix
						info := objectInfo(parts[0], obj, key)
						f.setCachedObject(bucket, key, info)
						if em != 0 {
							info = ninep.FileInfoWithMode(info, info.Mode()|em)
						}
						if !yield(info, nil) {
							return
						}
						seen[key] = struct{}{}

					default: // more sub directories needed for prefix to reach the object
						dirname := parts[0]
						if _, ok := seen[dirname]; ok {
							continue
						}

						var info fs.FileInfo = &ninep.SimpleFileInfo{
							FIName: dirname,
							FIMode: 0777 | fs.ModeDir,
						}
						f.setCachedObject(bucket, key, info)
						if !yield(info, nil) {
							return
						}
						seen[dirname] = struct{}{}
					}
				}

				if res.IsTruncated != nil && !*res.IsTruncated {
					return
				}
			}
		}
	}
}

// writes metadata about an object to w and then closes it.
func (f *fsys) writeMetadata(ctx context.Context, bucket, key string, w *io.PipeWriter) error {
	input := s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	resp, err := f.s3c.Client.HeadObjectWithContext(ctx, &input)
	if f.logger != nil {
		f.logger.InfoContext(ctx, "S3.HeadObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
	}
	if err != nil {
		w.CloseWithError(mapAwsErrToNinep(err))
		return mapAwsErrToNinep(err)
	}
	go func() {
		err := writePairs(w, map[string]any{
			"accept-ranges":                 resp.AcceptRanges,
			"cache-control":                 resp.CacheControl,
			"content-disposition":           resp.ContentDisposition,
			"content-encoding":              resp.ContentEncoding,
			"content-language":              resp.ContentLanguage,
			"content-length":                resp.ContentLength,
			"content-type":                  resp.ContentType,
			"delete-marker":                 resp.DeleteMarker,
			"etag":                          resp.ETag,
			"expiration":                    resp.Expiration,
			"expires":                       resp.Expires,
			"last-modified":                 resp.LastModified,
			"metadata":                      resp.Metadata,
			"missing-meta":                  resp.MissingMeta,
			"object-lock-legal-hold-status": resp.ObjectLockLegalHoldStatus,
			"object-lock-mode":              resp.ObjectLockMode,
			"object-lock-retain-until-date": resp.ObjectLockRetainUntilDate,
			"parts-count":                   resp.PartsCount,
			"replication-status":            resp.ReplicationStatus,
			"request-charged":               resp.RequestCharged,
			"restore":                       resp.Restore,
			"sse-customer-algorithm":        resp.SSECustomerAlgorithm,
			"sse-customer-key-md5":          resp.SSECustomerKeyMD5,
			"storage-class":                 resp.StorageClass,
			"version-id":                    resp.VersionId,
			"website-redirect-location":     resp.WebsiteRedirectLocation,
		})
		w.CloseWithError(mapAwsErrToNinep(err))
	}()
	return nil
}

func writePairs(w io.Writer, pairs map[string]any) error {
	keys := make([]string, 0, len(pairs))
	for k := range pairs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := pairs[k]
		if v == nil {
			continue
		}
		switch v := v.(type) {
		case *string:
			if v == nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, *v)); err != nil {
				return err
			}
		case *int64:
			if v == nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, strconv.FormatInt(*v, 10))); err != nil {
				return err
			}
		case *bool:
			if v == nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, strconv.FormatBool(*v))); err != nil {
				return err
			}
		case *time.Time:
			if v == nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, v.Format(time.RFC3339))); err != nil {
				return err
			}
		case *map[string]*string:
			if v == nil {
				continue
			}
			prefix := k
			m := *v
			for k, v := range m {
				if v == nil {
					continue
				}
				if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(fmt.Sprintf("%s-%s", prefix, k), *v)); err != nil {
					return err
				}
			}
		default:
			panic(fmt.Sprintf("unsupported type: %T", v))
		}
	}
	return nil
}

func awsErrCode(err error) string {
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		return awsErr.Code()
	}
	return ""
}

func isNoSuchBucket(err error) bool {
	return awsErrCode(err) == s3.ErrCodeNoSuchBucket
}

func (f *fsys) bucketCtlFile(ctx context.Context, bucket string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if r == nil || w == nil {
		r.Close()
		w.Close()
		return nil, fmt.Errorf("device handle must be read/write: %v", flag)
	}
	go func() {
		defer r.Close()
		scanner := bufio.NewScanner(r)
		help := [][2]string{
			{"help", "displays this help"},
			{"sign_upload_url", "get a signed upload URL for an object"},
		}
		for scanner.Scan() {
			line := scanner.Text()
			kv, err := kvp.ParseKeyValues(line)
			if err != nil {
				fmt.Fprintf(w, "ok=false error=parse_error reason=%q\n", err.Error())
				continue
			}
			switch kv.GetOne("op") {
			case "help", "":
				fmt.Fprintf(w, "HELP\n\nOPERATIONS\n")
				for _, cmd := range help {
					fmt.Fprintf(w, "  %s: %s\n", cmd[0], cmd[1])
				}
			case "sign_upload_url":
				if key := kv.GetOne("key"); key != "" {
					d := interpretTimeKeyValues(kv)
					if d <= 0 {
						d = f.getSigningExpiry(bucket)
					}
					url, err := f.signedUploadURL(bucket, key, d)
					if err != nil {
						fmt.Fprintf(w, "ok=false error=s3_error reason=%q\n", err.Error())
						continue
					}
					fmt.Fprintf(w, "url=%q\n", url)
				} else {
					fmt.Fprintf(w, "ok=false error=missing_key reason=%q\n", "key is required")
				}
			}
		}
	}()
	return h, nil
}

func (f *fsys) bucketAclFile(ctx context.Context, bucket string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if r != nil {
		go func() {
			data, err := io.ReadAll(r)
			if err != nil {
				r.CloseWithError(err)
				return
			}
			kv, err := kvp.ParseKeyValues(string(data))
			if err != nil {
				r.CloseWithError(err)
				return
			}
			input := s3.PutBucketAclInput{
				Bucket: aws.String(bucket),
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
			_, err = f.s3c.Client.PutBucketAcl(&input)
			if f.logger != nil {
				f.logger.InfoContext(ctx, "S3.PutBucketAcl", slog.String("bucket", bucket), slog.Any("err", err))
			}
			r.CloseWithError(mapAwsErrToNinep(err))
		}()
	}
	if w != nil {
		go func() {
			defer w.Close()
			input := s3.GetBucketAclInput{
				Bucket: aws.String(bucket),
			}
			out, err := f.s3c.Client.GetBucketAcl(&input)
			if err != nil {
				fmt.Fprintf(w, "ok=false error=s3_error reason=%q\n", err.Error())
				return
			}
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
				fmt.Fprintf(w, "%s\n", kvp.NonEmptyKeyPairs(pairs))
			}
		}()
	}
	return h, nil
}

func (f *fsys) signedDownloadURL(bucket, key string, expires time.Duration) (string, error) {
	req, _ := f.s3c.Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	url, err := req.Presign(expires)
	if err != nil {
		return "", err
	}
	return url, nil
}

func (f *fsys) signedUploadURL(bucket, key string, expires time.Duration) (string, error) {
	req, _ := f.s3c.Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	url, err := req.Presign(expires)
	if err != nil {
		return "", err
	}
	return url, nil
}

func (f *fsys) signExpiresFile(ctx context.Context, bucket string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	f.mu.Lock()
	d, ok := f.expires[bucket]
	f.mu.Unlock()
	if !ok {
		d = 15 * time.Minute
	}
	onFlush := func(p []byte) error {
		kvp, err := kvp.ParseKeyValues(string(p))
		if err != nil {
			return err
		}
		d := interpretTimeKeyValues(kvp)
		if d <= 0 {
			d = 15 * time.Minute
		}
		f.mu.Lock()
		f.expires[bucket] = d
		f.mu.Unlock()
		return nil
	}
	h := ninep.NewMemoryFileHandle([]byte(stringDuration(d)), nil, onFlush)
	return h, nil
}

func (f *fsys) signUploadKeyFile(bucket string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	f.mu.Lock()
	d := f.uploadKey[bucket]
	f.mu.Unlock()
	onFlush := func(p []byte) error {
		str := string(p)
		f.mu.Lock()
		f.uploadKey[bucket] = str
		f.mu.Unlock()
		return nil
	}
	h := ninep.NewMemoryFileHandle([]byte(d), nil, onFlush)
	return h, nil
}

func (f *fsys) signUploadUrlFile(bucket string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if flag.IsWriteable() {
		return nil, fmt.Errorf("signUploadUrlFile is not writeable")
	}
	key := f.getUploadKey(bucket)
	if key == "" {
		return nil, fmt.Errorf("no upload key for bucket %q", bucket)
	}
	d := f.getSigningExpiry(bucket)
	url, err := f.signedUploadURL(bucket, key, d)
	if err != nil {
		return nil, err
	}
	return ninep.NewMemoryFileHandle([]byte(url), nil, nil), nil
}
