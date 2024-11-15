// Implements a 9p file system that talks to Amazon's S3 Object Storage
// Service.
//
// Also supports any S3-compatible service as well.
package s3fs

import (
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
func NewBasicFs(endpoint string, truncate bool) ninep.FileSystem {
	cfg := &aws.Config{
		Endpoint: stringPtrOrNil(endpoint),
	}
	sess := session.Must(session.NewSession())
	svc := s3.New(sess, cfg)
	ctx := S3Ctx{
		Session: sess,
		Client:  svc,
	}

	return NewFs(&ctx, truncate)
}

func NewFs(s3c *S3Ctx, truncate bool) ninep.FileSystem {
	trace := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return newFs(s3c, trace, truncate, -1, -1)
	// return &ninep.SimpleWalkableFileSystem{
	// 	ninep.SimpleFileSystem{
	// 		Root: ninep.StaticRootDir(
	// 			&buckets{
	// 				ninep.SimpleFileInfo{
	// 					FIName:    "buckets",
	// 					FIMode:    os.ModeDir | 0755,
	// 					FIModTime: time.Now(),
	// 				},
	// 				s3c,
	// 			},
	// 		),
	// 	},
	// }
}

////////////////////////////////////////////////////////////////////////////////

func staticDir(name string, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0555,
		},
		Children: children,
	}
}

func staticDirWithTime(name string, modTime time.Time, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    os.ModeDir | 0555,
			FIModTime: modTime,
		},
		Children: children,
	}
}

func dynamicDir(name string, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDir {
	return &ninep.DynamicReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0555,
		},
		GetChildren: resolve,
	}
}

func dynamicDirItr(name string, resolve func() iter.Seq2[ninep.Node, error]) *ninep.DynamicReadOnlyDirItr {
	return &ninep.DynamicReadOnlyDirItr{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		GetChildren: resolve,
	}
}

func dynamicCtlFile(name string, thread func(m ninep.OpenMode, r io.Reader, w io.Writer)) *ninep.SimpleFile {
	return ninep.CtlFile(name, 0777, time.Time{}, thread)
}

func staticStringFile(name string, modTime time.Time, contents string) *ninep.SimpleFile {
	return ninep.StaticReadOnlyFile(name, 0444, modTime, []byte(contents))
}

func dynamicStringFile(name string, modTime time.Time, content func() ([]byte, error)) *ninep.SimpleFile {
	return ninep.DynamicReadOnlyFile(name, 0444, modTime, content)
}

type fsys struct {
	s3c      *S3Ctx
	logger   *slog.Logger
	listKeys bool // set to true to list keys in directory hierarchies instead of all keys matching prefix

	bucketCache *expirable.LRU[string, fs.FileInfo]
	objCache    *expirable.LRU[string, fs.FileInfo]
}

func newFs(s3c *S3Ctx, logger *slog.Logger, listKeys bool, bucketCacheSize, objCacheSize int) *fsys {
	if bucketCacheSize < 0 {
		bucketCacheSize = 32
	}
	if objCacheSize < 0 {
		objCacheSize = 512
	}
	return &fsys{
		s3c:         s3c,
		logger:      logger,
		listKeys:    listKeys,
		bucketCache: expirable.NewLRU[string, fs.FileInfo](bucketCacheSize, nil, 24*time.Hour),
		objCache:    expirable.NewLRU[string, fs.FileInfo](objCacheSize, nil, 1*time.Hour),
	}
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

var mx = ninep.NewMux().
	Define().Path("/").As("root").
	Define().Path("/ctl").As("ctl").
	Define().Path("/buckets").As("buckets").
	Define().Path("/buckets/{bucket}").As("bucket").
	Define().Path("/buckets/{bucket}/ctl").As("bucketCtl").
	Define().Path("/buckets/{bucket}/objects").As("objects").
	Define().Path("/buckets/{bucket}/objects/{key*}").As("objectByKey").
	Define().Path("/buckets/{bucket}/metadata").As("metadata").
	Define().Path("/buckets/{bucket}/metadata/{key*}").As("metadataByKey")

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
		return nil, fmt.Errorf("use MakeDir to create buckets")
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
		return nil, ninep.ErrWriteNotAllowed
	}
}

func (f *fsys) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	switch res.Id {
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
			&ninep.SimpleFileInfo{
				FIName: "buckets",
				FIMode: fs.ModeDir | 0777,
			},
			&ninep.SimpleFileInfo{
				FIName: "ctl",
				FIMode: fs.ModeDevice | 0666,
			},
		}
		return ninep.FileInfoSliceIterator(infos[:])
	case "ctl", "bucketCtl":
		return ninep.FileInfoErrorIterator(ninep.ErrListingOnNonDir)
	case "buckets":
		return f.listBuckets(ctx)
	case "bucket":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return ninep.FileInfoErrorIterator(err)
		}
		infos := [...]fs.FileInfo{
			&ninep.SimpleFileInfo{
				FIName: "ctl",
				FIMode: fs.ModeDevice | 0666,
			},
			&ninep.SimpleFileInfo{
				FIName: "metadata",
				FIMode: fs.ModeDir | 0o777,
			},
			&ninep.SimpleFileInfo{
				FIName: "objects",
				FIMode: fs.ModeDir | 0o777,
			},
		}
		return ninep.FileInfoSliceIterator(infos[:])
	case "objects", "metadata":
		bucket := res.Vars[0]
		return f.listObjects(ctx, bucket, "", 0)
	case "objectByKey":
		bucket := res.Vars[0]
		prefix := res.Vars[1]
		return f.listObjects(ctx, bucket, prefix, 0)
	case "metadataByKey":
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
		return &ninep.SimpleFileInfo{
			FIName: "/",
			FIMode: fs.ModeDir | 0o777,
		}, nil
	case "ctl":
		return &ninep.SimpleFileInfo{
			FIName: "ctl",
			FIMode: fs.ModeDevice | 0o666,
		}, nil
	case "buckets":
		return &ninep.SimpleFileInfo{
			FIName: "buckets",
			FIMode: fs.ModeDir | 0o777,
		}, nil
	case "bucket":
		bucket := res.Vars[0]
		return f.getBucketInfo(ctx, bucket)
	case "bucketCtl":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		}
		return &ninep.SimpleFileInfo{
			FIName: "ctl",
			FIMode: fs.ModeDevice | 0o666,
		}, nil
	case "objects":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		}
		return &ninep.SimpleFileInfo{
			FIName: "objects",
			FIMode: fs.ModeDir | 0o777,
		}, nil
	case "metadata":
		bucket := res.Vars[0]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		}
		return &ninep.SimpleFileInfo{
			FIName: "metadata",
			FIMode: fs.ModeDir | 0o777,
		}, nil
	case "objectByKey", "metadataByKey":
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
				info = &ninep.SimpleFileInfo{
					FIName: key,
					FIMode: 0o777 | fs.ModeDir,
				}
			} else {
				info = objectInfo(len(key), object, key)
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
	case "root", "buckets", "objects", "metadata", "ctl", "bucketCtl":
		return fs.ErrPermission
	case "objectByKey":
		bucket := res.Vars[0]
		// key := res.Vars[1]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return err
		}

		return ninep.ErrUnsupported
	case "metadataByKey":
		bucket := res.Vars[0]
		// key := res.Vars[1]
		if _, err := f.getBucketInfo(ctx, bucket); err != nil {
			return err
		}
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
	case "root", "buckets", "objects", "metadata", "ctl", "bucketCtl":
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

	addDir := func(out []fs.FileInfo, name string, mode fs.FileMode) []fs.FileInfo {
		return append(out, &ninep.SimpleFileInfo{
			FIName: name,
			FIMode: mode,
		})
	}

	infos = addDir(infos, ".", fs.ModeDir|0o777)

	switch res.Id {
	case "root":
		return infos, nil
	case "ctl":
		return infos, ninep.ErrListingOnNonDir
	case "buckets":
		infos = addDir(infos, "buckets", fs.ModeDir|0o777)
		return infos, nil
	case "bucket":
		infos = addDir(infos, "buckets", fs.ModeDevice|0o666)
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		return infos, nil
	case "bucketCtl":
		infos = addDir(infos, "buckets", fs.ModeDevice|0o666)
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		return infos, ninep.ErrListingOnNonDir
	case "objects":
		infos = addDir(infos, "buckets", fs.ModeDevice|0o666)
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = addDir(infos, "objects", fs.ModeDir|0o777)
		return infos, nil
	case "metadata":
		infos = addDir(infos, "buckets", fs.ModeDevice|0o666)
		bucket := res.Vars[0]
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = addDir(infos, "metadata", fs.ModeDir|0o777)
		return infos, nil
	case "objectByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		infos = addDir(infos, "buckets", fs.ModeDevice|0o666)
		if info, err := f.getBucketInfo(ctx, bucket); err != nil {
			return nil, err
		} else {
			infos = append(infos, info)
		}
		infos = addDir(infos, "objects", fs.ModeDir|0o777)

		for info, err := range f.listObjects(ctx, bucket, key, 0) {
			if err != nil {
				return nil, err
			}
			infos = append(infos, info)
			break
		}
		subpath := parts[4:]
		fmt.Printf("SUFFIX: %q -- %#v\n", key, subpath)
		input := s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(key),
			MaxKeys: aws.Int64(2),
		}
		resp, err := f.s3c.Client.ListObjectsV2(&input)
		if f.logger != nil {
			f.logger.InfoContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", key), slog.Int("count", len(resp.Contents)), slog.Any("err", err))
		}
		if err != nil {
			return infos, mapAwsErrToNinep(err)
		}
		if len(resp.Contents) == 0 {
			return infos, fs.ErrNotExist
		}
		if keysMatch(resp.Contents[0].Key, key) {
			object := resp.Contents[0]
			parentObjectKey := filepath.Join(subpath[:len(subpath)-1]...)
			if !strings.HasSuffix(key, "/") {
				key += "/"
			}
			objInfo := objectInfo(len(parentObjectKey), object, parentObjectKey)
			for i, part := range subpath[:len(subpath)-1] {
				name := part
				if name == "." && len(subpath) > 1 {
					name = subpath[len(subpath)-2]
				}
				info := &ninep.SimpleFileInfo{
					FIName:    name,
					FIMode:    0777 | fs.ModeDir,
					FIModTime: *object.LastModified,
					FISize:    *object.Size,
				}
				infos = append(infos, info)
				f.setCachedObject(bucket, filepath.Join(parentObjectKey, filepath.Join(subpath[:i+1]...)), info)
			}
			infos = append(infos, objInfo)
			f.setCachedObject(bucket, filepath.Join(parentObjectKey, key), objInfo)
			return infos, nil
		} else {
			for _, part := range subpath {
				infos = append(infos, &ninep.SimpleFileInfo{
					FIName: part,
					FIMode: 0777 | fs.ModeDir,
				})
			}
			return infos, nil
		}
	case "metadataByKey":
		// TODO: implement
		return nil, ninep.ErrUnsupported
	default:
		return nil, fs.ErrNotExist
	}
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
					info := objectInfo(len(prefix), obj, key)
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
					if !strings.HasPrefix(key, "/") {
						key = "/" + key
					}
					parts := ninep.PathSplit(key[len(prefix)+1:])
					var dirname string
					if len(parts) > 2 {
						dirname = parts[1]
						if _, ok := seen[dirname]; ok {
							continue
						}
					} else if len(parts) <= 1 {
						continue
					}
					if len(parts) == 2 {
						info := objectInfo(len(prefix), obj, key)
						f.setCachedObject(bucket, key, info)
						if em != 0 {
							info = ninep.FileInfoWithMode(info, info.Mode()|em)
						}
						if !yield(info, nil) {
							return
						}
						seen[key] = struct{}{}
					} else {
						var info fs.FileInfo = &ninep.SimpleFileInfo{
							FIName:    dirname,
							FIMode:    0777 | fs.ModeDir,
							FIModTime: *obj.LastModified,
							FISize:    *obj.Size,
						}
						f.setCachedObject(bucket, key, info)
						if em != 0 {
							info = ninep.FileInfoWithMode(info, info.Mode()|em)
						}
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
	sort.Sort(sort.StringSlice(keys))
	for _, k := range keys {
		switch v := pairs[k].(type) {
		case *string:
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, *v)); err != nil {
				return err
			}
		case *int64:
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, strconv.FormatInt(*v, 10))); err != nil {
				return err
			}
		case *bool:
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, strconv.FormatBool(*v))); err != nil {
				return err
			}
		case *time.Time:
			if _, err := fmt.Fprintf(w, "%s\n", kvp.KeyPair(k, v.Format(time.RFC3339))); err != nil {
				return err
			}
		case *map[string]*string:
			prefix := k
			m := *v
			for k, v := range m {
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
