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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

var mx = ninep.NewMuxWith[string]().
	Define().Path("/").As("root").
	Define().Path("/ctl").As("ctl").
	Define().Path("/buckets").TrailSlash().As("buckets").
	Define().Path("/buckets/{bucket}").TrailSlash().As("bucket").
	Define().Path("/buckets/{bucket}/ctl").As("bucketCtl").
	Define().Path("/buckets/{bucket}/cors").As("bucketCors").
	Define().Path("/buckets/{bucket}/objects").TrailSlash().As("objects").
	Define().Path("/buckets/{bucket}/objects/{key*}").With("objects").As("objectByKey").
	Define().Path("/buckets/{bucket}/metadata").TrailSlash().As("metadata").
	Define().Path("/buckets/{bucket}/metadata/{key*}").With("metadata").As("metadataByKey").
	Define().Path("/buckets/{bucket}/sign").TrailSlash().As("sign").
	Define().Path("/buckets/{bucket}/sign/expires").As("signExpires").
	Define().Path("/buckets/{bucket}/sign/download_url").TrailSlash().As("signDownload").
	Define().Path("/buckets/{bucket}/sign/download_url/{key*}").As("signDownloadByKey").
	Define().Path("/buckets/{bucket}/sign/upload").TrailSlash().As("signUpload").
	Define().Path("/buckets/{bucket}/sign/upload/key").As("signUploadKey").
	Define().Path("/buckets/{bucket}/sign/upload/url").As("signUploadUrl").
	Define().Path("/buckets/{bucket}/acl").As("bucketAcl")

type Options struct {
	Endpoint string
	//  Flatten makes listing an object key prefix returns the full keys instead of
	//  just the logical directory names. This can be more efficient to use S3
	//  API at the cost of breaking some of the file system abstraction layer.
	Flatten bool

	S3PathStyle bool
	// Logger to use. Default
	Logger *slog.Logger
}

type S3Ctx struct {
	Client *s3.Client
}

// New returns a reasonable default configuration of an S3 FileSystem, an empty
// string of endpoint defaults to AWS' S3 service.
//
// Parameters:
//   - endpoint defaults to AWS' S3 service if it is an empty string.
//   - opt contains options for configuring the Filesystem
func New(endpoint string, opt Options) ninep.FileSystem {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		panic(err)
	}
	return NewWithAwsConfig(cfg, opt)
}

// NewWithAwsConfig returns a s3 FileSystem from a given s3 aws.Config object.
//
// Parameters:
//   - cfg is the aws.Config used to configure the s3 client.
//   - opt contains options for configuring the Filesystem
func NewWithAwsConfig(cfg aws.Config, opt Options) ninep.FileSystem {
	svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = opt.S3PathStyle
	})
	ctx := S3Ctx{
		Client: svc,
	}
	return NewWithClient(&ctx, opt)
}

// NewWithClient returns a s3 FileSystem from a given s3 client and session.
//
// Parameters:
//   - s3c is the configured s3 client and session. Both fields are required.
//   - opt contains options for configuring the Filesystem
func NewWithClient(s3c *S3Ctx, opt Options) ninep.FileSystem {
	return newFs(s3c, fsysOptions{
		Logger:          opt.Logger,
		BucketCacheSize: -1,
		ObjectCacheSize: -1,
		ListKeys:        opt.Flatten,
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
	var res ninep.MatchWith[string]
	if !mx.Match(path, &res) {
		return fs.ErrPermission
	}
	switch res.Id {
	case "root":
		return nil
	case "bucket":
		bucket := res.Vars[0]
		_, err := f.s3c.Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		if f.logger != nil {
			if err != nil {
				f.logger.ErrorContext(ctx, "S3.CreateBucket", slog.String("bucket", bucket), slog.Any("err", err))
			} else {
				f.logger.InfoContext(ctx, "S3.CreateBucket", slog.String("bucket", bucket))
			}
		}
		return mapAwsErrToNinep(err)
	case "objectByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		if !strings.HasSuffix(key, "/") {
			key += "/"
		}
		input := s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			IfNoneMatch: aws.String("*"),
		}
		_, err := f.s3c.Client.PutObject(ctx, &input)
		return mapAwsErrToNinep(err)
	// case "metadataByKey":
	// 	return nil
	default:
		return ninep.ErrWriteNotAllowed
	}
}

func (f *fsys) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	var res ninep.MatchWith[string]
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
				resp, err := f.s3c.Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if f.logger != nil {
					if err != nil {
						f.logger.ErrorContext(ctx, "S3.GetObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
					} else {
						f.logger.InfoContext(ctx, "S3.GetObject", slog.String("bucket", bucket), slog.String("key", key))
					}
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
				input := &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   r,
				}
				if !flag.IsTruncate() {
					input.IfNoneMatch = aws.String("*")
				}
				_, err := f.s3c.Client.PutObject(ctx, input)
				if f.logger != nil {
					if err != nil {
						f.logger.ErrorContext(ctx, "S3.UploadObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
					} else {
						f.logger.InfoContext(ctx, "S3.UploadObject", slog.String("bucket", bucket), slog.String("key", key))
					}
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
	var res ninep.MatchWith[string]
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
		return f.signExpiresFile(bucket)
	case "signUploadKey":
		bucket := res.Vars[0]
		return f.signUploadKeyFile(bucket)
	case "signUploadUrl":
		bucket := res.Vars[0]
		return f.signUploadUrlFile(ctx, bucket, flag)
	case "objectByKey":
		bucket := res.Vars[0]
		key := res.Vars[1]
		key = strings.TrimSuffix(key, "/")
		h := &ninep.RWFileHandle{}
		if flag.IsReadable() {
			r, w := io.Pipe()
			go func() {
				defer r.Close()
				resp, err := f.s3c.Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				if f.logger != nil {
					if err != nil {
						f.logger.ErrorContext(ctx, "S3.GetObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
					} else {
						f.logger.InfoContext(ctx, "S3.GetObject", slog.String("bucket", bucket), slog.String("key", key))
					}
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
				_, err := f.s3c.Client.PutObject(ctx, &s3.PutObjectInput{
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

						ACL:                       types.ObjectCannedACL(kv.GetOne("acl")),
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
						Metadata:                  map[string]string(kv.GetOnePrefix("metadata-")),
						ObjectLockLegalHoldStatus: types.ObjectLockLegalHoldStatus(kv.GetOne("object-lock-legal-hold-status")),
						ObjectLockMode:            types.ObjectLockMode(kv.GetOne("object-lock-mode")),
						ObjectLockRetainUntilDate: timePtrIfNotEmpty(kv.GetOne("object-lock-retain-until-date")),
						SSECustomerAlgorithm:      stringPtrIfNotEmpty(kv.GetOne("sse-customer-algorithm")),
						SSECustomerKey:            stringPtrIfNotEmpty(kv.GetOne("sse-customer-key")),
						SSECustomerKeyMD5:         stringPtrIfNotEmpty(kv.GetOne("sse-customer-key-md5")),
						SSEKMSEncryptionContext:   stringPtrIfNotEmpty(kv.GetOne("sse-kms-encryption-context")),
						SSEKMSKeyId:               stringPtrIfNotEmpty(kv.GetOne("sse-kms-key-id")),
						ServerSideEncryption:      types.ServerSideEncryption(kv.GetOne("server-side-encryption")),
						StorageClass:              types.StorageClass(kv.GetOne("storage-class")),
						Tagging:                   stringPtrIfNotEmpty(kv.GetOne("tagging")),
						WebsiteRedirectLocation:   stringPtrIfNotEmpty(kv.GetOne("website-redirect-location")),
					}
					_, err = f.s3c.Client.PutObject(ctx, &input)
					if f.logger != nil {
						if err != nil {
							f.logger.ErrorContext(ctx, "S3.PutObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("modified", kv.SortedKeys()), slog.Any("err", err))
						} else {
							f.logger.InfoContext(ctx, "S3.PutObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("modified", kv.SortedKeys()))
						}
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
				url, err := f.signedDownloadURL(ctx, bucket, key, f.getSigningExpiry(bucket))
				if err != nil {
					w.CloseWithError(err)
				} else {
					_, err = io.WriteString(w, url)
					w.CloseWithError(err)
				}
			}()
		}
		return h, nil
	case "bucketCors":
		bucket := res.Vars[0]
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

				// Parse CORS configuration from key-value pairs
				var corsRules []types.CORSRule
				for _, ruleStr := range kv.GetAll("rule") {
					ruleKV, err := kvp.ParseKeyValues(ruleStr)
					if err != nil {
						r.CloseWithError(err)
						return
					}

					rule := types.CORSRule{}
					if maxAge := ruleKV.GetOne("max_age"); maxAge != "" {
						if age, err := strconv.ParseInt(maxAge, 10, 32); err == nil {
							rule.MaxAgeSeconds = aws.Int32(int32(age))
						}
					}
					if methods := ruleKV.GetOne("allowed_methods"); methods != "" {
						for _, m := range strings.Split(methods, ",") {
							rule.AllowedMethods = append(rule.AllowedMethods, strings.TrimSpace(m))
						}
					}
					if headers := ruleKV.GetOne("allowed_headers"); headers != "" {
						for _, h := range strings.Split(headers, ",") {
							rule.AllowedHeaders = append(rule.AllowedHeaders, strings.TrimSpace(h))
						}
					}
					if origins := ruleKV.GetOne("allowed_origins"); origins != "" {
						for _, o := range strings.Split(origins, ",") {
							rule.AllowedOrigins = append(rule.AllowedOrigins, strings.TrimSpace(o))
						}
					}
					if headers := ruleKV.GetOne("expose_headers"); headers != "" {
						for _, h := range strings.Split(headers, ",") {
							rule.ExposeHeaders = append(rule.ExposeHeaders, strings.TrimSpace(h))
						}
					}
					corsRules = append(corsRules, rule)
				}

				input := &s3.PutBucketCorsInput{
					Bucket:            aws.String(bucket),
					CORSConfiguration: &types.CORSConfiguration{CORSRules: corsRules},
				}
				_, err = f.s3c.Client.PutBucketCors(ctx, input)
				if f.logger != nil {
					if err != nil {
						f.logger.ErrorContext(ctx, "S3.PutBucketCors", slog.String("bucket", bucket), slog.Any("err", err))
					} else {
						f.logger.InfoContext(ctx, "S3.PutBucketCors", slog.String("bucket", bucket))
					}
				}
				r.CloseWithError(mapAwsErrToNinep(err))
			}()
		}
		if w != nil {
			go func() {
				defer w.Close()
				input := &s3.GetBucketCorsInput{
					Bucket: aws.String(bucket),
				}
				out, err := f.s3c.Client.GetBucketCors(ctx, input)
				if f.logger != nil {
					if err != nil {
						f.logger.ErrorContext(ctx, "S3.GetBucketCors", slog.String("bucket", bucket), slog.Any("err", err))
					} else {
						f.logger.InfoContext(ctx, "S3.GetBucketCors", slog.String("bucket", bucket))
					}
				}
				if err != nil {
					if awsErrCode(err) == "NoSuchCORSConfiguration" {
						// No CORS configuration exists - return empty
						return
					}
					w.CloseWithError(mapAwsErrToNinep(err))
					return
				}

				// Write each CORS rule as a separate key-value pair
				for _, rule := range out.CORSRules {
					pairs := [][2]string{}
					if rule.MaxAgeSeconds != nil {
						pairs = append(pairs, [2]string{"max_age", fmt.Sprintf("%d", *rule.MaxAgeSeconds)})
					}
					if len(rule.AllowedMethods) > 0 {
						methods := make([]string, len(rule.AllowedMethods))
						copy(methods, rule.AllowedMethods)
						pairs = append(pairs, [2]string{"allowed_methods", strings.Join(methods, ",")})
					}
					if len(rule.AllowedHeaders) > 0 {
						headers := make([]string, len(rule.AllowedHeaders))
						copy(headers, rule.AllowedHeaders)
						pairs = append(pairs, [2]string{"allowed_headers", strings.Join(headers, ",")})
					}
					if len(rule.AllowedOrigins) > 0 {
						origins := make([]string, len(rule.AllowedOrigins))
						copy(origins, rule.AllowedOrigins)
						pairs = append(pairs, [2]string{"allowed_origins", strings.Join(origins, ",")})
					}
					if len(rule.ExposeHeaders) > 0 {
						headers := make([]string, len(rule.ExposeHeaders))
						copy(headers, rule.ExposeHeaders)
						pairs = append(pairs, [2]string{"expose_headers", strings.Join(headers, ",")})
					}
					fmt.Fprintf(w, "rule=%s\n", kvp.NonEmptyKeyPairs(pairs))
				}
			}()
		}
		return h, nil
	default:
		return nil, fs.ErrPermission
	}
}

func (f *fsys) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.MatchWith[string]
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
			ninep.DevFileInfo("cors"),
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
	var res ninep.MatchWith[string]
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
				MaxKeys: aws.Int32(1),
			}
			res, err := f.s3c.Client.ListObjectsV2(ctx, input)
			if f.logger != nil {
				f.logger.InfoContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", key), slog.Any("err", err))
			}
			if err != nil {
				return nil, mapAwsErrToNinep(err)
			}
			if len(res.Contents) == 0 {
				return nil, fs.ErrNotExist
			}
			var object *types.Object
			if len(res.Contents) == 1 {
				obj := res.Contents[0]
				if keysMatch(obj.Key, key) {
					object = &obj
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
	var res ninep.MatchWith[string]
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
	var res ninep.MatchWith[string]
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
	var res ninep.MatchWith[string]
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
		infos = append(infos, ninep.DirFileInfo(res.Value))
		if res.Id != "objectByKey" {
			em = fs.ModeDevice
		}

		found := false
		for info, err := range f.listObjects(ctx, bucket, key, em) {
			if err != nil {
				return nil, err
			}
			if !f.listKeys {
				subdirs := strings.Split(key, "/")
				for _, subdir := range subdirs[:len(subdirs)-1] {
					infos = append(infos, ninep.DirFileInfo(subdir))
				}
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
			if !f.listKeys {
				subdirs := strings.Split(key, "/")
				for _, subdir := range subdirs[:len(subdirs)-1] {
					infos = append(infos, ninep.DirFileInfo(subdir))
				}
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
	if len(parts) > len(infos) && len(infos) > 0 && parts[len(parts)-1] == "." {
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
	_, err := f.s3c.Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if f.logger != nil {
		if err != nil {
			f.logger.ErrorContext(ctx, "S3.DeleteBucket", slog.String("bucket", bucket), slog.Any("err", err))
		} else {
			f.logger.InfoContext(ctx, "S3.DeleteBucket", slog.String("bucket", bucket))
		}
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
		resp, err := f.s3c.Client.ListObjectsV2(ctx, &input)
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
		var objectKeys []types.ObjectIdentifier
		for _, obj := range resp.Contents {
			if obj.Key != nil {
				objectKeys = append(objectKeys, types.ObjectIdentifier{Key: obj.Key})
			}
		}
		_, err = f.s3c.Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectKeys,
			},
		})
		if f.logger != nil {
			if err != nil {
				f.logger.ErrorContext(ctx, "S3.DeleteObject", slog.String("bucket", bucket), slog.Any("keys", objectKeys), slog.Any("err", err))
			} else {
				f.logger.InfoContext(ctx, "S3.DeleteObject", slog.String("bucket", bucket), slog.Any("keys", objectKeys))
			}
		}
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
	_, err := f.s3c.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if f.logger != nil {
		if err != nil {
			f.logger.ErrorContext(ctx, "S3.DeleteObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
		} else {
			f.logger.InfoContext(ctx, "S3.DeleteObject", slog.String("bucket", bucket), slog.String("key", key))
		}
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
	_, err := f.s3c.Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if f.logger != nil {
		f.logger.InfoContext(ctx, "S3.HeadBucket", slog.String("bucket", bucket), slog.Any("err", err))
	}
	if err != nil {
		if isFatalAwsErr(err) {
			return nil, mapAwsErrToNinep(err)
		}
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
	resp, err := f.s3c.Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if f.logger != nil {
		if err != nil {
			f.logger.ErrorContext(ctx, "S3.ListBuckets", slog.Any("err", err))
		} else {
			f.logger.InfoContext(ctx, "S3.ListBuckets", slog.Int("count", len(resp.Buckets)))
		}
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
			if bucket.Name != nil {
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
	return func(yield func(fs.FileInfo, error) bool) {
		input := s3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String(prefix),
		}
		if f.listKeys {
			for {
				res, err := f.s3c.Client.ListObjectsV2(ctx, &input)
				if f.logger != nil {
					if err != nil {
						if isNoSuchBucket(err) {
							f.logger.ErrorContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.Int("count", len(res.Contents)), slog.Any("err", err))
						} else {
							f.logger.WarnContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.Int("count", len(res.Contents)), slog.Any("err", err))
						}
					} else {
						f.logger.InfoContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.Int("count", len(res.Contents)))
					}
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
					if obj.Key == nil {
						continue
					}
					key := *obj.Key
					if !strings.HasPrefix(key, "/") {
						key = "/" + key
					}
					info := objectInfo(filepath.Base(key), &obj, key)
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
			// input.SetDelimiter("/")
			seen := make(map[string]struct{})
			for {
				res, err := f.s3c.Client.ListObjectsV2(ctx, &input)
				if f.logger != nil {
					if err != nil {
						if isNoSuchBucket(err) {
							f.logger.ErrorContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.String("err", err.Error()))
						} else {
							f.logger.WarnContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.String("err", err.Error()))
						}
					} else {
						f.logger.InfoContext(ctx, "S3.ListObjectsV2", slog.String("bucket", bucket), slog.String("prefix", prefix), slog.Int("count", len(res.Contents)))
					}
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
					if obj.Key == nil {
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
						info := objectInfo(key[prefixSize:], &obj, key)
						f.setCachedObject(bucket, key, info)
						if em != 0 {
							info = ninep.FileInfoWithMode(info, info.Mode()|em)
						}
						if !yield(info, nil) {
							return
						}
						seen[key] = struct{}{}
					case 1: // object is a direct child of the prefix
						info := objectInfo(parts[0], &obj, key)
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
	resp, err := f.s3c.Client.HeadObject(ctx, &input)
	if f.logger != nil {
		if err != nil {
			f.logger.ErrorContext(ctx, "S3.HeadObject", slog.String("bucket", bucket), slog.String("key", key), slog.Any("err", err))
		} else {
			f.logger.InfoContext(ctx, "S3.HeadObject", slog.String("bucket", bucket), slog.String("key", key))
		}
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
	var awsErr smithy.APIError
	if errors.As(err, &awsErr) {
		return awsErr.ErrorCode()
	}
	return ""
}

func isNoSuchBucket(err error) bool {
	return awsErrCode(err) == "NoSuchBucket"
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
					url, err := f.signedUploadURL(ctx, bucket, key, d)
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
				input.ACL = types.BucketCannedACL(acl)
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
			_, err = f.s3c.Client.PutBucketAcl(ctx, &input)
			if f.logger != nil {
				if err != nil {
					f.logger.ErrorContext(ctx, "S3.PutBucketAcl", slog.String("bucket", bucket), slog.Any("err", err))
				} else {
					f.logger.InfoContext(ctx, "S3.PutBucketAcl", slog.String("bucket", bucket))
				}
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
			out, err := f.s3c.Client.GetBucketAcl(ctx, &input)
			if err != nil {
				fmt.Fprintf(w, "ok=false error=s3_error reason=%q\n", err.Error())
				return
			}
			for _, grant := range out.Grants {
				pairs := [][2]string{}
				pairs = append(pairs, [2]string{"permission", string(grant.Permission)})

				if g := grant.Grantee; g != nil {
					pairs = append(pairs, [2]string{"id", aws.ToString(g.ID)})
					pairs = append(pairs, [2]string{"name", aws.ToString(g.DisplayName)})
					pairs = append(pairs, [2]string{"email", aws.ToString(g.EmailAddress)})
					pairs = append(pairs, [2]string{"type", string(g.Type)})
					pairs = append(pairs, [2]string{"uri", aws.ToString(g.URI)})
				}
				fmt.Fprintf(w, "%s\n", kvp.NonEmptyKeyPairs(pairs))
			}
		}()
	}
	return h, nil
}

func (f *fsys) signedDownloadURL(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	signer := s3.NewPresignClient(f.s3c.Client)
	presignedRequest, err := signer.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expires))

	if f.logger != nil {
		if err != nil {
			f.logger.Error("S3.Presign.GetObjectRequest", slog.String("bucket", bucket), slog.String("key", key), slog.Duration("expires", expires), slog.Any("err", err))
		} else {
			f.logger.Info("S3.Presign.GetObjectRequest", slog.String("bucket", bucket), slog.String("key", key), slog.Duration("expires", expires))
		}
	}
	if err != nil {
		return "", err
	}
	return presignedRequest.URL, nil
}

func (f *fsys) signedUploadURL(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	signer := s3.NewPresignClient(f.s3c.Client)
	presignedRequest, err := signer.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(expires))
	if f.logger != nil {
		if err != nil {
			f.logger.Error("S3.Presign.PutObjectRequest", slog.String("bucket", bucket), slog.String("key", key), slog.Duration("expires", expires), slog.Any("err", err))
		} else {
			f.logger.Info("S3.Presign.PutObjectRequest", slog.String("bucket", bucket), slog.String("key", key), slog.Duration("expires", expires))
		}
	}
	if err != nil {
		return "", err
	}
	return presignedRequest.URL, nil
}

func (f *fsys) signExpiresFile(bucket string) (ninep.FileHandle, error) {
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

func (f *fsys) signUploadKeyFile(bucket string) (ninep.FileHandle, error) {
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

func (f *fsys) signUploadUrlFile(ctx context.Context, bucket string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if flag.IsWriteable() {
		return nil, fmt.Errorf("signUploadUrlFile is not writeable")
	}
	key := f.getUploadKey(bucket)
	if key == "" {
		return nil, fmt.Errorf("no upload key for bucket %q", bucket)
	}
	d := f.getSigningExpiry(bucket)
	url, err := f.signedUploadURL(ctx, bucket, key, d)
	if err != nil {
		return nil, err
	}
	return ninep.NewMemoryFileHandle([]byte(url), nil, nil), nil
}
