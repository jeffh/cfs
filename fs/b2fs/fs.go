// Implements a 9p file system that talks to Backblaze's B2 Object Storage
// Service
package b2fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jeffh/b2client/b2"
	"github.com/jeffh/cfs/ninep"
)

const (
	bzEmptyFile = ".bzEmpty"
	debugLog    = false
)

func NewFromEnv() *FS {
	return NewFromEnvPrefix("")
}

func NewFromEnvPrefix(prefix string) *FS {
	appID := os.Getenv(prefix + "B2_ACCOUNT_ID")
	appKey := os.Getenv(prefix + "B2_ACCOUNT_KEY")
	return New(appID, appKey)
}

func New(keyID, appKey string) *FS {
	fs := &FS{}
	fs.C.KeyID = keyID
	fs.C.AppKey = appKey
	if debugLog {
		fs.C.C.L = log.New(os.Stdout, "[b2] ", log.LstdFlags)
	}
	return fs
}

type FS struct {
	C           b2.RetryClient
	TempStorage TempStorage

	mBuckets         sync.Mutex
	bucketNamesToIds map[string]string
	buckets          map[string]b2.Bucket // id->bucket

	mFiles         sync.Mutex
	fileNamesToIds map[string]string
	files          map[string]b2.File // bucketID+fileID->file
}

var _ ninep.WalkableFileSystem = (*FS)(nil)

func (fs *FS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	intent, err := parseIntent(false, path)
	if err != nil {
		return err
	}

	switch intent.op {
	case bucketOpNone:
		if intent.bucketName != "" {
			bkt, err := fs.C.CreateBucket(intent.bucketName, intent.bucketType, nil)
			if err != nil {
				return err
			}
			fs.mBuckets.Lock()
			defer fs.mBuckets.Unlock()
			fs.bucketNamesToIds[bkt.BucketName] = bkt.BucketID
			fs.buckets[bkt.BucketID] = b2.Bucket(bkt)
			return nil
		}
		return ninep.ErrWriteNotAllowed
	case bucketOpObjectData:
		bucketID, err := fs.getBucketID(intent.bucketType, intent.bucketName)
		if err != nil {
			return err
		}
		buf := bytes.NewBuffer(nil)
		res, err := fs.C.UploadFile(bucketID, b2.UploadFileOptions{
			FileName:      filepath.Join(intent.key, bzEmptyFile),
			ContentLength: 0,
			Body:          b2.Closer(buf),
		})
		if err != nil {
			return err
		}
		fs.storeFileID(bucketID, res.FileName, b2.File(res))
		return nil
	default:
		return ninep.ErrUnsupported
	}
}

func (fs *FS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	intent, err := parseIntent(false, path)
	if err != nil {
		return nil, err
	}

	switch intent.op {
	case bucketOpObjectData:
		bucketID, err := fs.getBucketID(intent.bucketType, intent.bucketName)
		if err != nil {
			return nil, err
		}

		f, _, err := fs.store(nil)
		if err != nil {
			return nil, err
		}
		return &handle{&fs.C, f, bucketID, intent.key}, err
	default:
		return nil, ninep.ErrWriteNotAllowed
	}
}

func (fs *FS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	intent, err := parseIntent(false, path)
	if err != nil {
		return nil, err
	}

	switch intent.op {
	case bucketOpObjectData:
		bucketID, err := fs.getBucketID(intent.bucketType, intent.bucketName)
		if err != nil {
			return nil, err
		}

		fileID, err := fs.getFileID(bucketID, intent.key)
		if err != nil {
			return nil, err
		}

		var r io.Reader
		if flag&ninep.OTRUNC == 0 {
			res, err := fs.C.DownloadFileByID(fileID, nil)
			if err != nil {
				return nil, err
			}
			defer res.Body.Close()
			r = res.Body
		}
		f, _, err := fs.store(r)
		if err != nil {
			return nil, err
		}
		return &handle{&fs.C, f, bucketID, intent.key}, err
	default:
		if flag.IsWriteable() {
			return nil, ninep.ErrWriteNotAllowed
		}
		return nil, ninep.ErrUnsupported
	}
}

func (fs *FS) Delete(ctx context.Context, path string) error {
	intent, err := parseIntent(true, path)
	if err != nil {
		return err
	}

	switch intent.op {
	case bucketOpNone:
		if intent.bucketName != "" {
			id, err := fs.getBucketID(intent.bucketType, intent.bucketName)
			if err != nil {
				return err
			}
			_, err = fs.C.DeleteBucket(id)
			if err != nil {
				return err
			}

			fs.mBuckets.Lock()
			defer fs.mBuckets.Unlock()
			delete(fs.bucketNamesToIds, intent.bucketName)
			delete(fs.buckets, id)
			return nil
		}
		return ninep.ErrWriteNotAllowed
	case bucketOpObjectData:
		bucketID, err := fs.getBucketID(intent.bucketType, intent.bucketName)
		if err != nil {
			return err
		}

		key := intent.key
		fileID, err := fs.getFileID(bucketID, key)
		if err != nil {
			// delete if it's a directory:
			if errors.Is(err, os.ErrNotExist) {
				key = filepath.Join(intent.key, bzEmptyFile)
				fileID, err = fs.getFileID(bucketID, key)
				if err != nil {
					return err
				}
				// walk through all "subdirectories" and delete them
				it := &keysIterator{C: &fs.C, I: intent, bucketID: bucketID}
				defer it.Close()
				for {
					fi, err := it.NextFileInfo()
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}
						return err
					}
					if fi.Name() != key {
						k := fi.Name()
						fid, err := fs.getFileID(bucketID, k)
						if err != nil {
							return err
						}
						// TODO(jeff): we should probably delete all versions?
						_, err = fs.C.DeleteFileVersion(fid, k)
						if err != nil {
							return err
						}
					}
				}
			} else {
				return err
			}
		}

		// TODO(jeff): we should probably delete all versions?
		_, err = fs.C.DeleteFileVersion(fileID, key)
		return err
	default:
		return ninep.ErrUnsupported
	}
}

func (fs *FS) ListDir(ctx context.Context, path string) (ninep.FileInfoIterator, error) {
	intent, err := parseIntent(false, path)
	if err != nil {
		return nil, err
	}

	switch intent.op {
	case bucketOpNone:
		if intent.bucketType == "" {
			now := time.Now()
			infos := []os.FileInfo{
				&staticDirInfo{"all", now},
				&staticDirInfo{"public", now},
				&staticDirInfo{"private", now},
				&staticDirInfo{"snapshot", now},
			}
			return ninep.FileInfoSliceIterator(infos), nil
		} else if intent.bucketName == "" {
			buckets, err := fs.getBucketInfos(intent.bucketType)
			if err != nil {
				return nil, err
			}

			return ninep.FileInfoSliceIterator(buckets), nil
		} else {
			now := time.Now()
			infos := []os.FileInfo{
				&staticDirInfo{string(bucketOpObjectData), now},
				// &staticDirInfo{string(bucketOpObjectMetadata), now},
			}
			return ninep.FileInfoSliceIterator(infos), nil
		}
	case bucketOpObjectData:
		bucketID, err := fs.getBucketID(intent.bucketType, intent.bucketName)
		if err != nil {
			return nil, err
		}
		prefixOffset := len(intent.key)
		if prefixOffset > 0 {
			prefixOffset++
		}
		return &keysIterator{C: &fs.C, I: intent, bucketID: bucketID, prefixOffset: prefixOffset}, nil
	default:
		return nil, ninep.ErrUnsupported
	}
}

func (fs *FS) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	intent, err := parseIntent(false, path)
	if err != nil {
		return nil, err
	}

	switch intent.op {
	case bucketOpNone:
		now := time.Now()
		if intent.bucketName != "" {
			bkt, err := fs.getBucketInfo(intent.bucketType, intent.bucketName)
			return bkt, err
		}
		switch intent.bucketType {
		case b2.BucketTypeAll:
			return &staticDirInfo{"all", now}, nil
		case b2.BucketTypePrivate:
			return &staticDirInfo{"private", now}, nil
		case b2.BucketTypePublic:
			return &staticDirInfo{"public", now}, nil
		case b2.BucketTypeSnapshot:
			return &staticDirInfo{"snapshot", now}, nil
		case "":
			return &staticDirInfo{".", now}, nil
		default:
			return nil, os.ErrNotExist
		}
	case bucketOpObjectData:
		bucketID, err := fs.getBucketID(intent.bucketType, intent.bucketName)
		if err != nil {
			return nil, err
		}
		index := strings.LastIndex(intent.key, "/")
		if index == -1 {
			index = 0
		}
		file, err := fs.getFileInfo(bucketID, intent.key, index)
		if errors.Is(err, os.ErrNotExist) {
			now := time.Now()
			// check if .bzInfo exists, so this is a "directory"
			_, err := fs.getFileInfo(bucketID, filepath.Join(intent.key, bzEmptyFile), index)
			if err == nil {
				return &staticDirInfo{intent.key, now}, nil
			}

			// heuristic: if writable, return file, otherwise return dir
			isDir := false
			switch m := ctx.Value("rawMessage").(type) {
			case ninep.Topen:
				isDir = !m.Mode().IsWriteable()
			case ninep.Tcreate:
				isDir = !m.Mode().IsWriteable()
			case ninep.Twstat:
				isDir = false
			}

			if isDir {
				return &staticDirInfo{intent.key, now}, nil
			} else {
				return &staticFileInfo{intent.key, now}, nil
			}
		}
		return file, err
	default:
		return nil, ninep.ErrUnsupported
	}
}

func (fs *FS) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	return ninep.ErrUnsupported
}

func (fs *FS) Walk(ctx context.Context, parts []string) ([]os.FileInfo, error) {
	next := func(p []string) ([]string, string) {
		if len(p) > 0 {
			return p[1:], p[0]
		}
		return p, ""
	}

	now := time.Now()
	infos := make([]os.FileInfo, 0, len(parts))
	parts, part := next(parts)
	if part == "." {
		infos = append(infos, &staticDirInfo{".", now})
	}
	if len(parts) == 0 {
		return infos, nil
	}

	parts, part = next(parts)
	bt := b2.BucketType(part)
	switch bt {
	case b2.BucketTypeAll:
		infos = append(infos, &staticDirInfo{"all", now})
	case b2.BucketTypePrivate:
		infos = append(infos, &staticDirInfo{"private", now})
	case b2.BucketTypePublic:
		infos = append(infos, &staticDirInfo{"public", now})
	case b2.BucketTypeSnapshot:
		infos = append(infos, &staticDirInfo{"snapshot", now})
	case "":
		break
	default:
		return infos, os.ErrNotExist
	}

	parts, bucketName := next(parts)
	var bucketID string
	var err error
	if bucketName != "" && bucketName != "." {
		bucketID, err = fs.getBucketID(bt, bucketName)
		if err != nil {
			return infos, err
		}

		fs.mBuckets.Lock()
		bkt, ok := fs.buckets[bucketID]
		fs.mBuckets.Unlock()
		if ok {
			infos = append(infos, &bucketInfo{bkt, now})
		} else {
			return infos, os.ErrNotExist
		}
	}

	parts, op := next(parts)
	switch bucketOp(op) {
	case bucketOpNone:
		break
	case bucketOpObjectData:
		infos = append(infos, &staticDirInfo{string(bucketOpObjectData), now})
	case ".":
		infos = append(infos, ninep.FileInfoWithName(infos[len(infos)-1], "."))
		return infos, nil
	default:
		return infos, os.ErrNotExist
	}

	for _, part := range parts {
		infos = append(infos, &staticDirInfo{part, now})
	}
	key := filepath.Clean(strings.Join(parts, "/"))
	index := strings.LastIndex(key, "/")
	if index < 0 {
		index = 0
	}
	var fi os.FileInfo
	fi, err = fs.getFileInfo(bucketID, key, index)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// check if .bzInfo exists, so this is a "directory"
			_, err := fs.getFileInfo(bucketID, filepath.Join(key, bzEmptyFile), index)
			if err == nil {
				fi = &staticDirInfo{key[index:], now}
			} else {
				// heuristic: if ends with a '.', assume dir, otherwise return file
				if len(parts) != 0 && parts[len(parts)-1] == "." {
					fi = &staticDirInfo{key[index:], now}
				} else {
					fi = &staticFileInfo{key[index:], now}
				}
			}
			err = nil
		} else {
			return infos, err
		}
	}
	infos[len(infos)-1] = fi

	names := []string{}
	for _, i := range infos {
		names = append(names, i.Name())
	}

	return infos, nil
}

func (fs *FS) getBucketInfos(bt b2.BucketType) ([]os.FileInfo, error) {
	fs.mBuckets.Lock()
	defer fs.mBuckets.Unlock()

	if fs.buckets == nil {
		fs.bucketNamesToIds = make(map[string]string)
		fs.buckets = make(map[string]b2.Bucket)
		res, err := fs.C.ListBuckets(nil)
		if err != nil {
			return nil, err
		}
		for _, bkt := range res.Buckets {
			fs.bucketNamesToIds[bkt.BucketName] = bkt.BucketID
			fs.buckets[bkt.BucketID] = bkt
		}
	}

	now := time.Now()
	infos := make([]os.FileInfo, 0, len(fs.buckets))
	for _, bkt := range fs.buckets {
		if bt == b2.BucketTypeAll || (bt == bkt.BucketType) {
			infos = append(infos, &bucketInfo{bkt, now})
		}
	}
	return infos, nil
}

func (fs *FS) getBucketInfo(bt b2.BucketType, bucketName string) (*bucketInfo, error) {
	fs.mBuckets.Lock()
	defer fs.mBuckets.Unlock()

	if fs.buckets == nil {
		fs.bucketNamesToIds = make(map[string]string)
		fs.buckets = make(map[string]b2.Bucket)
		res, err := fs.C.ListBuckets(nil)
		if err != nil {
			return nil, err
		}
		for _, bkt := range res.Buckets {
			fs.bucketNamesToIds[bkt.BucketName] = bkt.BucketID
			fs.buckets[bkt.BucketID] = bkt
		}
	}

	now := time.Now()
	for _, bkt := range fs.buckets {
		if bt == b2.BucketTypeAll || (bt == bkt.BucketType) {
			return &bucketInfo{bkt, now}, nil
		}
	}
	return nil, os.ErrNotExist
}

func (fs *FS) getBucketID(bt b2.BucketType, bucketName string) (string, error) {
	fs.mBuckets.Lock()
	defer fs.mBuckets.Unlock()

	if fs.buckets == nil {
		fs.bucketNamesToIds = make(map[string]string)
		fs.buckets = make(map[string]b2.Bucket)
	}

	id, ok := fs.bucketNamesToIds[bucketName]
	if !ok {
		res, err := fs.C.ListBuckets(nil)
		if err != nil {
			return "", err
		}
		for _, bkt := range res.Buckets {
			fs.bucketNamesToIds[bkt.BucketName] = bkt.BucketID
			fs.buckets[bkt.BucketID] = bkt
		}
		id, ok = fs.bucketNamesToIds[bucketName]
		if !ok {
			return "", os.ErrNotExist
		}
		return id, nil
	} else {
		return id, nil
	}
}

func (fs *FS) storeFileID(bucketID, fileName string, f b2.File) {
	fs.mFiles.Lock()
	defer fs.mFiles.Unlock()

	if fs.files == nil {
		fs.fileNamesToIds = make(map[string]string)
		fs.files = make(map[string]b2.File)
	}

	key := fs.fileKey(bucketID, fileName)
	fs.fileNamesToIds[key] = fileName
	fs.files[key] = f
}

func (fs *FS) getFileID(bucketID, fileName string) (string, error) {
	fs.mFiles.Lock()
	defer fs.mFiles.Unlock()

	if fs.files == nil {
		fs.fileNamesToIds = make(map[string]string)
		fs.files = make(map[string]b2.File)
	}
	key := fs.fileKey(bucketID, fileName)
	id, ok := fs.fileNamesToIds[key]
	if !ok {
		if err := fs.unsafeFetchFilesForKey(bucketID, fileName, key); err != nil {
			return "", err
		}
		id, ok = fs.fileNamesToIds[key]
		if !ok {
			return "", os.ErrNotExist
		}
	}
	return id, nil
}

func (fs *FS) getFileInfo(bucketID, fileName string, prefixOffset int) (*fileInfo, error) {
	fs.mFiles.Lock()
	defer fs.mFiles.Unlock()

	if fs.files == nil {
		fs.fileNamesToIds = make(map[string]string)
		fs.files = make(map[string]b2.File)
	}
	key := fs.fileKey(bucketID, fileName)

	fileID, ok := fs.fileNamesToIds[key]
	if !ok {
		if err := fs.unsafeFetchFilesForKey(bucketID, fileName, key); err != nil {
			return nil, err
		}
		fileID, ok = fs.fileNamesToIds[key]
		if !ok {
			return nil, os.ErrNotExist
		}
	}

	file, ok := fs.files[fileID]
	if !ok {
		return nil, os.ErrNotExist
	}

	return &fileInfo{file, prefixOffset, ""}, nil
}

func (fs *FS) fileKey(bucketID, fileName string) string {
	return fmt.Sprintf("%s://%s", bucketID, fileName)
}

func (fs *FS) unsafeFetchFilesForKey(bucketID, fileName, key string) error {
	res, err := fs.C.ListFileNames(bucketID, nil)
	if err != nil {
		return err
	}

	for _, f := range res.Files {
		fs.fileNamesToIds[fs.fileKey(bucketID, f.FileName)] = f.FileID
		fs.files[f.FileID] = f
	}

	_, ok := fs.fileNamesToIds[key]
	if res.NextFileName != "" && !ok {
		res, err = fs.C.ListFileNames(bucketID, &b2.ListFileNamesOptions{
			Prefix: fileName,
		})
		if err != nil {
			return err
		}

		for _, f := range res.Files {
			fs.fileNamesToIds[fs.fileKey(bucketID, f.FileName)] = f.FileID
			fs.files[f.FileID] = f
		}
	}

	return nil
}

func (fs *FS) store(r io.Reader) (TempFile, int64, error) {
	if fs.TempStorage == nil {
		// we don't want to have TempStorage use a lock, so we're using a global var fallback
		return globalTempStorage.Store(r)
	}
	return fs.TempStorage.Store(r)
}
