package cachefs

import (
	"context"
	"fmt"
	"io/fs"
	"iter"
	"log/slog"
	"os"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/jeffh/cfs/ninep"
)

type Options func(f *fsys)

func WithLogger(l *slog.Logger) Options {
	return func(f *fsys) {
		f.logger = l
	}
}

func WithMaxDataEntries(n int) Options {
	return func(f *fsys) {
		f.maxSize = n
	}
}

func WithMaxDirsCached(n int) Options {
	return func(f *fsys) {
		f.maxDirSize = n
	}
}

func WithMaxStatCache(n int) Options {
	return func(f *fsys) {
		f.maxStatSize = n
	}
}

func WithAsyncWrites(b bool) Options {
	return func(f *fsys) {
		f.writesAsync = b
	}
}

type fsys struct {
	logger      *slog.Logger
	underlying  ninep.FileSystem
	client      ninep.Client
	dirCache    *lru.Cache[string, []fs.FileInfo]
	statCache   *lru.Cache[string, fs.FileInfo]
	dataCache   *lru.Cache[dataKey, *dataValue]
	writesAsync bool
	maxSize     int
	maxDirSize  int
	maxStatSize int
}

func New(slowfs ninep.FileSystem, client ninep.Client, opts ...Options) ninep.FileSystem {
	f := &fsys{
		underlying:  slowfs,
		client:      client,
		maxSize:     1024,
		maxDirSize:  256,
		maxStatSize: 1024,
	}
	for _, opt := range opts {
		opt(f)
	}

	dirCache, err := lru.New[string, []fs.FileInfo](f.maxDirSize)
	if err != nil {
		panic(fmt.Sprintf("failed to create cache: %v", err))
	}
	f.dirCache = dirCache

	statCache, err := lru.New[string, fs.FileInfo](f.maxStatSize)
	if err != nil {
		panic(fmt.Sprintf("failed to create cache: %v", err))
	}
	f.statCache = statCache

	dataCache, err := lru.New[dataKey, *dataValue](f.maxSize)
	if err != nil {
		panic(fmt.Sprintf("failed to create cache: %v", err))
	}
	f.dataCache = dataCache

	return f
}

func (f *fsys) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	err := f.underlying.MakeDir(ctx, path, mode)
	if err != nil {
		return err
	}
	f.fileMetadataChanged(path)
	return nil
}

func (f *fsys) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	h, err := f.underlying.CreateFile(ctx, path, flag, mode)
	if err != nil {
		return nil, err
	}
	f.fileMetadataChanged(path)
	return &handle{
		fs:          f,
		open:        func() (ninep.FileHandle, error) { return h, nil },
		path:        path,
		isWriteable: flag.IsWriteable(),
	}, nil
}

func (f *fsys) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	info, err := f.Stat(ctx, path)
	if err != nil {
		if os.IsNotExist(err) {
			f.fileMetadataChanged(path)
			return nil, err
		}
		return nil, err
	}
	mode := info.Mode()
	if mode&fs.ModeDevice != 0 {
		return f.underlying.OpenFile(ctx, path, flag)
	}
	var open func() (ninep.FileHandle, error)
	if flag&ninep.OTRUNC != 0 {
		cached, ok := f.dataCache.Get(dataKey{path: path})
		if ok {
			cached.Truncate()
		} else {
			cached := &dataValue{
				blocks: []block{{offset: 0, data: nil}},
			}
			f.dataCache.Add(dataKey{path: path}, cached)
		}
		h, err := f.underlying.OpenFile(ctx, path, flag)
		if err != nil {
			return nil, err
		}
		open = func() (ninep.FileHandle, error) { return h, nil }
	} else {
		open = func() (ninep.FileHandle, error) { return f.underlying.OpenFile(ctx, path, flag) }
	}
	return &handle{
		fs:          f,
		open:        open,
		path:        path,
		isWriteable: flag.IsWriteable(),
	}, nil
}

func (f *fsys) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		cacheKey := strings.TrimSuffix(path, "/") + "/"
		if cached, ok := f.dirCache.Get(cacheKey); ok {
			if f.logger != nil {
				f.logger.Info("CacheFS.dirCache.hit", "path", cacheKey, "numResults", len(cached))
			}
			for _, info := range cached {
				if !yield(info, nil) {
					return
				}
			}
		} else {
			finished := false
			var results []fs.FileInfo
			for info, err := range f.underlying.ListDir(ctx, path) {
				if info != nil && err == nil {
					results = append(results, ninep.StatFromFileInfoClone(info).FileInfo())
				}
				finished = finished || !yield(info, err)
				if finished && err != nil {
					if f.logger != nil {
						f.logger.Info("CacheFS.dirCache.miss", "path", cacheKey, "error", err)
					}
					return
				}
			}
			eviction := f.dirCache.Add(cacheKey, results)
			if f.logger != nil {
				f.logger.Info("CacheFS.dirCache.miss", "path", cacheKey, "numResults", len(results), "eviction", eviction)
			}
		}
	}
}

func (f *fsys) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	if cached, ok := f.statCache.Get(path); ok {
		if f.logger != nil {
			f.logger.Info("CacheFS.statCache.hit", "path", path)
		}
		return cached, nil
	}
	info, err := f.underlying.Stat(ctx, path)
	if err == nil {
		if f.logger != nil {
			f.logger.Info("CacheFS.statCache.miss", "path", path)
		}
		f.statCache.Add(path, info)
	} else {
		if f.logger != nil {
			f.logger.Info("CacheFS.statCache.miss", "path", path, "error", err)
		}
	}
	return info, err
}

func (f *fsys) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	err := f.underlying.WriteStat(ctx, path, s)
	if err == nil {
		f.fileMetadataChanged(path)
	}
	return err
}

func (f *fsys) Delete(ctx context.Context, path string) error {
	err := f.underlying.Delete(ctx, path)
	fmt.Println("CacheFS.Delete", path, err)
	if err == nil || os.IsNotExist(err) {
		for _, k := range f.dataCache.Keys() {
			if k.path == path {
				present := f.dataCache.Remove(k)
				if f.logger != nil {
					f.logger.Info("CacheFS.dataCache.evict", "path", path, "present", present)
				}
			}
		}
		f.fileMetadataChanged(path)
	}
	return err
}

func (f *fsys) fileMetadataChanged(path string) {
	idx := strings.LastIndex(path, "/")
	if idx != -1 {
		cacheKey := strings.TrimSuffix(path[:idx], "/") + "/"
		present := f.dirCache.Remove(cacheKey)
		if f.logger != nil {
			f.logger.Info("CacheFS.dirCache.evict", "path", cacheKey, "present", present)
		}
	} else {
		present := f.dirCache.Remove("/")
		if f.logger != nil {
			f.logger.Info("CacheFS.dirCache.evict", "path", "/", "present", present)
		}
	}
	cacheKey := strings.TrimSuffix(path, "/") + "/"
	present := f.dirCache.Remove(cacheKey)
	if f.logger != nil {
		f.logger.Info("CacheFS.dirCache.evict", "path", cacheKey, "present", present)
	}
	present = f.statCache.Remove(path)
	if f.logger != nil {
		f.logger.Info("CacheFS.statCache.evict", "path", path, "present", present)
	}
}
