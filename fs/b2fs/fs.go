// Implements a 9p file system that talks to Backblaze's B2 Object Storage
// Service
package b2fs

import (
	"context"
	"os"
	"time"

	"github.com/jeffh/cfs/ninep"
	"github.com/kurin/blazer/b2"
)

type Config struct {
	B2id       string
	B2key      string
	CacheMeta  bool
	Expiration time.Duration
}

func (c *Config) getExpiration() time.Duration {
	if c.Expiration == 0 {
		return 15 * time.Minute
	}
	return c.Expiration
}

type B2Ctx struct {
	Client     *b2.Client
	CacheMeta  bool
	Expiration time.Duration
}

func NewFsFromEnv() (ninep.FileSystem, error) {
	b2id := os.Getenv("B2_ACCOUNT_ID")
	b2key := os.Getenv("B2_ACCOUNT_KEY")
	return NewFs(Config{B2id: b2id, B2key: b2key, CacheMeta: true})
}

// Returns a file system that maps a b2 account to a 9p file system
//
// Structure:
//   /<bucket_type>/ where bucket_type = "all" | "public" | "private" | "snapshot"
//       /objects/data/<key>
//       /objects/metadata/<key>
//       /objects/presigned-download-urls/<key>
//       /versions/<key>
//       /unfinished-uploads/<key>
//       /metadata
func NewFs(cfg Config) (ninep.FileSystem, error) {
	clt, err := b2.NewClient(context.Background(), cfg.B2id, cfg.B2key)
	if err != nil {
		return nil, mapB2ErrToNinep(err)
	}
	b2c := &B2Ctx{clt, cfg.CacheMeta, cfg.getExpiration()}
	fs := &ninep.SimpleWalkableFileSystem{
		ninep.SimpleFileSystem{
			Root: ninep.StaticRootDir(
				&buckets{
					SimpleFileInfo: ninep.SimpleFileInfo{
						FIName:    "all",
						FIMode:    os.ModeDir | 0755,
						FIModTime: time.Now(),
					},
					b2c:        b2c,
					bucketType: "",
				},
				&buckets{
					SimpleFileInfo: ninep.SimpleFileInfo{
						FIName:    "private",
						FIMode:    os.ModeDir | 0755,
						FIModTime: time.Now(),
					},
					b2c:        b2c,
					bucketType: b2.Private,
				},
				&buckets{
					SimpleFileInfo: ninep.SimpleFileInfo{
						FIName:    "public",
						FIMode:    os.ModeDir | 0755,
						FIModTime: time.Now(),
					},
					b2c:        b2c,
					bucketType: b2.Public,
				},
				&buckets{
					SimpleFileInfo: ninep.SimpleFileInfo{
						FIName:    "snapshots",
						FIMode:    os.ModeDir | 0755,
						FIModTime: time.Now(),
					},
					b2c:        b2c,
					bucketType: b2.Snapshot,
				},
			),
		},
	}
	return fs, nil
}
