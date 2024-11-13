package unionfs

import (
	"context"
	"io/fs"
	"iter"
	"path/filepath"

	"github.com/jeffh/cfs/fs/proxy"
)

func makeUnionIterator(ctx context.Context, path string, fsms []proxy.FileSystemMount) iter.Seq2[fs.FileInfo, error] {
	seenPaths := make(map[string]bool)
	seen := func(fpath string) bool {
		_, ok := seenPaths[path]
		seenPaths[path] = true
		return ok
	}
	return func(yield func(fs.FileInfo, error) bool) {
		for _, fsm := range fsms {
			for entry, err := range fsm.FS.ListDir(ctx, filepath.Join(fsm.Prefix, path)) {
				fpath := filepath.Join(path, entry.Name())
				if !seen(fpath) && !yield(entry, err) {
					return
				}
			}
		}
	}
}
