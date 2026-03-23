// muxfs provides a file system based on a mux. This can be used to provide a simple file system
package muxfs

import (
	"context"
	"io/fs"
	"iter"

	"github.com/jeffh/cfs/ninep"
)

type FS struct {
	m *ninep.Mux[Node]
}

// New returns a new file system based on the given mux.
func New(m *ninep.Mux[Node]) ninep.FileSystem { return &FS{m} }

// NewMux returns a new mux with the root directory set to the root node.
func NewMux() *ninep.Mux[Node] {
	return ninep.NewMuxWith[Node]().
		Define().Path("/").With(Dir(ninep.RootFileInfo)).As("root")
}

// DirName returns a node for a directory with the given name.
func DirName(name string) Node { return Dir(ninep.DirFileInfo(name)) }

// DevName returns a node for a device with the given name.
func DevName(name string) Node { return File(ninep.DevFileInfo(name)) }

// Dir returns a node for a directory.
func Dir(info fs.FileInfo) Node {
	if !info.IsDir() {
		panic("not a directory")
	}
	return Node{
		Stat: func(ctx context.Context, m ninep.MatchWith[Node]) (fs.FileInfo, error) { return info, nil },
	}
}

// File returns a node for a file.
func File(info fs.FileInfo) Node {
	if info.IsDir() {
		panic("not a file")
	}
	return Node{
		Stat: func(ctx context.Context, m ninep.MatchWith[Node]) (fs.FileInfo, error) { return info, nil },
	}
}

type Node struct {
	Stat      func(ctx context.Context, m ninep.MatchWith[Node]) (fs.FileInfo, error)
	WriteStat func(ctx context.Context, m ninep.MatchWith[Node], info ninep.Stat) error
	ListDir   func(ctx context.Context, m ninep.MatchWith[Node]) iter.Seq2[fs.FileInfo, error]
	OpenFile  func(ctx context.Context, m ninep.MatchWith[Node], flag ninep.OpenMode) (ninep.FileHandle, error)
	Delete    func(ctx context.Context, m ninep.MatchWith[Node]) error
	MakeDir   func(ctx context.Context, m ninep.MatchWith[Node], mode ninep.Mode) error
	Create    func(ctx context.Context, m ninep.MatchWith[Node], flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error)
}

func (f *FS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	var res ninep.MatchWith[Node]
	if !f.m.Match(path, &res) {
		return fs.ErrNotExist
	}
	if res.Value.MakeDir == nil {
		return fs.ErrPermission
	}
	return res.Value.MakeDir(ctx, res, mode)
}

func (f *FS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	var res ninep.MatchWith[Node]
	if !f.m.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	if res.Value.Create == nil {
		return nil, fs.ErrPermission
	}
	return res.Value.Create(ctx, res, flag, mode)
}

func (f *FS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.MatchWith[Node]
	if !f.m.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	if res.Value.OpenFile == nil {
		return nil, fs.ErrPermission
	}
	return res.Value.OpenFile(ctx, res, flag)
}

func (f *FS) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.MatchWith[Node]
	if !f.m.Match(path, &res) {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
	if res.Value.ListDir == nil {
		info, err := f.Stat(ctx, path)
		if err != nil {
			return ninep.FileInfoErrorIterator(err)
		}
		if !info.IsDir() {
			return ninep.FileInfoErrorIterator(ninep.ErrListingOnNonDir)
		}
		return func(yield func(fs.FileInfo, error) bool) {
			for _, m := range f.m.List(path) {
				info, err := m.Value.Stat(ctx, m)
				if err != nil || info == nil {
					continue
				}
				if !yield(info, nil) {
					return
				}
			}
		}
	} else {
		return res.Value.ListDir(ctx, res)
	}
}

func (f *FS) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	var res ninep.MatchWith[Node]
	if !f.m.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	if res.Value.Stat == nil {
		return nil, fs.ErrPermission
	}
	return res.Value.Stat(ctx, res)
}

func (f *FS) WriteStat(ctx context.Context, path string, info ninep.Stat) error {
	var res ninep.MatchWith[Node]
	if !f.m.Match(path, &res) {
		return fs.ErrNotExist
	}
	if res.Value.WriteStat == nil {
		return fs.ErrPermission
	}
	return res.Value.WriteStat(ctx, res, info)
}

func (f *FS) Delete(ctx context.Context, path string) error {
	var res ninep.MatchWith[Node]
	if !f.m.Match(path, &res) {
		return fs.ErrNotExist
	}
	if res.Value.Delete == nil {
		return fs.ErrPermission
	}
	return res.Value.Delete(ctx, res)
}
