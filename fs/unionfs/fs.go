package unionfs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jeffh/cfs/fs/proxy"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

// New creates a new union fs with the given initial mounts
func New(factory func(cfg proxy.FileSystemMountConfig) (proxy.FileSystemMount, error)) UnionFileSystem {
	return &unionFS{createMount: factory}
}

type UnionFileSystem interface {
	ninep.FileSystem
	Mount(fs proxy.FileSystemMount) (string, error)
	Unmount(path string) error
	Bind(oldPath, newPath string) error
	Unbind(path string) error
}

const (
	mountPrefix = "#m"
)

var mx = ninep.NewMux().
	Define().Path("/" + mountPrefix).TrailSlash().As("mounts").
	Define().Path("/" + mountPrefix + "/ctl").As("ctl").
	Define().Path("/" + mountPrefix + "/{n}").TrailSlash().As("mount").
	Define().Path("/" + mountPrefix + "/{n}/{path*}").TrailSlash().As("mountPath").
	Define().Path("/").As("root").
	Define().Path("/{path*}").As("path")

// /#m/ctl
//    echo "mount=localhost:6666/foo" > /#m/ctl
//    echo "unmount=/#m/1" > /#m/ctl
//    echo "unbind=/bin" > /#m/ctl
//    echo "bind=/#m/1/bin to=/bin" > /#m/ctl
//    echo "mount=localhost:6666/foo to=/" > /#m/ctl

type unionFS struct {
	mu       sync.RWMutex
	mountIds []uint64
	mounts   []proxy.FileSystemMount // ordered list of all mounts
	nextId   atomic.Uint64

	mu2              sync.RWMutex
	sourcePaths      []string
	destinationPaths [][]string

	createMount func(cfg proxy.FileSystemMountConfig) (proxy.FileSystemMount, error)
}

var _ ninep.FileSystem = (*unionFS)(nil)

func (ufs *unionFS) nextMountId() uint64 {
	return ufs.nextId.Add(1)
}

func (ufs *unionFS) getMountByIndex(indexStr string) (proxy.FileSystemMount, error) {
	index, err := strconv.ParseUint(indexStr, 10, 64)
	if err != nil {
		return proxy.FileSystemMount{}, fs.ErrNotExist
	}
	ufs.mu.RLock()
	defer ufs.mu.RUnlock()
	idx := slices.Index(ufs.mountIds, index)
	if idx == -1 {
		return proxy.FileSystemMount{}, fs.ErrNotExist
	}
	return ufs.mounts[idx], nil
}

func (ufs *unionFS) resolvePath(path string) ([]string, error) {
	mPrefix := "/" + mountPrefix + "/"
	if strings.HasPrefix(path, mPrefix) {
		return []string{path}, nil
	}
	var output []string
	ufs.mu2.RLock()
	defer ufs.mu2.RUnlock()
	stack := []string{path}
	seen := make(map[string]struct{})
	// relativeMatch := false
	for len(stack) > 0 {
		path = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for i, sp := range ufs.sourcePaths {
			relativePath := strings.TrimPrefix(path, sp)
			if relativePath != path {
				// relativeMatch = true
				for _, dp := range ufs.destinationPaths[i] {
					fullPath := filepath.Join(dp, relativePath)
					if strings.HasPrefix(fullPath, mPrefix) {
						if _, ok := seen[fullPath]; !ok {
							output = append(output, ninep.Clean(fullPath))
							seen[fullPath] = struct{}{}
						}
					} else {
						stack = append(stack, fullPath)
					}
				}
			}
		}
	}
	// if !relativeMatch {
	// 	return nil, fs.ErrNotExist
	// }
	return output, nil
}

type mountPath struct {
	mount proxy.FileSystemMount
	path  string
}

func (ufs *unionFS) prepareMounts(paths []string) ([]mountPath, error) {
	if paths == nil {
		return nil, nil
	}
	mounts := make([]mountPath, 0, len(paths))
	mprefix := "/" + mountPrefix + "/"
	var lastErr error
	for _, p := range paths {
		tmp := strings.TrimPrefix(p, mprefix)
		idx := strings.Index(tmp, "/")
		var subpath string
		if idx != -1 {
			subpath = tmp[idx+1:]
			tmp = tmp[:idx]
		}

		mount, err := ufs.getMountByIndex(tmp)
		if err != nil {
			lastErr = err
			continue
		}
		mounts = append(mounts, mountPath{mount, subpath})
	}
	if len(mounts) == 0 {
		return nil, lastErr
	}
	return mounts, nil
}

func (ufs *unionFS) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return fs.ErrNotExist
	}
	switch res.Id {
	case "mounts":
		return fs.ErrPermission
	case "ctl":
		return fs.ErrPermission
	case "mount":
		return fs.ErrPermission
	case "mountPath":
		indexStr := res.Vars[0]
		path := res.Vars[1]
		mount, err := ufs.getMountByIndex(indexStr)
		if err != nil {
			return fs.ErrNotExist
		}
		return mount.FS.MakeDir(ctx, path, mode)
	case "root":
		path = "/"
		fallthrough
	case "path":
		paths, err := ufs.resolvePath(path)
		if err != nil {
			return err
		}
		mpaths, err := ufs.prepareMounts(paths)
		if err != nil {
			return err
		}
		for _, mp := range mpaths {
			err = mp.mount.FS.MakeDir(ctx, filepath.Join(mp.mount.Prefix, mp.path), mode)
			if err != nil {
				continue
			} else {
				return nil
			}
		}
		return err
	default:
		return fs.ErrNotExist
	}
}
func (ufs *unionFS) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	switch res.Id {
	case "mounts":
		return nil, fs.ErrPermission
	case "ctl":
		return nil, fs.ErrPermission
	case "mount":
		return nil, fs.ErrPermission
	case "mountPath":
		indexStr := res.Vars[0]
		path := res.Vars[1]
		mount, err := ufs.getMountByIndex(indexStr)
		if err != nil {
			return nil, fs.ErrNotExist
		}
		return mount.FS.CreateFile(ctx, path, flag, mode)
	case "root":
		path = "/"
		fallthrough
	case "path":
		paths, err := ufs.resolvePath(path)
		if err != nil {
			return nil, err
		}
		mpaths, err := ufs.prepareMounts(paths)
		if err != nil {
			return nil, err
		}
		for _, mp := range mpaths {
			h, err := mp.mount.FS.CreateFile(ctx, filepath.Join(mp.mount.Prefix, mp.path), flag, mode)
			if err != nil {
				continue
			} else {
				return h, nil
			}
		}
		return nil, fs.ErrNotExist
	default:
		return nil, fs.ErrNotExist
	}
}
func (ufs *unionFS) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	switch res.Id {
	case "mounts":
		return nil, fs.ErrPermission
	case "ctl":
		return ufs.CtlHandle(ctx, flag)
	case "mount":
		return nil, fs.ErrPermission
	case "mountPath":
		indexStr := res.Vars[0]
		path := res.Vars[1]
		mount, err := ufs.getMountByIndex(indexStr)
		if err != nil {
			return nil, fs.ErrNotExist
		}
		return mount.FS.OpenFile(ctx, path, flag)
	case "root":
		path = "/"
		fallthrough
	case "path":
		paths, err := ufs.resolvePath(path)
		if err != nil {
			return nil, err
		}
		mpaths, err := ufs.prepareMounts(paths)
		if err != nil {
			return nil, err
		}
		for _, mp := range mpaths {
			h, err := mp.mount.FS.OpenFile(ctx, filepath.Join(mp.mount.Prefix, mp.path), flag)
			if err != nil {
				continue
			} else {
				return h, nil
			}
		}
		return nil, fs.ErrNotExist
	default:
		return nil, fs.ErrNotExist
	}
}
func (ufs *unionFS) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
	switch res.Id {
	case "mounts":
		ufs.mu.RLock()
		ids := make([]uint64, len(ufs.mountIds))
		copy(ids, ufs.mountIds)
		ufs.mu.RUnlock()
		return func(yield func(fs.FileInfo, error) bool) {
			for _, id := range ids {
				info := ninep.DirFileInfo(strconv.FormatUint(id, 10))
				if !yield(info, nil) {
					return
				}
			}
		}
	case "ctl":
		return ninep.FileInfoErrorIterator(ninep.ErrListingOnNonDir)
	case "mount":
		mnt, err := ufs.getMountByIndex(res.Vars[0])
		if err != nil {
			return ninep.FileInfoErrorIterator(err)
		}
		return mnt.FS.ListDir(ctx, ".")
	case "mountPath":
		indexStr := res.Vars[0]
		path := res.Vars[1]
		mount, err := ufs.getMountByIndex(indexStr)
		if err != nil {
			return ninep.FileInfoErrorIterator(err)
		}
		return mount.FS.ListDir(ctx, path)
	case "root":
		return func(yield func(fs.FileInfo, error) bool) {
			if !yield(ninep.DirFileInfo(mountPrefix), nil) {
				return
			}

			seen := make(map[string]struct{})
			matches := ufs.getSourceDirs(seen, "/")
			for _, match := range matches {
				if !yield(ninep.DirFileInfo(match), nil) {
					return
				}
			}

			paths, err := ufs.resolvePath(path)
			if err != nil {
				return
			}
			mpaths, err := ufs.prepareMounts(paths)
			if err != nil {
				return
			}
			for _, mp := range mpaths {
				for h, err := range mp.mount.FS.ListDir(ctx, filepath.Join(mp.mount.Prefix, mp.path)) {
					if _, ok := seen[h.Name()]; ok {
						continue
					}
					if !yield(h, err) {
						return
					}
					seen[h.Name()] = struct{}{}
				}
			}
		}
	case "path":
		path = ninep.Clean(path)
		return func(yield func(fs.FileInfo, error) bool) {
			seen := make(map[string]struct{})
			{
				mpath := path + "/"
				matches := ufs.getSourceDirs(seen, mpath)
				for _, match := range matches {
					if match == mpath {
						continue
					} else {
						if !yield(ninep.DirFileInfo(match), nil) {
							return
						}
					}
				}
			}

			paths, err := ufs.resolvePath(path)
			if err != nil {
				yield(nil, err)
				return
			}
			mpaths, err := ufs.prepareMounts(paths)
			if err != nil {
				yield(nil, err)
				return
			}
			for _, mp := range mpaths {
				fpath := ninep.Clean(filepath.Join(mp.mount.Prefix, mp.path))
				for h, err := range mp.mount.FS.ListDir(ctx, fpath) {
					if err != nil {
						if errors.Is(err, fs.ErrNotExist) {
							break
						}
						yield(nil, err)
						return
					}
					if _, ok := seen[h.Name()]; ok {
						continue
					}
					if !yield(h, err) {
						return
					}
					seen[h.Name()] = struct{}{}
				}
			}
		}
	default:
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
}
func (ufs *unionFS) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return nil, fs.ErrNotExist
	}
	switch res.Id {
	case "mounts":
		return ninep.DirFileInfo(mountPrefix), nil
	case "ctl":
		return ninep.DevFileInfo("ctl"), nil
	case "mount":
		_, err := ufs.getMountByIndex(res.Vars[0])
		if err != nil {
			return nil, err
		}
		return ninep.DirFileInfo(res.Vars[0]), nil
	case "mountPath":
		indexStr := res.Vars[0]
		path := res.Vars[1]
		mount, err := ufs.getMountByIndex(indexStr)
		if err != nil {
			return nil, err
		}
		return mount.FS.Stat(ctx, path)
	case "root":
		path = "/"
		fallthrough
	case "path":
		paths, err := ufs.resolvePath(path)
		if err != nil {
			if path == "/" {
				return ninep.DirFileInfo("."), nil
			}
			return nil, err
		}
		mpaths, err := ufs.prepareMounts(paths)
		if err != nil {
			return nil, err
		}
		for _, mp := range mpaths {
			if info, err := mp.mount.FS.Stat(ctx, mp.path); err == nil {
				return info, nil
			}
		}
		if path == "/" {
			return ninep.DirFileInfo("."), nil
		}
		return nil, fs.ErrNotExist
	default:
		return nil, fs.ErrNotExist
	}
}
func (ufs *unionFS) WriteStat(ctx context.Context, path string, s ninep.Stat) error {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return fs.ErrNotExist
	}
	switch res.Id {
	case "mounts":
		return fs.ErrPermission
	case "ctl":
		return fs.ErrPermission
	case "mount":
		_, err := ufs.getMountByIndex(res.Vars[0])
		if err != nil {
			return err
		}
		return fs.ErrPermission
	case "mountPath":
		indexStr := res.Vars[0]
		path := res.Vars[1]
		mount, err := ufs.getMountByIndex(indexStr)
		if err != nil {
			return err
		}
		return mount.FS.WriteStat(ctx, path, s)
	case "root":
		path = "/"
		fallthrough
	case "path":
		paths, err := ufs.resolvePath(path)
		if err != nil {
			return err
		}
		mpaths, err := ufs.prepareMounts(paths)
		if err != nil {
			return err
		}
		for _, mp := range mpaths {
			if err := mp.mount.FS.WriteStat(ctx, mp.path, s); err == nil {
				return nil
			}
		}
		return fs.ErrNotExist
	default:
		return fs.ErrNotExist
	}
}
func (ufs *unionFS) Delete(ctx context.Context, path string) error {
	var res ninep.Match
	if !mx.Match(path, &res) {
		return fs.ErrNotExist
	}
	switch res.Id {
	case "mounts":
		return fs.ErrPermission
	case "ctl":
		return fs.ErrPermission
	case "mount":
		_, err := ufs.getMountByIndex(res.Vars[0])
		if err != nil {
			return err
		}
		return fs.ErrPermission
	case "mountPath":
		indexStr := res.Vars[0]
		path := res.Vars[1]
		mount, err := ufs.getMountByIndex(indexStr)
		if err != nil {
			return err
		}
		return mount.FS.Delete(ctx, path)
	case "root":
		path = "/"
		fallthrough
	case "path":
		paths, err := ufs.resolvePath(path)
		if err != nil {
			return err
		}
		mpaths, err := ufs.prepareMounts(paths)
		if err != nil {
			return err
		}
		for _, mp := range mpaths {
			if err := mp.mount.FS.Delete(ctx, mp.path); err == nil {
				return nil
			}
		}
		return fs.ErrNotExist
	default:
		return fs.ErrNotExist
	}
}

func (ufs *unionFS) Mount(fsm proxy.FileSystemMount) (string, error) {
	idx := ufs.nextMountId()
	ufs.mu.Lock()
	defer ufs.mu.Unlock()
	ufs.mounts = append(ufs.mounts, fsm)
	ufs.mountIds = append(ufs.mountIds, idx)
	return mountPrefix + "/" + strconv.FormatUint(idx, 10), nil
}
func (ufs *unionFS) Unmount(path string) error {
	path = ninep.Clean(path)
	target := strings.TrimPrefix(path, mountPrefix+"/")
	if target == path {
		return fs.ErrInvalid
	}
	target = strings.TrimSuffix(target, "/")
	idx, err := strconv.ParseUint(target, 10, 64)
	if err != nil {
		return fs.ErrNotExist
	}
	ufs.mu.Lock()
	index := slices.Index(ufs.mountIds, idx)
	if index == -1 {
		ufs.mu.Unlock()
		return fs.ErrNotExist
	}
	ufs.mounts = slices.Delete(ufs.mounts, index, index+1)
	ufs.mountIds = slices.Delete(ufs.mountIds, index, index+1)
	ufs.mu.Unlock()

	ufs.mu2.Lock()
	defer ufs.mu2.Unlock()
	for i, sp := range ufs.sourcePaths {
		if strings.HasPrefix(sp, path) {
			ufs.sourcePaths = slices.Delete(ufs.sourcePaths, i, i+1)
			ufs.destinationPaths = slices.Delete(ufs.destinationPaths, i, i+1)
		}
		for j, dp := range ufs.destinationPaths[i] {
			if strings.HasPrefix(dp, path) {
				ufs.destinationPaths[i] = slices.Delete(ufs.destinationPaths[i], j, j+1)
			}
		}
	}
	return nil
}
func (ufs *unionFS) Bind(srcPath, dstPath string) error {
	srcPath = ninep.Clean(srcPath)
	dstPath = ninep.Clean(dstPath)
	ufs.mu2.Lock()
	defer ufs.mu2.Unlock()
	i := slices.Index(ufs.sourcePaths, dstPath)
	if i == -1 {
		ufs.sourcePaths = append(ufs.sourcePaths, dstPath)
		ufs.destinationPaths = append(ufs.destinationPaths, []string{srcPath})
	} else {
		ufs.destinationPaths[i] = append(ufs.destinationPaths[i], srcPath)
	}
	return nil
}
func (ufs *unionFS) Unbind(path string) error {
	path = ninep.Clean(path)
	ufs.mu2.Lock()
	defer ufs.mu2.Unlock()
	i := slices.Index(ufs.sourcePaths, path)
	if i == -1 {
		return fs.ErrNotExist
	}
	ufs.sourcePaths = slices.Delete(ufs.sourcePaths, i, i+1)
	ufs.destinationPaths = slices.Delete(ufs.destinationPaths, i, i+1)
	return nil
}

func (ufs *unionFS) Walk(ctx context.Context, parts []string) ([]fs.FileInfo, error) {
	infos := make([]fs.FileInfo, 0, len(parts))
	if len(parts) > 1 && parts[len(parts)-1] == "." {
		parts = parts[:len(parts)-1]
	}
	if len(parts) > 0 {
		infos = append(infos, ninep.DirFileInfo("."))
	}
	if len(parts) > 1 && parts[1] == mountPrefix {
		L := len(parts)
		if L >= 2 { // mounts dir
			infos = append(infos, ninep.DirFileInfo(mountPrefix))
		}
		var mount proxy.FileSystemMount
		if L >= 3 { // specific mount or ctl
			if parts[2] == "ctl" {
				infos = append(infos, ninep.DevFileInfo("ctl"))
			} else {
				var err error
				mount, err = ufs.getMountByIndex(parts[2])
				if err != nil {
					return infos, err
				}
				infos = append(infos, ninep.DirFileInfo(parts[2]))
			}
		}
		if L >= 4 { // specific mount + path
			if mount.FS == nil {
				return infos, fs.ErrNotExist
			}
			infos = append(infos, ninep.DirFileInfo(parts[3]))
			addt, err := ninep.Walk(ctx, mount.FS, parts[3:])
			if len(addt) > 0 {
				infos = append(infos, addt...)
			}
			if err != nil {
				return infos, err
			}
		}
		return infos, nil
	} else {
		rpath := ninep.Clean(filepath.Join(parts...))
		// seen := make(map[string]struct{})
		ufs.mu2.RLock()
		var match *string
		for _, spath := range ufs.sourcePaths {
			if strings.HasPrefix(rpath, spath) {
				match = &spath
				break
			}
		}
		ufs.mu2.RUnlock()
		if match != nil {
			subparts := ninep.PathSplit(*match)
			if len(subparts) > 0 {
				infos = append(infos, ninep.DirFileInfo(subparts[len(subparts)-1]))
			}
		}

		paths, err := ufs.resolvePath(rpath)
		if err != nil {
			return nil, err
		}
		mpaths, err := ufs.prepareMounts(paths)
		if err != nil {
			return nil, err
		}
		var best []fs.FileInfo
		for _, mp := range mpaths {
			subparts := strings.Split(mp.path, "/")
			if walkable, ok := mp.mount.FS.(ninep.WalkableFileSystem); ok {
				if infos, err := walkable.Walk(ctx, subparts); err == nil {
					best = infos
					break
				} else if len(best) < len(infos) {
					best = infos
				}
			} else {
				if infos, err := ninep.NaiveWalk(ctx, mp.mount.FS, subparts); err == nil {
					best = infos
					break
				} else if len(best) < len(infos) {
					best = infos
				}
			}
		}
		infos = append(infos, best...)
		return infos, nil
	}
}

func (ufs *unionFS) CtlHandle(ctx context.Context, flag ninep.OpenMode) (ninep.FileHandle, error) {
	h, r, w := ninep.DeviceHandle(flag)
	if w != nil {
		go func() {
			defer w.Close()
			ufs.mu.RLock()
			mounts := make([]string, 0, len(ufs.mounts))
			for i, m := range ufs.mounts {
				index := ufs.mountIds[i]
				mounts = append(mounts, fmt.Sprintf("mount=%q # id=%d", m.Addr+"/"+strings.TrimPrefix(m.Prefix, "/"), index))
			}
			ufs.mu.RUnlock()

			ufs.mu2.RLock()
			srcs := make([]string, len(ufs.sourcePaths))
			dsts := make([][]string, len(ufs.destinationPaths))
			copy(srcs, ufs.sourcePaths)
			for i, dst := range ufs.destinationPaths {
				dsts[i] = append([]string(nil), dst...)
			}
			ufs.mu2.RUnlock()
			for _, m := range mounts {
				fmt.Fprintf(w, "%s\n", m)
			}
			for i, sp := range srcs {
				for _, dp := range dsts[i] {
					fmt.Fprintf(w, "bind=%q to=%q\n", "/"+strings.TrimPrefix(dp, "/"), "/"+strings.TrimPrefix(sp, "/"))
				}
			}
		}()
	}
	if r != nil {
		go func() {
			defer r.Close()
			scanner := bufio.NewScanner(r)
			for scanner.Scan() {
				line := scanner.Text()
				kv, err := kvp.ParseKeyValues(line)
				if err != nil {
					continue
				}
				if path := kv.GetOne("mount"); path != "" {
					to := kv.GetOne("to")
					mountConfig, ok := proxy.ParseMount(path)
					if !ok {
						continue
					}
					mnt, err := ufs.createMount(mountConfig)
					if err != nil {
						continue
					}
					path, err := ufs.Mount(mnt)
					if err != nil {
						continue
					}
					if to != "" {
						err = ufs.Bind(path, to)
						if err != nil {
							continue
						}
					}
				}
				if path := kv.GetOne("unmount"); path != "" {
					err = ufs.Unmount(path)
					if err != nil {
						continue
					}
				}
				if path := kv.GetOne("bind"); path != "" {
					to := kv.GetOne("to")
					if to == "" {
						to = "/"
					}
					err = ufs.Bind(path, to)
					if err != nil {
						continue
					}
				}
				if path := kv.GetOne("unbind"); path != "" {
					err = ufs.Unbind(path)
					if err != nil {
						continue
					}
				}
			}
		}()
	}
	return h, nil
}

func (ufs *unionFS) getSourceDirs(seen map[string]struct{}, path string) []string {
	path = ninep.Clean(path)
	ufs.mu2.RLock()
	defer ufs.mu2.RUnlock()
	dirs := make([]string, 0, len(ufs.sourcePaths))
	for _, sp := range ufs.sourcePaths {
		relPath := strings.TrimPrefix(sp+"/", path)
		if relPath != path {
			dir := ninep.NextSegment(relPath)
			if dir == "." {
				continue
			}
			if _, ok := seen[dir]; !ok {
				dirs = append(dirs, dir)
			}
		}
	}
	return dirs
}
