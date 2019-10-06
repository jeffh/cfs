package fs

import (
	"fmt"
	"os"
	"sync"
	"time"

	ninep "github.com/jeffh/cfs/ninep"
)

// Provides an easy struct to conform to os.FileInfo
// Note: can "leak" memory because truncate only slices and doesn't free
type MemFileInfo struct {
	FIName    string
	FISize    int64
	FIMode    os.FileMode
	FIModTime time.Time
	FISys     interface{}
}

func (f *MemFileInfo) Name() string       { return f.FIName }
func (f *MemFileInfo) Size() int64        { return f.FISize }
func (f *MemFileInfo) Mode() os.FileMode  { return f.FIMode }
func (f *MemFileInfo) ModTime() time.Time { return f.FIModTime }
func (f *MemFileInfo) IsDir() bool        { return f.FIMode&os.ModeDir != 0 }
func (f *MemFileInfo) Sys() interface{}   { return f.FISys }

////////////////////////////////////////////////

// Implements a basic file system in memory only
// Also, not a particularly efficient implementation
type Mem struct {
	Root memNode
}

type memFileHandle struct {
	n *memNode
}

func (b *memFileHandle) ReadAt(p []byte, off int64) (n int, err error) {
	b.n.mut.RLock()
	n = copy(p, b.n.contents[int(off):int(off)+len(p)])
	b.n.mut.RUnlock()
	return
}

func (b *memFileHandle) WriteAt(p []byte, off int64) (n int, err error) {
	b.n.mut.Lock()
	startLen := len(b.n.contents)
	rst := make([]byte, startLen-int(off))
	copy(rst, b.n.contents[off:])
	b.n.contents = append(b.n.contents[:off], p...)
	b.n.contents = append(b.n.contents, rst...)

	n = len(b.n.contents) - startLen
	b.n.modTime = time.Now()
	b.n.mut.Unlock()
	return
}

func (b *memFileHandle) Sync() error  { return nil }
func (b *memFileHandle) Close() error { return nil }

type memNode struct {
	name   string
	isFile bool

	m        sync.RWMutex
	children []memNode

	mut      sync.RWMutex
	contents []byte
	modTime  time.Time
}

func (n *memNode) FindChild(name string) *memNode {
	if name == "" {
		return n
	}
	n.m.RLock()
	defer n.m.RUnlock()
	for i, child := range n.children {
		if child.name == name {
			return &n.children[i]
		}
	}
	return nil
}

func (n *memNode) RemoveChild(name string) bool {
	n.m.Lock()
	defer n.m.Unlock()
	for i, child := range n.children {
		if child.name == name {
			copy(n.children[i:], n.children[i+1:])
			n.children[len(n.children)-1] = memNode{}
			n.children = n.children[:len(n.children)-1]
			return true
		}
	}
	return false
}

func (m *Mem) openFile(n *memNode) (ninep.FileHandle, error) {
	if !n.isFile {
		panic(fmt.Errorf("implementation error: opening a dir: %#v", n.name))
	}
	return &memFileHandle{n}, nil
}

func (m *Mem) traverse(parts []string) (*memNode, error) {
	n := &m.Root
	for i, l := 0, len(parts); i < l; i++ {
		if parts[i] == "" {
			parts = append(parts[:i], parts[i+1:]...)
			l--
		}
	}
walk:
	for _, seg := range parts {
		n.m.RLock()
		n.m.RUnlock()
		if child := n.FindChild(seg); child != nil {
			if child.isFile {
				return nil, fmt.Errorf("Cannot traverse file: %s", seg)
			}
			n = child
			continue walk
		} else {
			return nil, os.ErrNotExist
		}
	}
	return n, nil
}

func (m *Mem) traverseFile(parts []string) (node *memNode, parent *memNode, err error) {
	last := len(parts) - 1
	n, err := m.traverse(parts[:last-1])
	if err != nil {
		return nil, nil, err
	}

	if child := n.FindChild(parts[last]); child != nil {
		return child, n, nil
	} else {
		return nil, n, os.ErrNotExist
	}
}

func (m *Mem) MakeDir(path string, mode ninep.Mode) error {
	parts := ninep.PathSplit(path)
	last := len(parts) - 1
	n, err := m.traverse(parts[:last-1])
	if err != nil {
		return err
	}

	nc := memNode{name: parts[last]}
	n.m.Lock()
	n.children = append(n.children, nc)
	n.m.Unlock()
	// TODO: support modes
	return nil
}

func (m *Mem) CreateFile(path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	parts := ninep.PathSplit(path)
	n, parent, err := m.traverseFile(parts)
	if !os.IsNotExist(err) && err != nil {
		return nil, err
	}

	if n != nil {
		if !n.isFile {
			return nil, fmt.Errorf("Topen: Cannot create file where dir exists: %s", n.name)
		}
	} else {
		nc := memNode{name: parts[len(parts)-1], isFile: true, modTime: time.Now()}
		parent.m.Lock()
		parent.children = append(parent.children, nc)
		n = &parent.children[len(parent.children)-1]
		parent.m.Unlock()
	}
	// TODO: support modes
	return m.openFile(n)
}

func (m *Mem) OpenFile(path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	parts := ninep.PathSplit(path)
	last := len(parts) - 1
	n, err := m.traverse(parts[:last-1])
	if err != nil {
		return nil, err
	}

	if child := n.FindChild(parts[last]); child != nil {
		if !child.isFile {
			return nil, fmt.Errorf("Topen: Cannot open file: %s", parts[last])
		}
		return m.openFile(child)
	} else {
		return nil, os.ErrNotExist
	}
}

func (m *Mem) stat(n *memNode) *MemFileInfo {
	n.mut.RLock()
	size := int64(len(n.contents))
	modTime := n.modTime
	n.mut.RUnlock()

	mode := os.ModePerm // TODO: support modes
	if !n.isFile {
		mode |= os.ModeDir
	}

	return &MemFileInfo{
		FIName:    n.name,
		FISize:    size,
		FIMode:    mode,
		FIModTime: modTime,
	}
}

func (m *Mem) ListDir(path string) ([]os.FileInfo, error) {
	parts := ninep.PathSplit(path)
	n, err := m.traverse(parts)
	if err != nil {
		return nil, err
	}

	infos := make([]os.FileInfo, len(n.children))
	for i := range n.children {
		infos[i] = m.stat(&n.children[i])
	}

	return infos, nil
}

func (m *Mem) Stat(path string) (os.FileInfo, error) {
	parts := ninep.PathSplit(path)
	child, _, err := m.traverseFile(parts)
	if err != nil {
		return nil, err
	} else if child != nil {
		return m.stat(child), nil
	} else {
		return nil, os.ErrNotExist
	}
}

func (m *Mem) WriteStat(path string, s ninep.Stat) error {
	parts := ninep.PathSplit(path)
	last := len(parts) - 1
	n, err := m.traverse(parts[:last-1])
	if err != nil {
		return err
	}

	if child := n.FindChild(parts[last]); child != nil {
		if !child.isFile {
			err = fmt.Errorf("Wstat: Cannot open file: %s", parts[last])
		}
		// for restoring:
		// "Either all the changes in wstat request happen, or none of them does:
		// if the request succeeds, all changes were made; if it fails, none were."
		info := m.stat(child)
		child.mut.Lock()
		defer child.mut.Unlock()

		if !s.NameNoTouch() && path != s.Name() {
			// TODO: check if already exists
			if n.FindChild(s.Name()) != nil {
				err = fmt.Errorf("Cannot rename to file that already exists: %s", s.Name())
				return err
			}
			oldName := child.name
			child.name = s.Name()

			defer func() {
				if err != nil {
					child.name = oldName
				}
			}()
		}

		if !s.ModeNoTouch() {
			// TODO: implement me
			// old := info.Mode()
			// err = os.Chmod(fullPath, s.Mode().ToOsMode())
			// if err != nil {
			// 	return err
			// }
			// defer func() {
			// 	if err != nil {
			// 		os.Chmod(fullPath, old)
			// 	}
			// }()
		}

		changeGid := !s.GidNoTouch()
		changeUid := !s.UidNoTouch()
		// NOTE(jeff): technically, the spec disallows changing uids
		if changeUid {
			return ninep.ErrChangeUidNotAllowed
		}
		if changeGid {
			return ninep.ErrUnsupported
		}

		if !s.MtimeNoTouch() {
			oldMtime := info.ModTime()

			t := time.Unix(int64(s.Mtime()), 0)
			child.modTime = t
			defer func() {
				if err != nil {
					child.modTime = oldMtime
				}
			}()
		}
		if !s.AtimeNoTouch() {
			// TODO: implement
		}

		// this should be last since it's really hard to undo this
		if !s.LengthNoTouch() {
			size := int(s.Length())
			currSize := len(child.contents)
			if currSize > size {
				child.contents = child.contents[:size]
			} else if currSize < size {
				for i, j := 0, currSize-size; i < j; i++ {
					child.contents = append(child.contents, 0)
				}
			}
		}
		return err
	} else {
		return os.ErrNotExist
	}
}

func (m *Mem) Delete(path string) error {
	parts := ninep.PathSplit(path)
	last := len(parts) - 1
	n, err := m.traverse(parts[:last-1])
	if err != nil {
		return err
	}

	if n.RemoveChild(parts[last]) {
		return nil
	} else {
		return os.ErrNotExist
	}
}
