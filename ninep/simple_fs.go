package ninep

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type Node interface {
	Info() (os.FileInfo, error)
	WriteInfo(in os.FileInfo) error
}

type RenamedNode struct {
	Node
	NewName string
}

func (n *RenamedNode) Info() (os.FileInfo, error) {
	info, err := n.Node.Info()
	if err != nil {
		return nil, err
	}
	return FileInfoWithName(info, n.NewName), nil
}

// A simple interface for a file. File Systems may use this to easily structure
// a file system in memory
type File interface {
	Node
	Open() (FileHandle, error)
}

type Dir interface {
	Node
	List() (NodeIterator, error)
	CreateFile(name string, flag OpenMode, mode Mode) (FileHandle, error)
	CreateDir(name string, mode Mode) error
	Delete(name string) error
}

// Optionally support to walk to a specific node in a Dir. This can provide
// better performance for directories that can be large.
type StepDir interface {
	Dir
	Step(name string) (Node, error)
}

// Optionally support walking for Dirs. Unlike StepDir, this interface receives
// the entire child path to the desired file. This can provide better
// performance for directories that can be large through recursive paths.
type WalkDir interface {
	Dir
	Walk(subpath []string) (Node, error)
}

type NodeIterator interface {
	NextNode() (Node, error)
	Reset() error
	Close() error
}

// Provides a simpler, object-oriented file system interface at the expense of
// CPU/memory costs. Useful for small file systems
type SimpleFileSystem struct {
	Root Node
}

func (f *SimpleFileSystem) walk(path string, walkLast bool) (Node, string, error) {
	return Walk(f.Root, path, walkLast)
}

func (f *SimpleFileSystem) MakeDir(path string, mode Mode) error {
	node, name, err := f.walk(path, false)
	if err != nil {
		return err
	}

	dir, ok := node.(Dir)
	if !ok {
		return fmt.Errorf("Cannot create directory under a path that is a file: %s", path)
	}

	return dir.CreateDir(name, mode)
}

func (f *SimpleFileSystem) CreateFile(path string, flag OpenMode, mode Mode) (FileHandle, error) {
	node, name, err := f.walk(path, false)
	if err != nil {
		return nil, err
	}

	dir, ok := node.(Dir)
	if !ok {
		return nil, fmt.Errorf("Cannot create file under a path that is a file: %s", path)
	}

	return dir.CreateFile(name, flag, mode)
}
func (f *SimpleFileSystem) OpenFile(path string, flag OpenMode) (FileHandle, error) {
	node, _, err := f.walk(path, true)
	if err != nil {
		return nil, err
	}

	file, ok := node.(File)
	if !ok {
		return nil, fmt.Errorf("Cannot open directory: %s", path)
	}

	return file.Open()
}
func (f *SimpleFileSystem) ListDir(path string) (FileInfoIterator, error) {
	node, _, err := f.walk(path, true)
	if err != nil {
		return nil, err
	}

	it, err := node.(Dir).List()
	if err != nil {
		return nil, err
	}

	return &fileInfoNodeIterator{it}, nil
}
func (f *SimpleFileSystem) Stat(path string) (os.FileInfo, error) {
	node, _, err := f.walk(path, true)
	if err != nil {
		return nil, err
	}
	return node.Info()
}

func (f *SimpleFileSystem) WriteStat(path string, s Stat) error {
	node, name, err := f.walk(path, false)
	if err != nil {
		return err
	}
	// TODO: FIXME, need to pass along name
	_ = name
	info := StatFileInfo{s}
	return node.WriteInfo(info)
}

func (f *SimpleFileSystem) Delete(path string) error {
	node, name, err := f.walk(path, false)
	if err != nil {
		return err
	}
	dir, ok := node.(Dir)
	if !ok {
		return os.ErrNotExist
	}
	return dir.Delete(name)
}

/////////////////////////////////////////////

func FilterNodeIterator(it NodeIterator, filter func(Node) bool) NodeIterator {
	return &nodeFilterIterator{it, filter}
}

type nodeFilterIterator struct {
	it     NodeIterator
	filter func(n Node) bool
}

func (itr *nodeFilterIterator) Close() error { return itr.it.Close() }
func (itr *nodeFilterIterator) Reset() error { return itr.it.Reset() }
func (itr *nodeFilterIterator) NextNode() (Node, error) {
	for {
		n, err := itr.it.NextNode()
		if n != nil && itr.filter(n) {
			return n, nil
		}

		if err != nil {
			return nil, err
		}
	}
}

/////////////////////////////////////////////

type nodeSliceIterator struct {
	nodes []Node
	index int
}

func makeNodeSliceIterator(fi []Node) NodeIterator {
	return &nodeSliceIterator{fi, 0}
}

func (itr *nodeSliceIterator) Close() error { return nil }
func (itr *nodeSliceIterator) Reset() error { itr.index = 0; return nil }
func (itr *nodeSliceIterator) NextNode() (Node, error) {
	idx := itr.index
	if idx >= len(itr.nodes) {
		return nil, io.EOF
	}
	itr.index++
	return itr.nodes[idx], nil
}

/////////////////////////////////////////////

func NodeToFileInfoIterator(it NodeIterator) FileInfoIterator {
	return &fileInfoNodeIterator{it}
}
func PassNodeToFileInfoIterator(it NodeIterator, err error) (FileInfoIterator, error) {
	if it == nil {
		return nil, err
	}
	return &fileInfoNodeIterator{it}, err
}

type fileInfoNodeIterator struct {
	it NodeIterator
}

func (i *fileInfoNodeIterator) NextFileInfo() (os.FileInfo, error) {
	node, err := i.it.NextNode()
	if err != nil {
		return nil, err
	}
	return node.Info()
}

func (i *fileInfoNodeIterator) Reset() error { return i.it.Reset() }
func (i *fileInfoNodeIterator) Close() error { return i.it.Close() }

/////////////////////////////////////////////

// Creates a static directory that can't be modified
type StaticReadOnlyDir struct {
	SimpleFileInfo
	Children []Node
}

func (d *StaticReadOnlyDir) Info() (os.FileInfo, error)     { return d, nil }
func (d *StaticReadOnlyDir) WriteInfo(in os.FileInfo) error { return ErrUnsupported }
func (d *StaticReadOnlyDir) Delete(name string) error       { return ErrUnsupported }
func (d *StaticReadOnlyDir) List() (NodeIterator, error) {
	return makeNodeSliceIterator(d.Children), nil
}
func (d *StaticReadOnlyDir) CreateFile(name string, flag OpenMode, mode Mode) (FileHandle, error) {
	return nil, ErrUnsupported
}
func (d *StaticReadOnlyDir) CreateDir(name string, mode Mode) error {
	return ErrUnsupported
}

func StaticRootDir(children ...Node) *StaticReadOnlyDir {
	return &StaticReadOnlyDir{
		SimpleFileInfo: SimpleFileInfo{
			FIName: "",
			FIMode: os.ModeDir | 0777,
		},
		Children: children,
	}
}

/////////////////////////////////////////////

// Creates a dynamic, readonly directory that can't be modified
type DynamicReadOnlyDir struct {
	SimpleFileInfo
	GetChildren func() ([]Node, error)
}

func (d *DynamicReadOnlyDir) Info() (os.FileInfo, error)     { return d, nil }
func (d *DynamicReadOnlyDir) WriteInfo(in os.FileInfo) error { return ErrUnsupported }
func (d *DynamicReadOnlyDir) Delete(name string) error       { return ErrUnsupported }
func (d *DynamicReadOnlyDir) List() (NodeIterator, error) {
	nodes, err := d.GetChildren()
	if err != nil {
		return nil, err
	}
	return makeNodeSliceIterator(nodes), nil
}
func (d *DynamicReadOnlyDir) CreateFile(name string, flag OpenMode, mode Mode) (FileHandle, error) {
	return nil, ErrUnsupported
}
func (d *DynamicReadOnlyDir) CreateDir(name string, mode Mode) error {
	return ErrUnsupported
}

/////////////////////////////////////////////

// Creates a dynamic, readonly directory that can't be modified. Allows arbitrary nested paths.
// Unlike normal directories, displays children as "top-level" children
type DynamicReadOnlyDirTree struct {
	SimpleFileInfo
	GetFlatTree func() ([]Node, error)
}

func (d *DynamicReadOnlyDirTree) Info() (os.FileInfo, error)     { return d, nil }
func (d *DynamicReadOnlyDirTree) WriteInfo(in os.FileInfo) error { return ErrUnsupported }
func (d *DynamicReadOnlyDirTree) Delete(name string) error       { return ErrUnsupported }
func (d *DynamicReadOnlyDirTree) List() (NodeIterator, error) {
	nodes, err := d.GetFlatTree()
	if err != nil {
		return nil, err
	}
	return makeNodeSliceIterator(nodes), nil
}
func (d *DynamicReadOnlyDirTree) Walk(subpath []string) (Node, error) {
	path := strings.Join(subpath, "/")
	nodes, err := d.GetFlatTree()
	if err != nil {
		return nil, err
	}
	for _, n := range nodes {
		info, err := n.Info()
		if err != nil {
			return nil, err
		}
		if info.Name() == path {
			return n, nil
		}
	}

	// fallback, try prefix matching
	slash := 0
	if len(path) > 0 && path[len(path)-1] != '/' {
		slash += 1
	}
	var res []Node
	for _, n := range nodes {
		info, err := n.Info()
		if err != nil {
			return nil, err
		}
		name := info.Name()
		if IsSubpath(name, path) {
			res = append(res, &RenamedNode{n, name[len(path)+slash:]})
		}
	}
	if len(res) > 0 {
		fi := d.SimpleFileInfo
		fi.FIName = path
		dir := &DynamicReadOnlyDirTree{
			SimpleFileInfo: fi,
			GetFlatTree: func() ([]Node, error) {
				return res, nil
			},
		}
		return dir, nil
	}

	return nil, os.ErrNotExist
}
func (d *DynamicReadOnlyDirTree) CreateFile(name string, flag OpenMode, mode Mode) (FileHandle, error) {
	return nil, ErrUnsupported
}
func (d *DynamicReadOnlyDirTree) CreateDir(name string, mode Mode) error {
	return ErrUnsupported
}

/////////////////////////////////////////////

func StaticReadOnlyFile(name string, mode os.FileMode, modTime time.Time, b []byte) *SimpleFile {
	return &SimpleFile{
		SimpleFileInfo: SimpleFileInfo{
			FIName:    name,
			FIMode:    mode,
			FIModTime: modTime,
		},
		OpenFn: func() (FileHandle, error) {
			return &ReadOnlyMemoryFileHandle{b}, nil
		},
	}
}

func DynamicReadOnlyFile(name string, mode os.FileMode, modTime time.Time, open func() ([]byte, error)) *SimpleFile {
	return &SimpleFile{
		SimpleFileInfo: SimpleFileInfo{
			FIName:    name,
			FIMode:    mode,
			FIModTime: modTime,
		},
		OpenFn: func() (FileHandle, error) {
			b, err := open()
			return &ReadOnlyMemoryFileHandle{b}, err
		},
	}
}

/////////////////////////////////////////////////////////////////////////////

func FindChild(root Node, name string) (Node, os.FileInfo, error) {
	if name != "" {
		var (
			foundNode Node

			itr NodeIterator
			err error

			info    os.FileInfo
			infoErr error
		)

		if walkDir, ok := root.(StepDir); ok {
			n, err := walkDir.Step(name)
			if err != nil {
				return nil, nil, err
			}
			info, err := n.Info()
			if err != nil {
				return nil, nil, err
			}
			return n, info, nil
		}

		rootDir, ok := root.(Dir)
		if !ok {
			return nil, nil, ErrListingOnNonDir
		}

		itr, err = rootDir.List()
		if err != nil {
			return nil, nil, err
		}
		for {
			node, err := itr.NextNode()
			if node != nil {
				info, infoErr = node.Info()
				if infoErr != nil {
					itr.Close()
					return nil, nil, infoErr
				}

				if info.Name() == name {
					foundNode = node
					break
				}
			}

			if err == io.EOF {
				itr.Close()
				return nil, info, os.ErrNotExist
			} else if err != nil {
				itr.Close()
				return nil, info, err
			}
		}
		itr.Close()
		return foundNode, info, err
	} else {
		return nil, nil, os.ErrNotExist
	}
}

// Walks a given node using the path as guidance. Returns the filename reached.
func Walk(root Node, path string, walkLast bool) (Node, string, error) {
	currNode := root
	parts := PathSplit(path)[1:]
	if len(parts) == 0 {
		return currNode, "", nil
	}

	lastPart := parts[len(parts)-1]
	dirParts := parts[:len(parts)-1]
	for i, part := range dirParts {
		var (
			node Node
			info os.FileInfo
			err  error
		)
		if walkNode, ok := currNode.(WalkDir); ok {
			node, err = walkNode.Walk(parts[i:])
			if err != nil {
				return nil, lastPart, err
			}
			return node, lastPart, err
		} else {
			node, info, err = FindChild(currNode, part)
		}
		if err != nil {
			return nil, lastPart, err
		}

		if !info.IsDir() {
			return nil, lastPart, os.ErrNotExist
		}

		currNode = node
	}

	if walkLast && lastPart != "" {
		var (
			node Node
			err  error
		)
		if walkNode, ok := currNode.(WalkDir); ok {
			node, err = walkNode.Walk(parts[len(parts)-1:])

		} else {
			node, _, err = FindChild(currNode, lastPart)
		}
		if err != nil {
			return nil, lastPart, err
		}
		currNode = node
	}

	return currNode, lastPart, nil
}
