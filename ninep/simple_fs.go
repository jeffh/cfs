package ninep

import (
	"fmt"
	"io"
	"os"
	"time"
)

type Node interface {
	Info() (os.FileInfo, error)
	WriteInfo(in os.FileInfo) error
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
	currNode := f.Root
	parts := PathSplit(path)[1:]
	lastPart := parts[len(parts)-1]
	dirParts := parts[:len(parts)-1]
	for _, part := range dirParts {
		itr, err := currNode.(Dir).List()
		if err != nil {
			return nil, lastPart, err
		}
		for {
			node, err := itr.NextNode()
			info, infoErr := node.Info()
			if infoErr != nil {
				itr.Close()
				return nil, lastPart, infoErr
			}

			if info.Name() == part {
				if !info.IsDir() {
					itr.Close()
					return nil, lastPart, os.ErrNotExist
				}

				currNode = node
				break
			}

			if err == io.EOF {
				itr.Close()
				return nil, lastPart, os.ErrNotExist
			} else if err != nil {
				itr.Close()
				return nil, lastPart, err
			}
		}
		itr.Close()
	}

	if walkLast && lastPart != "" {
		itr, err := currNode.(Dir).List()
		if err != nil {
			return nil, lastPart, err
		}
		for {
			node, err := itr.NextNode()
			if node != nil {
				info, infoErr := node.Info()
				if infoErr != nil {
					itr.Close()
					return nil, lastPart, infoErr
				}

				if info.Name() == lastPart {
					currNode = node
					break
				}
			}

			if err == io.EOF {
				itr.Close()
				return nil, lastPart, os.ErrNotExist
			} else if err != nil {
				itr.Close()
				return nil, lastPart, err
			}
		}
		itr.Close()
	}

	return currNode, lastPart, nil
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

	return &FileInfoNodeIterator{it}, nil
}
func (f *SimpleFileSystem) Stat(path string) (os.FileInfo, error) {
	node, _, err := f.walk(path, true)
	if err != nil {
		return nil, err
	}
	return node.Info()
}

func (f *SimpleFileSystem) WriteStat(path string, s Stat) error {
	node, _, err := f.walk(path, true)
	if err != nil {
		return err
	}
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

type FileInfoNodeIterator struct {
	it NodeIterator
}

func (i *FileInfoNodeIterator) NextFileInfo() (os.FileInfo, error) {
	node, err := i.it.NextNode()
	if err != nil {
		return nil, err
	}
	return node.Info()
}

func (i *FileInfoNodeIterator) Reset() error { return i.it.Reset() }
func (i *FileInfoNodeIterator) Close() error { return i.it.Close() }

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
			FIMode: os.ModeDir | 0222,
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
