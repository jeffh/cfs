package ninep

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"strings"
	"time"
)

type Node interface {
	Info() (os.FileInfo, error)
	WriteInfo(in os.FileInfo) error
}

type NameSortableNodes []Node

func (s NameSortableNodes) Len() int      { return len(s) }
func (s NameSortableNodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s NameSortableNodes) Less(i, j int) bool {
	const iIsSmaller = true

	a, aErr := s[i].Info()
	b, bErr := s[j].Info()
	if bErr != nil {
		return iIsSmaller
	}
	if aErr != nil {
		return !iIsSmaller
	}
	return a.Name() < b.Name()
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
	Open(m OpenMode) (FileHandle, error)
}

type Dir interface {
	Node
	List() iter.Seq2[Node, error]
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
	Walk(subpath []string) ([]Node, error)
}

type DeleteWithModeDir interface {
	Dir
	DeleteWithMode(name string, m Mode) error
}

type NodeIterator interface {
	// return err = io.EOF to indicate not more nodes
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
	return WalkNode(f.Root, path, walkLast)
}

func (f *SimpleFileSystem) MakeDir(ctx context.Context, path string, mode Mode) error {
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

func (f *SimpleFileSystem) CreateFile(ctx context.Context, path string, flag OpenMode, mode Mode) (FileHandle, error) {
	node, name, err := f.walk(path, false)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, os.ErrNotExist
	}

	dir, ok := node.(Dir)
	if !ok {
		return nil, fmt.Errorf("Cannot create file under a path that is a file: %s (node: %#v)", path, dir)
	}

	return dir.CreateFile(name, flag, mode)
}
func (f *SimpleFileSystem) OpenFile(ctx context.Context, path string, flag OpenMode) (FileHandle, error) {
	node, _, err := f.walk(path, true)
	if err != nil {
		return nil, err
	}

	file, ok := node.(File)
	if !ok {
		return nil, fmt.Errorf("Cannot open directory: %s", path)
	}

	return file.Open(flag)
}
func (f *SimpleFileSystem) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		node, _, err := f.walk(path, true)
		if err != nil {
			yield(nil, err)
			return
		}

		for node, err := range node.(Dir).List() {
			if err != nil {
				yield(nil, err)
				return
			}
			info, err := node.Info()
			if !yield(info, err) {
				return
			}
		}
	}
}
func (f *SimpleFileSystem) Stat(ctx context.Context, path string) (os.FileInfo, error) {
	node, _, err := f.walk(path, true)
	if err != nil {
		return nil, err
	}
	return node.Info()
}

func (f *SimpleFileSystem) WriteStat(ctx context.Context, path string, s Stat) error {
	node, name, err := f.walk(path, false)
	if err != nil {
		return err
	}
	// TODO: FIXME, need to pass along name
	_ = name
	info := StatFileInfo{s}
	return node.WriteInfo(info)
}

func (f *SimpleFileSystem) Delete(ctx context.Context, path string) error {
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

var _ DeleteWithModeFileSystem = (*SimpleFileSystem)(nil)

func (f *SimpleFileSystem) DeleteWithMode(ctx context.Context, path string, m Mode) error {
	node, name, err := f.walk(path, false)
	if err != nil {
		return err
	}
	dir, ok := node.(DeleteWithModeDir)
	if !ok {
		dir, ok := node.(Dir)
		if !ok {
			return os.ErrNotExist
		}
		return dir.Delete(name)
	}
	return dir.DeleteWithMode(name, m)
}

/////////////////////////////////////////////

type SimpleWalkableFileSystem struct {
	SimpleFileSystem
}

var _ WalkableFileSystem = (*SimpleWalkableFileSystem)(nil)

func (f *SimpleWalkableFileSystem) Walk(ctx context.Context, parts []string) ([]os.FileInfo, error) {
	nodes, err := WalkTrail(f.SimpleFileSystem.Root, parts)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, len(nodes))
	for i, n := range nodes {
		inf, err := n.Info()
		if err != nil {
			return nil, err
		}
		infos[i] = inf
	}
	return infos, nil
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

func MakeNodeSliceErrorIterator(err error) iter.Seq2[Node, error] {
	return func(yield func(Node, error) bool) {
		yield(nil, err)
	}
}

func MakeNodeSliceIterator(fi []Node) iter.Seq2[Node, error] {
	return func(yield func(Node, error) bool) {
		for _, n := range fi {
			if !yield(n, nil) {
				return
			}
		}
	}
}

/////////////////////////////////////////////

// Creates a static directory that can't be modified
type StaticReadOnlyDir struct {
	SimpleFileInfo
	Children []Node
}

func (d *StaticReadOnlyDir) Info() (os.FileInfo, error)     { return d, nil }
func (d *StaticReadOnlyDir) WriteInfo(in os.FileInfo) error { return ErrUnsupported }
func (d *StaticReadOnlyDir) Delete(name string) error       { return ErrUnsupported }
func (d *StaticReadOnlyDir) List() iter.Seq2[Node, error] {
	return MakeNodeSliceIterator(d.Children)
}
func (d *StaticReadOnlyDir) CreateFile(name string, flag OpenMode, mode Mode) (FileHandle, error) {
	return nil, ErrUnsupported
}
func (d *StaticReadOnlyDir) CreateDir(name string, mode Mode) error {
	return ErrUnsupported
}

func StaticDir(name string, children ...Node) *StaticReadOnlyDir {
	return &StaticReadOnlyDir{
		SimpleFileInfo: SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0755,
		},
		Children: children,
	}
}

func StaticRootDir(children ...Node) *StaticReadOnlyDir {
	return &StaticReadOnlyDir{
		SimpleFileInfo: SimpleFileInfo{
			FIName: "",
			FIMode: os.ModeDir | 0755,
		},
		Children: children,
	}
}

/////////////////////////////////////////////

// Creates a dynamic, readonly directory that can't be modified, uses iterator instead of a slisce
type DynamicReadOnlyDirItr struct {
	SimpleFileInfo
	GetChildren func() iter.Seq2[Node, error]
}

func (d *DynamicReadOnlyDirItr) Info() (os.FileInfo, error)     { return d, nil }
func (d *DynamicReadOnlyDirItr) WriteInfo(in os.FileInfo) error { return ErrUnsupported }
func (d *DynamicReadOnlyDirItr) Delete(name string) error       { return ErrUnsupported }
func (d *DynamicReadOnlyDirItr) List() iter.Seq2[Node, error] {
	return d.GetChildren()
}
func (d *DynamicReadOnlyDirItr) CreateFile(name string, flag OpenMode, mode Mode) (FileHandle, error) {
	return nil, ErrUnsupported
}
func (d *DynamicReadOnlyDirItr) CreateDir(name string, mode Mode) error {
	return ErrUnsupported
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
func (d *DynamicReadOnlyDir) List() iter.Seq2[Node, error] {
	nodes, err := d.GetChildren()
	if err != nil {
		return MakeNodeSliceErrorIterator(err)
	}
	return MakeNodeSliceIterator(nodes)
}
func (d *DynamicReadOnlyDir) CreateFile(name string, flag OpenMode, mode Mode) (FileHandle, error) {
	return nil, ErrUnsupported
}
func (d *DynamicReadOnlyDir) CreateDir(name string, mode Mode) error {
	return ErrUnsupported
}

func DynamicRootDir(getChildren func() ([]Node, error)) *DynamicReadOnlyDir {
	return &DynamicReadOnlyDir{
		SimpleFileInfo: SimpleFileInfo{
			FIName: "",
			FIMode: os.ModeDir | 0777,
		},
		GetChildren: getChildren,
	}
}

/////////////////////////////////////////////

// Creates a dynamic, readonly directory that can't be modified. Allows arbitrary nested paths.
// Unlike normal directories, displays children in nested subdirectories as "top-level" children.
//
// Not particularly efficient, but sure is easy!
type DynamicReadOnlyDirTree struct {
	SimpleFileInfo
	GetFlatTree func() ([]Node, error)
}

func (d *DynamicReadOnlyDirTree) Info() (os.FileInfo, error)     { return d, nil }
func (d *DynamicReadOnlyDirTree) WriteInfo(in os.FileInfo) error { return ErrUnsupported }
func (d *DynamicReadOnlyDirTree) Delete(name string) error       { return ErrUnsupported }
func (d *DynamicReadOnlyDirTree) List() iter.Seq2[Node, error] {
	nodes, err := d.GetFlatTree()
	if err != nil {
		return MakeNodeSliceErrorIterator(err)
	}
	return MakeNodeSliceIterator(nodes)
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

	// NOTE: if the input path is more specific than one we know about, we'll
	// have to query the closest possible match (if it's a dir) to manually
	// recurse.
	if len(res) == 0 {
		var bestMatch Node
		var bestMatchName string
		for _, n := range nodes {
			info, err := n.Info()
			if err != nil {
				return nil, err
			}
			name := info.Name()
			// /foo/bar  /foo/bar/baz
			if IsSubpath(path, name) && len(bestMatchName) < len(name) {
				bestMatch = n
				bestMatchName = name
			}
		}
		n, _, err := WalkNode(bestMatch, path[len(bestMatchName):], true)
		return n, err
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
		FileInfo: &SimpleFileInfo{
			FIName:    name,
			FIMode:    mode,
			FIModTime: modTime,
		},
		OpenFn: func(m OpenMode) (FileHandle, error) {
			return &ReadOnlyMemoryFileHandle{b}, nil
		},
	}
}

func DynamicReadOnlyFile(name string, mode os.FileMode, modTime time.Time, open func() ([]byte, error)) *SimpleFile {
	return &SimpleFile{
		FileInfo: &SimpleFileInfo{
			FIName:    name,
			FIMode:    mode,
			FIModTime: modTime,
		},
		OpenFn: func(m OpenMode) (FileHandle, error) {
			if !m.IsReadable() {
				return nil, ErrWriteNotAllowed
			}
			b, err := open()
			return &ReadOnlyMemoryFileHandle{b}, err
		},
	}
}

func CtlFile(name string, mode os.FileMode, modTime time.Time, thread func(m OpenMode, r io.Reader, w io.Writer)) *SimpleFile {
	return &SimpleFile{
		FileInfo: &SimpleFileInfo{
			FIName:    name,
			FIMode:    mode,
			FIModTime: modTime,
		},
		OpenFn: func(m OpenMode) (FileHandle, error) {
			r1, w1 := io.Pipe()
			r2, w2 := io.Pipe()
			go func() {
				thread(m, r1, w2)
				r1.Close()
				w2.Close()
			}()
			return &RWFileHandle{R: r2, W: w1}, nil
		},
	}
}

type LineReader struct {
	R      io.Reader
	buf    []byte
	offset int
	length int
}

func (r *LineReader) Read(p []byte) (int, error) {
	n := 0
	if r.buf != nil {
		n += copy(p, r.buf[r.offset:r.length+r.offset])
		r.buf = nil
	}
	if len(p[n:]) == 0 {
		return n, nil
	}

	np, err := r.R.Read(p[n:])
	return n + np, err
}

func (r *LineReader) ReadLine() (string, error) {
	if r.buf == nil {
		r.buf = make([]byte, 4096)
	}
	if r.offset < 0 {
		r.offset = 0
	}
	var line string

	for i, ch := range r.buf[r.offset : r.offset+r.length] {
		if ch == '\n' || ch == 0 {
			line = string(r.buf[:i])
			r.buf = append(r.buf[:0], r.buf[i+1:]...)
			r.offset -= len(line)
			r.length -= len(line)
			return line, nil
		}
	}
	r.offset = r.length

	for {
		n, err := r.R.Read(r.buf[r.offset:])
		r.length += n
		for i, ch := range r.buf[r.offset : r.offset+n] {
			if ch == '\n' || ch == 0 {
				line = string(r.buf[:i])
				r.buf = append(r.buf[:0], r.buf[i+1:]...)
				r.offset -= len(line)
				r.length -= len(line)
				break
			}
			r.offset++
		}

		if err != nil {
			return line, err
		}
		if n == 0 && len(r.buf[r.offset:]) == 0 {
			// uh oh. too large
			line = string(r.buf)
			r.buf = r.buf[:0]
			r.offset -= len(line)
			r.length -= len(line)
			return line, io.ErrShortBuffer
		}

		return line, nil
	}
}

/////////////////////////////////////////////////////////////////////////////

func FindChild(root Node, name string) (Node, os.FileInfo, error) {
	if name == "." {
		info, err := root.Info()
		return root, info, err
	} else if name != "" {
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

		for node, err := range rootDir.List() {
			if node != nil {
				info, infoErr = node.Info()
				if infoErr != nil {
					itr.Close()
					return nil, nil, infoErr
				}

				if info.Name() == name {
					foundNode = node
					err = nil
					break
				}
			}

			if err != nil {
				return nil, info, err
			}
		}
		return foundNode, info, err
	} else {
		return nil, nil, os.ErrNotExist
	}
}

// WalkNode a given node using the path as guidance. Returns the filename reached.
func WalkNode(root Node, path string, walkLast bool) (Node, string, error) {
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
			var nodes []Node
			var p []string
			if walkLast {
				p = parts[i:]
			} else {
				p = dirParts[i:]
			}
			nodes, err = walkNode.Walk(p)
			if len(nodes) == 0 {
				err = fmt.Errorf("Failed to Walk %#v with %#v", currNode, p)
				return nil, lastPart, err
			}
			node = nodes[len(nodes)-1]
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
			var nodes []Node
			nodes, err = walkNode.Walk(parts[len(parts)-1:])
			if s := len(nodes); s > 0 {
				node = nodes[s-1]
			}

		} else {
			node, _, err = FindChild(currNode, lastPart)
		}
		if err != nil {
			return nil, lastPart, err
		}
		currNode = node
	}

	if currNode == nil {
		return nil, lastPart, os.ErrNotExist
	}

	return currNode, lastPart, nil
}

// Like Walk, but expects []Node for each node traversed
func WalkTrail(root Node, path []string) ([]Node, error) {
	currNode := root
	parts := path
	if len(parts) == 0 {
		return []Node{currNode}, nil
	}
	history := make([]Node, 0, len(parts))
	// history = append(history, currNode)

	last := len(parts) - 1
	for i, part := range parts {
		var (
			node Node
			info os.FileInfo
			err  error
		)
		if walkNode, ok := currNode.(WalkDir); ok {
			nodes, err := walkNode.Walk(parts[i:])
			if err != nil {
				return nil, err
			}
			history = append(history, nodes...)
			return history, err
		} else {
			node, info, err = FindChild(currNode, part)
		}
		if err != nil {
			return nil, err
		}

		if last != i && !info.IsDir() {
			return nil, os.ErrNotExist
		}

		currNode = node
		history = append(history, currNode)
	}

	return history, nil
}

// Like WalkTrail, but used for a node that doesn't support Walk, but wants to allow children to utilize Walk
func WalkPassthrough(root Node, path []string) ([]Node, error) {
	currNode := root
	parts := path
	if len(parts) == 0 {
		return []Node{currNode}, nil
	}
	history := make([]Node, 0, len(parts))
	node, info, err := FindChild(currNode, parts[0])
	if err != nil {
		return nil, err
	}
	if !info.IsDir() && len(parts) > 1 {
		return nil, os.ErrNotExist
	}

	history = append(history, node)
	if len(parts) > 1 {
		moreNodes, err := WalkTrail(node, parts[1:])
		if err != nil {
			return history, err
		}
		history = append(history, moreNodes...)
	}
	return history, nil
}

func MakeDirAll(ctx context.Context, fs FileSystem, path string, mode Mode) error {
	parts := PathSplit(path)
	for i := range parts {
		subpath := strings.Join(parts[:i], "/")
		if err := fs.MakeDir(ctx, subpath, mode); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
	}
	if err := fs.MakeDir(ctx, path, mode); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return nil
}
