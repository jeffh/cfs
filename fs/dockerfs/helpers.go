package dockerfs

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jeffh/cfs/ninep"
)

func staticDir(name string, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		Children: children,
	}
}

func staticDirWithTime(name string, modTime time.Time, children ...ninep.Node) *ninep.StaticReadOnlyDir {
	return &ninep.StaticReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    os.ModeDir | 0777,
			FIModTime: modTime,
		},
		Children: children,
	}
}

func dynamicFile(name string, modTime time.Time, content func() ([]byte, error)) *ninep.SimpleFile {
	return &ninep.SimpleFile{
		FileInfo: &ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    0777,
			FIModTime: modTime,
		},
		OpenFn: func() (ninep.FileHandle, error) {
			b, err := content()
			if err != nil {
				return nil, err
			}
			return &ninep.ReadOnlyMemoryFileHandle{b}, nil
		},
	}
}

func dynamicDir(name string, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDir {
	return &ninep.DynamicReadOnlyDir{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		GetChildren: resolve,
	}
}

func dynamicDirWithTime(name string, modTime time.Time, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDir {
	n := dynamicDir(name, resolve)
	n.FIModTime = modTime
	return n
}

func dynamicDirTree(name string, resolve func() ([]ninep.Node, error)) *ninep.DynamicReadOnlyDirTree {
	return &ninep.DynamicReadOnlyDirTree{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName: name,
			FIMode: os.ModeDir | 0777,
		},
		GetFlatTree: resolve,
	}
}

func staticStringFile(name string, modTime time.Time, contents string) *ninep.SimpleFile {
	return ninep.StaticReadOnlyFile(name, 0444, modTime, []byte(contents))
}

func dynamicCtlFile(name string, thread func(r io.Reader, w io.Writer)) *ninep.SimpleFile {
	return ninep.CtlFile(name, 0777, time.Time{}, thread)
}

func strBool(s string) bool {
	return s == "true" || s == "t" || s == "yes" || s == "y"
}

func boolStr(v bool) string {
	if v {
		return "true"
	} else {
		return "false"
	}
}
func intStr(v int) string { return fmt.Sprintf("%d", v) }
