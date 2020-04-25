package proxy

import (
	"fmt"
	"io"
	"strings"

	"github.com/jeffh/cfs/ninep"
)

// TODO: it would be nice to make FileSystemMount conform to FileSystem
type FileSystemMount struct {
	FS     ninep.TraversableFileSystem // required
	Prefix string                      // required
	Client ninep.Client                // optional
	Clean  func() error                // optional
}

func (fsm *FileSystemMount) Close() error {
	if fsm.Clean != nil {
		err := fsm.Clean()
		if err != nil {
			fsm.Client.Close()
			return err
		}
	}
	if fsm.Client != nil {
		return fsm.Client.Close()
	}
	return nil
}

type FileSystemMountConfig struct {
	Addr   string
	Prefix string
}

func ParseMount(arg string) (FileSystemMountConfig, bool) {
	parts := strings.SplitN(arg, "/", 2)
	count := len(parts)
	if count == 1 {
		return FileSystemMountConfig{Addr: parts[0]}, true
	} else if count >= 2 {
		return FileSystemMountConfig{
			Addr:   parts[0],
			Prefix: parts[1],
		}, true
	}
	return FileSystemMountConfig{}, false
}

func ParseMounts(args []string) []FileSystemMountConfig {
	fsmc := make([]FileSystemMountConfig, 0, len(args))
	for _, arg := range args {
		m, ok := ParseMount(arg)
		if ok {
			fsmc = append(fsmc, m)
		}
	}
	return fsmc
}

func PrintMountsHelp(w io.Writer) {
	fmt.Fprintf(w, "\nMounts refers to a 9p server and path and are represented like <SERVER>:<PORT><PATH> like 'localhost:6666/prefix/path'.\n")
	fmt.Fprintf(w, "\nThere are a few special mount values that are recognized:\n")
	fmt.Fprintf(w, "  - ':memory' indicates an in memory file system that gets discarded after the program exits.\n")
	fmt.Fprintf(w, "  - ':tmp' indicates an on-disk temporarily directory that gets discarded after the program exits.\n")
	fmt.Fprintf(w, "  -  starting with a '/' or '.' indicates an on-disk local path.\n")
}
