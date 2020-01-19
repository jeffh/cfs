package proxy

import (
	"strings"

	"github.com/jeffh/cfs/ninep"
)

type FileSystemMount struct {
	FS     ninep.FileSystem
	Prefix string
}

type FileSystemMountConfig struct {
	Addr   string
	Prefix string
}

func ParseMounts(args []string) []FileSystemMountConfig {
	fsmc := make([]FileSystemMountConfig, 0, len(args))
	for _, arg := range args {
		parts := strings.Split(arg, "/")
		count := len(parts)
		if count == 1 {
			fsmc = append(fsmc, FileSystemMountConfig{Addr: parts[0]})
		} else if count >= 2 {
			fsmc = append(fsmc, FileSystemMountConfig{
				Addr:   parts[0],
				Prefix: parts[1],
			})
		}
	}
	return fsmc
}
