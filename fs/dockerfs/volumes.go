package dockerfs

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/docker/docker/api/types/volume"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

func volumeFileContents(fileType string, inspect volume.Volume) (string, error) {
	var content string
	switch fileType {
	case "volumeScope":
		content = inspect.Scope
	case "volumeDriver":
		content = inspect.Driver
	case "volumeLabels":
		var b strings.Builder
		keys := slices.Collect(maps.Keys(inspect.Labels))
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%s\n", kvp.KeyPair(k, inspect.Labels[k]))
		}
		content = b.String()
	case "volumeMountpoint":
		content = inspect.Mountpoint
	}
	return content, nil
}

func handleVolumeFile(f *Fs, fileType string, volumeName string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	inspect, err := f.C.VolumeInspect(context.Background(), volumeName)
	if err != nil {
		return nil, err
	}

	content, err := volumeFileContents(fileType, inspect)
	if err != nil {
		return nil, err
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(content)}, nil
}
