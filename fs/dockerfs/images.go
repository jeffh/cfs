package dockerfs

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

func contentsImageFile(fileType string, inspect types.ImageInspect) (string, error) {
	var content string
	switch fileType {
	case "imageRepoTags":
		content = strings.Join(inspect.RepoTags, "\n")
	case "imageParent":
		content = inspect.Parent
	case "imageComment":
		content = inspect.Comment
	case "imageDockerVersion":
		content = inspect.DockerVersion
	case "imageAuthor":
		content = inspect.Author
	case "imageArch":
		content = inspect.Architecture
	case "imageVariant":
		content = inspect.Variant
	case "imageOS":
		content = inspect.Os
	case "imageSize":
		content = fmt.Sprintf("%d", inspect.Size)
	case "imageMetadata":
		var b strings.Builder
		if !inspect.Metadata.LastTagTime.IsZero() {
			fmt.Fprintf(&b, "%s\n", kvp.KeyPair("LastTagTime", inspect.Metadata.LastTagTime.Format(time.RFC3339Nano)))
		}
		content = b.String()
	case "imageID":
		content = inspect.ID
	case "imageLabels":
		var b strings.Builder
		keys := slices.Collect(maps.Keys(inspect.Config.Labels))
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%s\n", kvp.KeyPair(k, inspect.Config.Labels[k]))
		}
		content = b.String()
	case "imageJSON":
		b, err := json.MarshalIndent(inspect, "", "  ")
		if err != nil {
			return "", err
		}
		content = string(b)
	case "imageEnv":
		var b strings.Builder
		for _, env := range inspect.Config.Env {
			// Environment variables are already in KEY=value format
			fmt.Fprintf(&b, "%s\n", env)
		}
		content = b.String()
	}
	return content, nil
}

func handleImageFile(f *Fs, fileType string, imageID string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	inspect, _, err := f.C.ImageInspectWithRaw(context.Background(), imageID)
	if err != nil {
		return nil, err
	}

	content, err := contentsImageFile(fileType, inspect)
	if err != nil {
		return nil, err
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(content)}, nil
}
