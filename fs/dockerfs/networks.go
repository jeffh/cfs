package dockerfs

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/docker/docker/api/types/network"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

func networkFileContents(fileType string, inspect network.Inspect) (string, error) {
	var content string
	switch fileType {
	case "networkName":
		content = inspect.Name
	case "networkLabels":
		var b strings.Builder
		keys := slices.Collect(maps.Keys(inspect.Labels))
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%s\n", kvp.KeyPair(k, inspect.Labels[k]))
		}
		content = b.String()
	case "networkDriver":
		content = inspect.Driver
	case "networkIPv6":
		content = fmt.Sprintf("%v", inspect.EnableIPv6)
	case "networkJSON":
		b, err := json.MarshalIndent(inspect, "", "  ")
		if err != nil {
			return "", err
		}
		content = string(b)
	}
	return content, nil
}

func handleNetworkFile(f *Fs, fileType string, networkID string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	inspect, err := f.C.NetworkInspect(context.Background(), networkID, network.InspectOptions{})
	if err != nil {
		return nil, err
	}

	content, err := networkFileContents(fileType, inspect)
	if err != nil {
		return nil, err
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(content)}, nil
}
