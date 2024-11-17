package dockerfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/events"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/volume"
	"github.com/jeffh/cfs/ninep"
)

type controlFile struct {
	Open func(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error)
	Stat func(modTime time.Time) fs.FileInfo
}

// Helper functions for stats
func staticFileStat(name string) func(time.Time) fs.FileInfo {
	return func(modTime time.Time) fs.FileInfo {
		return &ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    ninep.Readable,
			FIModTime: modTime,
		}
	}
}

func dirStat(name string) func(time.Time) fs.FileInfo {
	return func(modTime time.Time) fs.FileInfo {
		return &ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
			FIModTime: modTime,
		}
	}
}

// Helper to sort map keys
func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Control file implementations
func imageHelpOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	const helpText = `Files in this directory:
help   - that's this file!
ctl    - perform most docker operations on images
build  - build docker images from a tar context
load   - load docker images from a tar
export - export docker images to a tar
ids    - list images by their IDs
`
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(helpText)}, nil
}

func imageCtlOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go imagesCtl(f.C)(flag, r, w)
	return NewPipeHandle(r, w), nil
}

func imageLoadOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go imageLoadCtl(f.C)(flag, r, w)
	return NewPipeHandle(r, w), nil
}

func imageBuildOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go imageBuildCtl(f.C)(flag, r, w)
	return NewPipeHandle(r, w), nil
}

func imageExportOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go imageExportCtl(f.C)(flag, r, w)
	return NewPipeHandle(r, w), nil
}

func imageIdsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	imgs, err := f.C.ImageList(context.Background(), image.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, img := range imgs {
		// Add both full ID and short ID
		fmt.Fprintf(&b, "%s\n", img.ID)
		if len(img.ID) > 12 {
			fmt.Fprintf(&b, "%s\n", img.ID[:12])
		}
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func imageTagsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	imgs, err := f.C.ImageList(context.Background(), image.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, img := range imgs {
		for _, tag := range img.RepoTags {
			fmt.Fprintf(&b, "%s\n", tag)
		}
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func imageDigestsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	imgs, err := f.C.ImageList(context.Background(), image.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, img := range imgs {
		for _, digest := range img.RepoDigests {
			fmt.Fprintf(&b, "%s\n", digest)
		}
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func imageLabelsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	imgs, err := f.C.ImageList(context.Background(), image.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, img := range imgs {
		for k, v := range img.Labels {
			fmt.Fprintf(&b, "%s=%s\n", k, v)
		}
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

// Container control implementations
func containerHelpOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	const helpText = `Files in this directory:
help  - that's this file!
ctl   - perform docker operations on containers
ids   - list containers by their IDs
names - list containers by their names
`
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(helpText)}, nil
}

func containerCtlOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go containersCtl(f.C, container.ListOptions{})(flag, r, w)
	return NewPipeHandle(r, w), nil
}

func containerIdsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	containers, err := f.C.ContainerList(context.Background(), container.ListOptions{All: true})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, c := range containers {
		b.WriteString(c.ID)
		b.WriteString("\n")
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func containerNamesOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	containers, err := f.C.ContainerList(context.Background(), container.ListOptions{All: true})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, c := range containers {
		for _, name := range c.Names {
			// Remove leading slash from container names
			b.WriteString(strings.TrimPrefix(name, "/"))
			b.WriteString("\n")
		}
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func containerStateOpen(f *Fs, state string) func(ninep.OpenMode) (ninep.FileHandle, error) {
	return func(flag ninep.OpenMode) (ninep.FileHandle, error) {
		if !flag.IsReadable() {
			return nil, ninep.ErrWriteNotAllowed
		}

		opts := container.ListOptions{
			All:     true,
			Filters: filters.NewArgs(filters.Arg("status", state)),
		}

		containers, err := f.C.ContainerList(context.Background(), opts)
		if err != nil {
			return nil, err
		}

		var b strings.Builder
		for _, c := range containers {
			fmt.Fprintf(&b, "%s\n", c.ID)
			for _, name := range c.Names {
				fmt.Fprintf(&b, "%s\n", strings.TrimPrefix(name, "/"))
			}
		}

		return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
	}
}

func containerLogsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	r, w := io.Pipe()
	go func() {
		defer w.Close()

		lr := &ninep.LineReader{R: r}
		for {
			cmd, err := lr.ReadLine()
			if err != nil {
				if err != io.EOF {
					w.CloseWithError(err)
				}
				return
			}

			args := strings.Fields(cmd)
			if len(args) == 0 {
				continue
			}

			containerID := args[0]
			options := container.LogsOptions{
				ShowStdout: true,
				ShowStderr: true,
				Follow:     true,
				Timestamps: true,
			}

			// Parse optional flags
			for i := 1; i < len(args); i++ {
				switch args[i] {
				case "--tail":
					if i+1 < len(args) {
						options.Tail = args[i+1]
						i++
					}
				case "--since":
					if i+1 < len(args) {
						options.Since = args[i+1]
						i++
					}
				case "--until":
					if i+1 < len(args) {
						options.Until = args[i+1]
						i++
					}
				case "--no-stdout":
					options.ShowStdout = false
				case "--no-stderr":
					options.ShowStderr = false
				case "--no-timestamps":
					options.Timestamps = false
				case "--no-follow":
					options.Follow = false
				case "help":
					fmt.Fprintf(w, "Usage: CONTAINER_ID [OPTIONS]\n\n")
					fmt.Fprintf(w, "Options:\n")
					fmt.Fprintf(w, "  --tail N          Number of lines to show from the end of the logs\n")
					fmt.Fprintf(w, "  --since TIME      Show logs since timestamp\n")
					fmt.Fprintf(w, "  --until TIME      Show logs before timestamp\n")
					fmt.Fprintf(w, "  --no-stdout      Don't show stdout\n")
					fmt.Fprintf(w, "  --no-stderr      Don't show stderr\n")
					fmt.Fprintf(w, "  --no-timestamps  Don't show timestamps\n")
					fmt.Fprintf(w, "  --no-follow      Don't follow log output\n")
					return
				}
			}

			logs, err := f.C.ContainerLogs(context.Background(), containerID, options)
			if err != nil {
				fmt.Fprintf(w, "error: %v\n", err)
				continue
			}

			_, err = io.Copy(w, logs)
			if err != nil {
				fmt.Fprintf(w, "error: %v\n", err)
			}
			logs.Close()
		}
	}()

	return NewPipeHandle(r, w), nil
}

func containerStdoutOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	r, w := io.Pipe()
	go func() {
		defer w.Close()

		lr := &ninep.LineReader{R: r}
		for {
			cmd, err := lr.ReadLine()
			if err != nil {
				if err != io.EOF {
					w.CloseWithError(err)
				}
				return
			}

			args := strings.Fields(cmd)
			if len(args) == 0 {
				continue
			}

			containerID := args[0]
			options := container.LogsOptions{
				ShowStdout: true,
				ShowStderr: false,
				Follow:     true,
				Timestamps: true,
			}

			// Parse optional flags similar to containerLogsOpen
			for i := 1; i < len(args); i++ {
				switch args[i] {
				case "--tail":
					if i+1 < len(args) {
						options.Tail = args[i+1]
						i++
					}
				case "--since":
					if i+1 < len(args) {
						options.Since = args[i+1]
						i++
					}
				case "--until":
					if i+1 < len(args) {
						options.Until = args[i+1]
						i++
					}
				case "--no-timestamps":
					options.Timestamps = false
				case "--no-follow":
					options.Follow = false
				}
			}

			logs, err := f.C.ContainerLogs(context.Background(), containerID, options)
			if err != nil {
				fmt.Fprintf(w, "error: %v\n", err)
				continue
			}

			_, err = io.Copy(w, logs)
			if err != nil {
				fmt.Fprintf(w, "error: %v\n", err)
			}
			logs.Close()
		}
	}()

	return NewPipeHandle(r, w), nil
}

func containerStderrOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	r, w := io.Pipe()
	go func() {
		defer w.Close()

		lr := &ninep.LineReader{R: r}
		for {
			cmd, err := lr.ReadLine()
			if err != nil {
				if err != io.EOF {
					w.CloseWithError(err)
				}
				return
			}

			args := strings.Fields(cmd)
			if len(args) == 0 {
				continue
			}

			containerID := args[0]
			options := container.LogsOptions{
				ShowStdout: false,
				ShowStderr: true,
				Follow:     true,
				Timestamps: true,
			}

			// Parse optional flags similar to containerLogsOpen
			for i := 1; i < len(args); i++ {
				switch args[i] {
				case "--tail":
					if i+1 < len(args) {
						options.Tail = args[i+1]
						i++
					}
				case "--since":
					if i+1 < len(args) {
						options.Since = args[i+1]
						i++
					}
				case "--until":
					if i+1 < len(args) {
						options.Until = args[i+1]
						i++
					}
				case "--no-timestamps":
					options.Timestamps = false
				case "--no-follow":
					options.Follow = false
				}
			}

			logs, err := f.C.ContainerLogs(context.Background(), containerID, options)
			if err != nil {
				fmt.Fprintf(w, "error: %v\n", err)
				continue
			}

			_, err = io.Copy(w, logs)
			if err != nil {
				fmt.Fprintf(w, "error: %v\n", err)
			}
			logs.Close()
		}
	}()

	return NewPipeHandle(r, w), nil
}

// System control implementations
func systemInfoOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	info, err := f.C.Info(context.Background())
	if err != nil {
		return nil, err
	}

	var b strings.Builder

	// Write basic info
	fmt.Fprintf(&b, "ID: %s\n", info.ID)
	fmt.Fprintf(&b, "Containers: %d\n", info.Containers)
	fmt.Fprintf(&b, "Running: %d\n", info.ContainersRunning)
	fmt.Fprintf(&b, "Paused: %d\n", info.ContainersPaused)
	fmt.Fprintf(&b, "Stopped: %d\n", info.ContainersStopped)
	fmt.Fprintf(&b, "Images: %d\n", info.Images)

	// System info
	fmt.Fprintf(&b, "\nSystem:\n")
	fmt.Fprintf(&b, "OS: %s\n", info.OperatingSystem)
	fmt.Fprintf(&b, "Architecture: %s\n", info.Architecture)
	fmt.Fprintf(&b, "Kernel Version: %s\n", info.KernelVersion)
	fmt.Fprintf(&b, "Memory: %d bytes\n", info.MemTotal)
	fmt.Fprintf(&b, "CPUs: %d\n", info.NCPU)

	// Docker info
	fmt.Fprintf(&b, "\nDocker:\n")
	fmt.Fprintf(&b, "Version: %s\n", info.ServerVersion)
	fmt.Fprintf(&b, "Root Dir: %s\n", info.DockerRootDir)
	fmt.Fprintf(&b, "Driver: %s\n", info.Driver)
	fmt.Fprintf(&b, "Logging Driver: %s\n", info.LoggingDriver)
	fmt.Fprintf(&b, "Cgroup Driver: %s\n", info.CgroupDriver)

	// Plugins
	fmt.Fprintf(&b, "\nPlugins:\n")
	fmt.Fprintf(&b, "Volume: %s\n", strings.Join(info.Plugins.Volume, ", "))
	fmt.Fprintf(&b, "Network: %s\n", strings.Join(info.Plugins.Network, ", "))
	fmt.Fprintf(&b, "Authorization: %s\n", strings.Join(info.Plugins.Authorization, ", "))

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func systemVersionOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	version, err := f.C.ServerVersion(context.Background())
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	fmt.Fprintf(&b, "Version: %s\n", version.Version)
	fmt.Fprintf(&b, "API Version: %s\n", version.APIVersion)
	fmt.Fprintf(&b, "Min API Version: %s\n", version.MinAPIVersion)
	fmt.Fprintf(&b, "Git Commit: %s\n", version.GitCommit)
	fmt.Fprintf(&b, "Go Version: %s\n", version.GoVersion)
	fmt.Fprintf(&b, "OS/Arch: %s/%s\n", version.Os, version.Arch)
	fmt.Fprintf(&b, "Experimental: %v\n", version.Experimental)
	fmt.Fprintf(&b, "Build Time: %s\n", version.BuildTime)

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func systemEventsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		msgs, errs := f.C.Events(context.Background(), events.ListOptions{})
		for {
			select {
			case msg := <-msgs:
				data, err := json.Marshal(msg)
				if err != nil {
					w.CloseWithError(err)
					return
				}
				fmt.Fprintf(w, "%s\n", data)
			case err := <-errs:
				if err != nil {
					w.CloseWithError(err)
					return
				}
			}
		}
	}()

	return NewPipeHandle(r, w), nil
}

// Define the control maps
var imageControls = map[string]controlFile{
	"help":    {Open: imageHelpOpen, Stat: staticFileStat("help")},
	"ctl":     {Open: imageCtlOpen, Stat: staticFileStat("ctl")},
	"load":    {Open: imageLoadOpen, Stat: staticFileStat("load")},
	"build":   {Open: imageBuildOpen, Stat: staticFileStat("build")},
	"export":  {Open: imageExportOpen, Stat: staticFileStat("export")},
	"ids":     {Open: imageIdsOpen, Stat: dirStat("ids")},
	"tags":    {Open: imageTagsOpen, Stat: dirStat("tags")},
	"digests": {Open: imageDigestsOpen, Stat: dirStat("digests")},
	"labels":  {Open: imageLabelsOpen, Stat: dirStat("labels")},
}

var containerControls = map[string]controlFile{
	"help":   {Open: containerHelpOpen, Stat: staticFileStat("help")},
	"ctl":    {Open: containerCtlOpen, Stat: staticFileStat("ctl")},
	"ids":    {Open: containerIdsOpen, Stat: dirStat("ids")},
	"names":  {Open: containerNamesOpen, Stat: dirStat("names")},
	"logs":   {Open: containerLogsOpen, Stat: staticFileStat("logs")},
	"stdout": {Open: containerStdoutOpen, Stat: staticFileStat("stdout")},
	"stderr": {Open: containerStderrOpen, Stat: staticFileStat("stderr")},
}

var systemControls = map[string]controlFile{
	"info":    {Open: systemInfoOpen, Stat: staticFileStat("info")},
	"version": {Open: systemVersionOpen, Stat: staticFileStat("version")},
	"events":  {Open: systemEventsOpen, Stat: staticFileStat("events")},
}

// Initialize sorted keys
var sortedImageKeys = sortedKeys(imageControls)
var sortedContainerKeys = sortedKeys(containerControls)
var sortedSystemKeys = sortedKeys(systemControls)

// Add volume control implementations
func volumeHelpOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	const helpText = `Files in this directory:
help   - that's this file!
ctl    - perform docker operations on volumes
list   - list all volumes
names  - list volume names only
labels - list volumes by labels
`
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(helpText)}, nil
}

func volumeCtlOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		lr := &ninep.LineReader{R: r}
		for {
			cmd, err := lr.ReadLine()
			if err != nil {
				if err != io.EOF {
					w.CloseWithError(err)
				}
				return
			}

			args := strings.Fields(cmd)
			if len(args) == 0 {
				continue
			}

			switch args[0] {
			case "help":
				fmt.Fprintf(w, "COMMANDS:\n\n")
				fmt.Fprintf(w, " create NAME [OPTS]  - Create a new volume\n")
				fmt.Fprintf(w, " remove NAME        - Remove a volume\n")
				fmt.Fprintf(w, " prune             - Remove all unused volumes\n")
				fmt.Fprintf(w, " inspect NAME      - Show detailed volume info\n")
				fmt.Fprintf(w, " exit              - Close this control file\n")
				fmt.Fprintf(w, " help              - Show this help\n")
			case "create":
				if len(args) < 2 {
					fmt.Fprintf(w, "error: missing volume name\n")
					continue
				}
				vol, err := f.C.VolumeCreate(context.Background(), volume.CreateOptions{
					Name: args[1],
				})
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				fmt.Fprintf(w, "created: %s\n", vol.Name)
			case "remove":
				if len(args) < 2 {
					fmt.Fprintf(w, "error: missing volume name\n")
					continue
				}
				err := f.C.VolumeRemove(context.Background(), args[1], true)
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				fmt.Fprintf(w, "removed: %s\n", args[1])
			case "prune":
				report, err := f.C.VolumesPrune(context.Background(), filters.NewArgs())
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				fmt.Fprintf(w, "space reclaimed: %d bytes\n", report.SpaceReclaimed)
				for _, vol := range report.VolumesDeleted {
					fmt.Fprintf(w, "deleted: %s\n", vol)
				}
			case "inspect":
				if len(args) < 2 {
					fmt.Fprintf(w, "error: missing volume name\n")
					continue
				}
				vol, err := f.C.VolumeInspect(context.Background(), args[1])
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				data, _ := json.MarshalIndent(vol, "", "  ")
				fmt.Fprintf(w, "%s\n", data)
			case "exit", "quit":
				return
			default:
				fmt.Fprintf(w, "error: unknown command: %s\n", args[0])
			}
		}
	}()
	return NewPipeHandle(r, w), nil
}

func volumeListOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	vols, err := f.C.VolumeList(context.Background(), volume.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, vol := range vols.Volumes {
		fmt.Fprintf(&b, "Name: %s\n", vol.Name)
		fmt.Fprintf(&b, "Driver: %s\n", vol.Driver)
		fmt.Fprintf(&b, "Mountpoint: %s\n", vol.Mountpoint)
		if len(vol.Labels) > 0 {
			fmt.Fprintf(&b, "Labels:\n")
			for k, v := range vol.Labels {
				fmt.Fprintf(&b, "  %s: %s\n", k, v)
			}
		}
		fmt.Fprintf(&b, "Scope: %s\n", vol.Scope)
		fmt.Fprintf(&b, "\n")
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func volumeNamesOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	vols, err := f.C.VolumeList(context.Background(), volume.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, vol := range vols.Volumes {
		fmt.Fprintf(&b, "%s\n", vol.Name)
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func volumeLabelsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	vols, err := f.C.VolumeList(context.Background(), volume.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, vol := range vols.Volumes {
		if len(vol.Labels) > 0 {
			fmt.Fprintf(&b, "Volume: %s\n", vol.Name)
			for k, v := range vol.Labels {
				fmt.Fprintf(&b, "  %s=%s\n", k, v)
			}
			fmt.Fprintf(&b, "\n")
		}
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

// Add volume controls map
var volumeControls = map[string]controlFile{
	"help":   {Open: volumeHelpOpen, Stat: staticFileStat("help")},
	"ctl":    {Open: volumeCtlOpen, Stat: staticFileStat("ctl")},
	"list":   {Open: volumeListOpen, Stat: staticFileStat("list")},
	"names":  {Open: volumeNamesOpen, Stat: staticFileStat("names")},
	"labels": {Open: volumeLabelsOpen, Stat: staticFileStat("labels")},
}

// Add sorted volume keys
var sortedVolumeKeys = sortedKeys(volumeControls)

// Add network control implementations
func networkHelpOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	const helpText = `Files in this directory:
help   - that's this file!
ctl    - perform docker operations on networks
list   - list all networks
names  - list network names only
ids    - list network IDs
labels - list networks by labels
`
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(helpText)}, nil
}

func networkCtlOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	r, w := io.Pipe()
	go func() {
		defer w.Close()

		lr := &ninep.LineReader{R: r}
		for {
			cmd, err := lr.ReadLine()
			if err != nil {
				if err != io.EOF {
					w.CloseWithError(err)
				}
				return
			}

			args := strings.Fields(cmd)
			if len(args) == 0 {
				continue
			}

			switch args[0] {
			case "help":
				fmt.Fprintf(w, "COMMANDS:\n\n")
				fmt.Fprintf(w, " create NAME [OPTS]  - Create a new network\n")
				fmt.Fprintf(w, " remove NAME        - Remove a network\n")
				fmt.Fprintf(w, " prune             - Remove all unused networks\n")
				fmt.Fprintf(w, " inspect NAME      - Show detailed network info\n")
				fmt.Fprintf(w, " connect NAME CONTAINER [OPTS] - Connect a container to a network\n")
				fmt.Fprintf(w, " disconnect NAME CONTAINER     - Disconnect a container from a network\n")
				fmt.Fprintf(w, " exit              - Close this control file\n")
				fmt.Fprintf(w, " help              - Show this help\n")
			case "create":
				if len(args) < 2 {
					fmt.Fprintf(w, "error: missing network name\n")
					continue
				}
				net, err := f.C.NetworkCreate(context.Background(), args[1], network.CreateOptions{})
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				fmt.Fprintf(w, "created: %s\n", net.ID)
			case "remove":
				if len(args) < 2 {
					fmt.Fprintf(w, "error: missing network name\n")
					continue
				}
				err := f.C.NetworkRemove(context.Background(), args[1])
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				fmt.Fprintf(w, "removed: %s\n", args[1])
			case "prune":
				report, err := f.C.NetworksPrune(context.Background(), filters.NewArgs())
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				for _, net := range report.NetworksDeleted {
					fmt.Fprintf(w, "deleted: %s\n", net)
				}
			case "inspect":
				if len(args) < 2 {
					fmt.Fprintf(w, "error: missing network name\n")
					continue
				}
				net, err := f.C.NetworkInspect(context.Background(), args[1], network.InspectOptions{})
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				data, _ := json.MarshalIndent(net, "", "  ")
				fmt.Fprintf(w, "%s\n", data)
			case "connect":
				if len(args) < 3 {
					fmt.Fprintf(w, "error: missing network name or container ID\n")
					continue
				}
				err := f.C.NetworkConnect(context.Background(), args[1], args[2], nil)
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				fmt.Fprintf(w, "connected container %s to network %s\n", args[2], args[1])
			case "disconnect":
				if len(args) < 3 {
					fmt.Fprintf(w, "error: missing network name or container ID\n")
					continue
				}
				err := f.C.NetworkDisconnect(context.Background(), args[1], args[2], true)
				if err != nil {
					fmt.Fprintf(w, "error: %v\n", err)
					continue
				}
				fmt.Fprintf(w, "disconnected container %s from network %s\n", args[2], args[1])
			case "exit", "quit":
				return
			default:
				fmt.Fprintf(w, "error: unknown command: %s\n", args[0])
			}
		}
	}()
	return NewPipeHandle(r, w), nil
}

func networkListOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	networks, err := f.C.NetworkList(context.Background(), network.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, net := range networks {
		fmt.Fprintf(&b, "Name: %s\n", net.Name)
		fmt.Fprintf(&b, "ID: %s\n", net.ID)
		fmt.Fprintf(&b, "Driver: %s\n", net.Driver)
		fmt.Fprintf(&b, "Scope: %s\n", net.Scope)
		if len(net.IPAM.Config) > 0 {
			fmt.Fprintf(&b, "Subnet: %s\n", net.IPAM.Config[0].Subnet)
			fmt.Fprintf(&b, "Gateway: %s\n", net.IPAM.Config[0].Gateway)
		}
		if len(net.Labels) > 0 {
			fmt.Fprintf(&b, "Labels:\n")
			for k, v := range net.Labels {
				fmt.Fprintf(&b, "  %s: %s\n", k, v)
			}
		}
		fmt.Fprintf(&b, "\n")
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func networkNamesOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	networks, err := f.C.NetworkList(context.Background(), network.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, net := range networks {
		fmt.Fprintf(&b, "%s\n", net.Name)
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func networkIdsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	networks, err := f.C.NetworkList(context.Background(), network.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, net := range networks {
		fmt.Fprintf(&b, "%s\n", net.ID)
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

func networkLabelsOpen(f *Fs, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	networks, err := f.C.NetworkList(context.Background(), network.ListOptions{})
	if err != nil {
		return nil, err
	}

	var b strings.Builder
	for _, net := range networks {
		if len(net.Labels) > 0 {
			fmt.Fprintf(&b, "Network: %s\n", net.Name)
			for k, v := range net.Labels {
				fmt.Fprintf(&b, "  %s=%s\n", k, v)
			}
			fmt.Fprintf(&b, "\n")
		}
	}

	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(b.String())}, nil
}

// Add network controls map
var networkControls = map[string]controlFile{
	"help":   {Open: networkHelpOpen, Stat: staticFileStat("help")},
	"ctl":    {Open: networkCtlOpen, Stat: staticFileStat("ctl")},
	"list":   {Open: networkListOpen, Stat: staticFileStat("list")},
	"names":  {Open: networkNamesOpen, Stat: staticFileStat("names")},
	"ids":    {Open: networkIdsOpen, Stat: staticFileStat("ids")},
	"labels": {Open: networkLabelsOpen, Stat: staticFileStat("labels")},
}

// Add sorted network keys
var sortedNetworkKeys = sortedKeys(networkControls)
