// Implements a 9p file system that talks to a docker daemon.
package dockerfs

import (
	"context"
	"io/fs"
	"iter"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/jeffh/cfs/ninep"
)

var mx = ninep.NewMux().
	Define().Path("/").As("root").
	Define().Path("/images").TrailSlash().As("images").
	Define().Path("/images/{id}").TrailSlash().As("image").
	Define().Path("/images/{id}/repo_tags").As("imageRepoTags").
	Define().Path("/images/{id}/parent").As("imageParent").
	Define().Path("/images/{id}/comment").As("imageComment").
	Define().Path("/images/{id}/docker_version").As("imageDockerVersion").
	Define().Path("/images/{id}/author").As("imageAuthor").
	Define().Path("/images/{id}/arch").As("imageArch").
	Define().Path("/images/{id}/variant").As("imageVariant").
	Define().Path("/images/{id}/os").As("imageOS").
	Define().Path("/images/{id}/size").As("imageSize").
	Define().Path("/images/{id}/metadata").As("imageMetadata").
	Define().Path("/images/{id}/image_id").As("imageID").
	Define().Path("/images/{id}/labels").As("imageLabels").
	Define().Path("/images/{id}/json").As("imageJSON").
	Define().Path("/images/{id}/env").As("imageEnv").
	Define().Path("/containers").TrailSlash().As("containers").
	Define().Path("/containers/{id}").TrailSlash().As("container").
	Define().Path("/containers/{id}/status").As("containerStatus").
	Define().Path("/containers/{id}/pid").As("containerPID").
	Define().Path("/containers/{id}/started_at").As("containerStartedAt").
	Define().Path("/containers/{id}/finished_at").As("containerFinishedAt").
	Define().Path("/containers/{id}/health").TrailSlash().As("containerHealth").
	Define().Path("/containers/{id}/health/status").As("containerHealthStatus").
	Define().Path("/containers/{id}/health/failing_streak").As("containerHealthFailingStreak").
	Define().Path("/containers/{id}/health/logs").As("containerHealthLogs").
	Define().Path("/containers/{id}/image").As("containerImage").
	Define().Path("/containers/{id}/name").As("containerName").
	Define().Path("/containers/{id}/restart_count").As("containerRestartCount").
	Define().Path("/containers/{id}/platform").As("containerPlatform").
	Define().Path("/containers/{id}/labels").As("containerLabels").
	Define().Path("/containers/{id}/json").As("containerJSON").
	Define().Path("/containers/{id}/env").As("containerEnv").
	Define().Path("/containers/{id}/ports").As("containerPorts").
	Define().Path("/containers/{id}/mounts").As("containerMounts")

type Fs struct {
	sync.Mutex
	C *client.Client
}

var _ ninep.FileSystem = (*Fs)(nil)

func NewFs() (*Fs, error) {
	c, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &Fs{C: c}, nil
}

func (f *Fs) Close() error {
	if f.C != nil {
		return f.C.Close()
	}
	return nil
}

func (f *Fs) MakeDir(ctx context.Context, path string, mode ninep.Mode) error {
	return ninep.ErrWriteNotAllowed
}

func (f *Fs) CreateFile(ctx context.Context, path string, flag ninep.OpenMode, mode ninep.Mode) (ninep.FileHandle, error) {
	return nil, ninep.ErrWriteNotAllowed
}

func (f *Fs) OpenFile(ctx context.Context, path string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}

	switch res.Id {
	case "image", "imageRepoTags", "imageParent", "imageComment", "imageDockerVersion",
		"imageAuthor", "imageArch", "imageVariant", "imageOS", "imageSize",
		"imageMetadata", "imageID", "imageLabels", "imageEnv", "imageJSON":
		return handleImageFile(f, res.Id, res.Vars[0], flag)
	case "container", "containerStatus", "containerPID", "containerStartedAt",
		"containerFinishedAt", "containerHealthStatus", "containerHealthFailingStreak",
		"containerHealthLogs", "containerImage", "containerName", "containerRestartCount",
		"containerPlatform", "containerLabels", "containerJSON", "containerEnv", "containerPorts", "containerMounts":
		return handleContainerFile(f, res.Id, res.Vars[0], flag)
	default:
		return nil, fs.ErrNotExist
	}
}

func (f *Fs) ListDir(ctx context.Context, path string) iter.Seq2[fs.FileInfo, error] {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}

	now := time.Now()
	switch res.Id {
	case "root":
		return ninep.FileInfoSliceIterator([]fs.FileInfo{
			ninep.DirFileInfo("images"),
			ninep.DirFileInfo("containers"),
		})
	case "images":
		return func(yield func(fs.FileInfo, error) bool) {
			images, err := f.C.ImageList(context.Background(), image.ListOptions{})
			if err != nil {
				yield(nil, err)
				return
			}
			for _, img := range images {
				info := &ninep.SimpleFileInfo{
					FIName:    img.ID,
					FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
					FIModTime: now,
				}
				if !yield(info, nil) {
					return
				}
			}
		}
	case "image":
		inspect, _, err := f.C.ImageInspectWithRaw(ctx, res.Vars[0])
		if err != nil {
			return ninep.FileInfoErrorIterator(fs.ErrNotExist)
		}
		user := inspect.Config.User
		return ninep.FileInfoSliceIteratorWithUsers([]fs.FileInfo{
			readOnlyImageFileInfo("imageRepoTags", "repo_tags", now, inspect),
			readOnlyImageFileInfo("imageParent", "parent", now, inspect),
			readOnlyImageFileInfo("imageComment", "comment", now, inspect),
			readOnlyImageFileInfo("imageDockerVersion", "docker_version", now, inspect),
			readOnlyImageFileInfo("imageAuthor", "author", now, inspect),
			readOnlyImageFileInfo("imageArch", "arch", now, inspect),
			readOnlyImageFileInfo("imageVariant", "variant", now, inspect),
			readOnlyImageFileInfo("imageOS", "os", now, inspect),
			readOnlyImageFileInfo("imageSize", "size", now, inspect),
			readOnlyImageFileInfo("imageMetadata", "metadata", now, inspect),
			readOnlyImageFileInfo("imageID", "image_id", now, inspect),
			readOnlyImageFileInfo("imageLabels", "labels", now, inspect),
			readOnlyImageFileInfo("imageEnv", "env", now, inspect),
			readOnlyImageFileInfo("imageJSON", "json", now, inspect),
		}, user, "", "")
	case "containers":
		return func(yield func(fs.FileInfo, error) bool) {
			containers, err := f.C.ContainerList(context.Background(), container.ListOptions{All: true})
			if err != nil {
				yield(nil, err)
				return
			}
			for _, container := range containers {
				info := &ninep.SimpleFileInfo{
					FIName:    container.ID,
					FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
					FIModTime: now,
				}
				if !yield(info, nil) {
					return
				}
			}
		}
	case "container":
		inspect, err := f.C.ContainerInspect(ctx, res.Vars[0])
		if err != nil {
			return ninep.FileInfoErrorIterator(fs.ErrNotExist)
		}
		t, err := time.Parse(time.RFC3339, inspect.Created)
		if err != nil {
			t = now
		}
		user := inspect.Config.User
		return ninep.FileInfoSliceIteratorWithUsers([]fs.FileInfo{
			readOnlyContainerFileInfo("containerStatus", "status", t, inspect),
			readOnlyContainerFileInfo("containerPID", "pid", t, inspect),
			readOnlyContainerFileInfo("containerStartedAt", "started_at", t, inspect),
			readOnlyContainerFileInfo("containerFinishedAt", "finished_at", t, inspect),
			ninep.DirFileInfo("health"),
			readOnlyContainerFileInfo("containerImage", "image", t, inspect),
			readOnlyContainerFileInfo("containerName", "name", t, inspect),
			readOnlyContainerFileInfo("containerRestartCount", "restart_count", t, inspect),
			readOnlyContainerFileInfo("containerPlatform", "platform", t, inspect),
			readOnlyContainerFileInfo("containerLabels", "labels", t, inspect),
			readOnlyContainerFileInfo("containerEnv", "env", t, inspect),
			readOnlyContainerFileInfo("containerJSON", "json", t, inspect),
			readOnlyContainerFileInfo("containerPorts", "ports", t, inspect),
			readOnlyContainerFileInfo("containerMounts", "mounts", t, inspect),
		}, user, "", "")
	case "containerHealth":
		inspect, err := f.C.ContainerInspect(ctx, res.Vars[0])
		if err != nil {
			return ninep.FileInfoErrorIterator(fs.ErrNotExist)
		}
		t, err := time.Parse(time.RFC3339, inspect.Created)
		if err != nil {
			t = now
		}
		user := inspect.Config.User
		return ninep.FileInfoSliceIteratorWithUsers([]fs.FileInfo{
			readOnlyContainerFileInfo("containerHealthStatus", "status", t, inspect),
			readOnlyContainerFileInfo("containerHealthFailingStreak", "failing_streak", t, inspect),
			readOnlyContainerFileInfo("containerHealthLogs", "logs", t, inspect),
		}, user, "", "")
	default:
		return ninep.FileInfoErrorIterator(fs.ErrNotExist)
	}
}

func (f *Fs) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	var res ninep.Match
	ok := mx.Match(path, &res)
	if !ok {
		return nil, fs.ErrNotExist
	}

	now := time.Now()
	switch res.Id {
	case "root", "images", "containers":
		return &ninep.SimpleFileInfo{
			FIName:    ".",
			FIModTime: now,
			FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
		}, nil

	case "image":
		// Verify image exists
		_, _, err := f.C.ImageInspectWithRaw(ctx, res.Vars[0])
		if err != nil {
			return nil, fs.ErrNotExist
		}
		return &ninep.SimpleFileInfo{
			FIName:    res.Vars[0],
			FIModTime: now,
			FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
		}, nil

	case "imageRepoTags", "imageParent", "imageComment", "imageDockerVersion",
		"imageAuthor", "imageArch", "imageVariant", "imageOS", "imageSize",
		"imageMetadata", "imageID", "imageLabels", "imageEnv", "imageJSON":
		// Verify image exists
		inspect, _, err := f.C.ImageInspectWithRaw(ctx, res.Vars[0])
		if err != nil {
			return nil, fs.ErrNotExist
		}
		content, err := contentsImageFile(strings.TrimPrefix(res.Id, "image"), inspect)
		if err != nil {
			return nil, err
		}
		return &ninep.SimpleFileInfo{
			FIName:    strings.TrimPrefix(res.Id, "image"),
			FIModTime: now,
			FIMode:    ninep.Readable,
			FISize:    int64(len(content)),
		}, nil

	case "container":
		// Verify container exists
		inspect, err := f.C.ContainerInspect(ctx, res.Vars[0])
		if err != nil {
			return nil, fs.ErrNotExist
		}
		created, err := time.Parse(time.RFC3339, inspect.Created)
		if err != nil {
			created = now
		}
		return &ninep.SimpleFileInfo{
			FIName:    res.Vars[0],
			FIModTime: created,
			FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
		}, nil

	case "containerHealth":
		inspect, err := f.C.ContainerInspect(ctx, res.Vars[0])
		if err != nil {
			return nil, fs.ErrNotExist
		}
		created, err := time.Parse(time.RFC3339, inspect.Created)
		if err != nil {
			created = now
		}
		return &ninep.SimpleFileInfo{
			FIName:    "health",
			FIModTime: created,
			FIMode:    fs.ModeDir | ninep.Readable | ninep.Executable,
		}, nil

	case "containerStatus", "containerPID", "containerStartedAt",
		"containerFinishedAt", "containerHealthStatus", "containerHealthFailingStreak",
		"containerHealthLogs", "containerImage", "containerName",
		"containerRestartCount", "containerPlatform", "containerLabels", "containerJSON", "containerEnv", "containerPorts", "containerMounts":
		// Verify container exists
		_, err := f.C.ContainerInspect(ctx, res.Vars[0])
		if err != nil {
			return nil, fs.ErrNotExist
		}
		return &ninep.SimpleFileInfo{
			FIName:    strings.TrimPrefix(res.Id, "container"),
			FIModTime: now,
			FIMode:    ninep.Readable,
		}, nil

	default:
		return nil, fs.ErrNotExist
	}
}

func (f *Fs) Delete(ctx context.Context, path string) error {
	return ninep.ErrWriteNotAllowed
}

func (f *Fs) WriteStat(ctx context.Context, path string, stat ninep.Stat) error {
	return ninep.ErrWriteNotAllowed
}

func staticDirListing(modTime time.Time, names []string) iter.Seq2[fs.FileInfo, error] {
	return func(yield func(fs.FileInfo, error) bool) {
		for _, name := range names {
			info := &ninep.SimpleFileInfo{
				FIName:    name,
				FIMode:    ninep.Readable,
				FIModTime: modTime,
			}
			if !yield(info, nil) {
				return
			}
		}
	}
}

func readOnlyImageFileInfo(ftype, name string, modTime time.Time, inspect types.ImageInspect) fs.FileInfo {
	content, _ := contentsImageFile(ftype, inspect)
	return &ninep.SimpleFileInfo{
		FIName:    name,
		FIMode:    ninep.Readable,
		FIModTime: modTime,
		FISize:    int64(len(content)),
	}
}

func readOnlyContainerFileInfo(ftype, name string, modTime time.Time, inspect types.ContainerJSON) fs.FileInfo {
	content, _ := containerFileContents(ftype, inspect)
	return &ninep.SimpleFileInfo{
		FIName:    name,
		FIMode:    ninep.Readable,
		FIModTime: modTime,
		FISize:    int64(len(content)),
	}
}
