package dockerfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/google/shlex"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

func imagesCtl(c *client.Client) func(ninep.OpenMode, io.Reader, io.Writer) {
	return func(m ninep.OpenMode, rdr io.Reader, w io.Writer) {
		r := ninep.LineReader{R: rdr}
		for {
			cmd, readErr := r.ReadLine()

			args, err := shlex.Split(cmd)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				fmt.Fprintf(w, "error: %s\n", err)
				continue
			}
			if len(args) == 0 {
				goto finished
			}
			switch args[0] {
			case "help":
				fmt.Fprintf(w, "COMMANDS:\n\n")
				fmt.Fprintf(w, " pull IMAGE_NAME   - makes the docker host fetch a docker image from a remote registry\n")
				fmt.Fprintf(w, " push IMAGE_NAME   - makes the docker host push a docker image from a remote registry\n")
				fmt.Fprintf(w, " tag SOURCE TAG    - makes the docker host tag a given source\n")
				fmt.Fprintf(w, " search QUERY      - makes the docker host search for docker images from a remote registry\n")
				fmt.Fprintf(w, " delete IMAGE_NAME - makes the docker host delete a local docker image\n")
				fmt.Fprintf(w, " exit              - tells the fs to close the ctl file. Useful when you want to wait for a command to finish\n")
				fmt.Fprintf(w, " help              - returns this help\n")
			case "pull", "fetch":
				if len(args) > 1 {
					ref := resolveImageRef(args[1])
					res, err := c.ImagePull(context.Background(), ref, image.PullOptions{})
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					}
					io.Copy(io.Discard, res)
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
					} else {
						fmt.Fprintf(w, "ok\n")
					}
					res.Close()
				} else {
					w.Write([]byte("error: missing image to fetch"))
				}
			case "tag":
				if len(args) > 2 {
					source := args[1]
					tag := args[2]
					err := c.ImageTag(context.Background(), source, tag)
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					}
					fmt.Fprintf(w, "ok\n")
				} else {
					w.Write([]byte("error: missing source or tag"))
				}
			case "push":
				if len(args) > 1 {
					ref := resolveImageRef(args[1])
					res, err := c.ImagePush(context.Background(), ref, image.PushOptions{})
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					}
					io.Copy(io.Discard, res)
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
					} else {
						fmt.Fprintf(w, "ok\n")
					}
					res.Close()
				} else {
					w.Write([]byte("error: missing image to fetch"))
				}
			case "search":
				if len(args) > 1 {
					term := args[1]
					imgs, err := c.ImageSearch(context.Background(), term, types.ImageSearchOptions{Limit: 100})
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					}
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
					} else {
						for _, img := range imgs {
							star := ""
							if img.IsOfficial {
								star = "*"
							}
							fmt.Fprintf(w, "%s %s- (%d) %s\n", img.Name, star, img.StarCount, img.Description)
						}
					}
				} else {
					w.Write([]byte("error: missing image to fetch"))
				}
			case "delete":
				if len(args) > 1 {
					imageId := args[1]
					_, err := c.ImageRemove(context.Background(), imageId, image.RemoveOptions{})
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					} else {
						fmt.Fprintf(w, "ok\n")
					}
				}
			case "prune":
				res, err := c.ImagesPrune(context.Background(), filters.NewArgs())
				if err != nil {
					fmt.Printf("error: %s\n", err)
					fmt.Fprintf(w, "error: %s\n", err)
					goto finished
				} else {
					fmt.Fprintf(w, "space_reclaimed %d\n", res.SpaceReclaimed)
					for _, img := range res.ImagesDeleted {
						if img.Deleted != "" {
							fmt.Fprintf(w, "deleted %s\n", img.Deleted)
						}
						if img.Untagged != "" {
							fmt.Fprintf(w, "untagged %s\n", img.Untagged)
						}
					}
					fmt.Fprintf(w, "ok\n")
				}
			case "exit", "done", "quit":
				return
			default:
				fmt.Fprintf(w, "error: unrecognized command: %v", args[0])
			}
		finished:
			if err != nil {
				fmt.Printf("error: %s\n", err)
				fmt.Fprintf(w, "error: %s\n", err)
				return
			}
			if cmd == "" {
				return
			}

			if readErr != nil {
				fmt.Printf("error: %s\n", readErr)
				fmt.Fprintf(w, "error: %s\n", readErr)
				return
			}
		}
	}
}

func imageLoadCtl(c *client.Client) func(ninep.OpenMode, io.Reader, io.Writer) {
	return func(m ninep.OpenMode, r io.Reader, w io.Writer) {
		wr := w.(*io.PipeWriter)
		out, err := c.ImageLoad(context.Background(), r, false)
		if err != nil {
			wr.CloseWithError(err)
			return
		}
		defer out.Body.Close()
		_, err = io.Copy(wr, out.Body)
		if err != nil {
			wr.CloseWithError(err)
			return
		}
	}
}

func imageBuildCtl(c *client.Client) func(ninep.OpenMode, io.Reader, io.Writer) {
	return func(m ninep.OpenMode, r io.Reader, w io.Writer) {
		lr := &ninep.LineReader{R: r}
		line, err := lr.ReadLine()
		if err != nil {
			return
		}

		kvs := kvp.MustParseKeyValues(line)
		buildOpts := types.ImageBuildOptions{
			Tags:           kvs.GetAll("tags"),
			SuppressOutput: kvs.GetOneBool("suppress_output"),
			RemoteContext:  kvs.GetOne("remote_context"),
			NoCache:        kvs.GetOneBool("no_cache"),
			Remove:         kvs.GetOneBool("remove"),
			ForceRemove:    kvs.GetOneBool("force_remove"),
			PullParent:     kvs.GetOneBool("pull_parent"),
			// Isolation      container.Isolation
			CPUSetCPUs:   kvs.GetOne("cpu_set_cpus"),
			CPUSetMems:   kvs.GetOne("cpu_set_mems"),
			CPUShares:    kvs.GetOneInt64("cpu_shares"),
			CPUQuota:     kvs.GetOneInt64("cpu_quota"),
			CPUPeriod:    kvs.GetOneInt64("cpu_period"),
			Memory:       kvs.GetOneInt64("memory"),
			MemorySwap:   kvs.GetOneInt64("memory_swap"),
			CgroupParent: kvs.GetOne("cgroup_parent"),
			NetworkMode:  kvs.GetOne("network_mode"),
			ShmSize:      kvs.GetOneInt64("shm_size"),
			Dockerfile:   kvs.GetOne("dockerfile"),
			// Ulimits:      ulimitsFrom(kvs),
			// BuildArgs   map[string]*string
			// AuthConfigs map[string]AuthConfig
			// Context     io.Reader
			Labels:      kvs.GetAllPrefix("label").Flatten(),
			Squash:      kvs.GetOneBool("squash"),
			CacheFrom:   kvs.GetAll("cache_from"),
			SecurityOpt: kvs.GetAll("security_opt"),
			// ExtraHosts:     kvs.GetAll("extra_hosts"),
			// Target:         kvs.GetOne("target"),
			// SessionID:      kvs.GetOne("session_id"),
			// Platform:       kvs.GetOne("platform"),
			// BuilderVersion: types.BuilderVersion(kvs.GetOne("build_version")),
			// BuilderID:      kvs.GetOne("build_id"), // TODO: we should propbably generate this and manage a dir as the build is occurring
			// Outputs []ImageBuildOutput
		}
		out, err := c.ImageBuild(context.Background(), lr, buildOpts)
		if err != nil {
			fmt.Fprintf(w, "error: %s\n", err)
			return
		}
		defer out.Body.Close()
		_, err = io.Copy(w, out.Body)
		if err != nil {
			fmt.Fprintf(w, "error: %s\n", err)
			return
		}
		return
	}
}

func imageExportCtl(c *client.Client) func(ninep.OpenMode, io.Reader, io.Writer) {
	return func(m ninep.OpenMode, r io.Reader, w io.Writer) {
		wr := w.(*io.PipeWriter)
		imageIdsByLine, err := io.ReadAll(r)
		if err != nil {
			wr.CloseWithError(err)
			return
		}
		imageIds := strings.Split(string(imageIdsByLine), "\n")
		rc, err := c.ImageSave(context.Background(), imageIds)
		if err != nil {
			wr.CloseWithError(err)
			return
		}
		_, err = io.Copy(w, rc)
		if err != nil {
			wr.CloseWithError(err)
			return
		}
	}
}

func resolveImageRef(ref string) string {
	if strings.Index(ref, ".") == -1 {
		if strings.Index(ref, "/") == -1 {
			ref = fmt.Sprintf("docker.io/library/%s", ref)
		} else {
			ref = fmt.Sprintf("docker.io/%s", ref)
		}
	}
	return ref
}

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
