package dockerfs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/jeffh/cfs/ninep"
)

type Fs struct {
	C *client.Client

	ninep.SimpleFileSystem
}

func NewFs() (*Fs, error) {
	// c, err := client.NewClientWithOpts(client.FromEnv)
	c, err := client.NewEnvClient()

	imageListAs := func(c *client.Client, fn func(results []ninep.Node, img types.ImageSummary) []ninep.Node) ([]ninep.Node, error) {
		imgs, err := c.ImageList(context.Background(), types.ImageListOptions{})
		if err != nil {
			return nil, err
		}

		children := make([]ninep.Node, 0, len(imgs))
		for _, img := range imgs {
			children = fn(children, img)
		}
		return children, nil
	}

	containerDir := func(name string, ct types.Container) ninep.Node {
		createdAt := time.Unix(ct.Created, 0)
		ports := make([]string, len(ct.Ports))
		for i, p := range ct.Ports {
			ports[i] = fmt.Sprintf("%s %s:%d %d", p.Type, p.IP, p.PublicPort, p.PrivatePort)
		}
		return staticDir(name,
			staticStringFile("id", createdAt, ct.ID),
			staticStringFile("names", createdAt, strings.Join(ct.Names, "\n")),
			staticStringFile("image", createdAt, fmt.Sprintf("%s\n%s\n", ct.Image, ct.ImageID)),
			staticStringFile("cmdline", createdAt, ct.Command),
			staticStringFile("state", createdAt, ct.State),
			staticStringFile("status", createdAt, ct.Status),
			staticStringFile("ports", createdAt, strings.Join(ports, "\n")),
		)
	}

	containerById := func(r []ninep.Node, cntr types.Container) []ninep.Node {
		return append(r, containerDir(cntr.ID, cntr))
	}

	containerListAs := func(c *client.Client, opts types.ContainerListOptions, fn func([]ninep.Node, types.Container) []ninep.Node) ([]ninep.Node, error) {
		opts.All = true
		cntrs, err := c.ContainerList(context.Background(), opts)
		if err != nil {
			return nil, err
		}

		children := make([]ninep.Node, 0, len(cntrs))
		for _, cntr := range cntrs {
			children = fn(children, cntr)
		}
		return children, nil
	}

	containerListByState := func(c *client.Client, opts types.ContainerListOptions, state string) ([]ninep.Node, error) {
		opts.Filters.FuzzyMatch("status", state)
		return containerListAs(c, opts, containerById)
	}

	imageDir := func(name string, img types.ImageSummary) ninep.Node {
		createdAt := time.Unix(img.Created, 0)
		dir := staticDir(name,
			staticStringFile("id", createdAt, img.ID),
			staticStringFile("parent_id", createdAt, img.ParentID),
			staticStringFile("size", createdAt, fmt.Sprintf("%d", img.Size)),
			staticStringFile("shared_size", createdAt, fmt.Sprintf("%d", img.SharedSize)),
			staticStringFile("virtual_size", createdAt, fmt.Sprintf("%d", img.VirtualSize)),
			dynamicDirWithTime("labels", createdAt, func() ([]ninep.Node, error) {
				n := make([]ninep.Node, 0, len(img.Labels))
				for label, value := range img.Labels {
					n = append(n, staticStringFile(label, createdAt, value))
				}
				return n, nil
			}),
			staticStringFile("virtual_size", createdAt, fmt.Sprintf("%d", img.VirtualSize)),
		)
		dir.SimpleFileInfo.FIModTime = createdAt
		return dir
	}

	var noCListOpts types.ContainerListOptions

	fs := &Fs{
		c,
		ninep.SimpleFileSystem{
			Root: ninep.StaticRootDir(
				staticDir("images",
					dynamicDir("ids", func() ([]ninep.Node, error) {
						return imageListAs(c, func(r []ninep.Node, img types.ImageSummary) []ninep.Node {
							return append(r, imageDir(img.ID, img))
						})
					}),
					dynamicDir("labels", func() ([]ninep.Node, error) {
						return imageListAs(c, func(r []ninep.Node, img types.ImageSummary) []ninep.Node {
							for label := range img.Labels {
								r = append(r, imageDir(label, img))
							}
							return r
						})
					}),
					dynamicDirTree("repos", func() ([]ninep.Node, error) {
						return imageListAs(c, func(r []ninep.Node, img types.ImageSummary) []ninep.Node {
							for _, digest := range img.RepoDigests {
								r = append(r, imageDir(digest, img))
							}
							for _, label := range img.RepoTags {
								r = append(r, imageDir(label, img))
							}
							return r
						})
					}),
				),
				staticDir("containers",
					dynamicDir("ids", func() ([]ninep.Node, error) {
						return containerListAs(c, noCListOpts, containerById)
					}),
					dynamicDir("names", func() ([]ninep.Node, error) {
						return containerListAs(c, noCListOpts, func(r []ninep.Node, cntr types.Container) []ninep.Node {
							for _, name := range cntr.Names {
								r = append(r, containerDir(name, cntr))
							}
							return r
						})
					}),
					staticDir("state",
						dynamicDir("created", func() ([]ninep.Node, error) { return containerListByState(c, noCListOpts, "created") }),
						dynamicDir("running", func() ([]ninep.Node, error) { return containerListByState(c, noCListOpts, "running") }),
						dynamicDir("paused", func() ([]ninep.Node, error) { return containerListByState(c, noCListOpts, "paused") }),
						dynamicDir("restarting", func() ([]ninep.Node, error) { return containerListByState(c, noCListOpts, "restarting") }),
						dynamicDir("removing", func() ([]ninep.Node, error) { return containerListByState(c, noCListOpts, "removing") }),
						dynamicDir("exited", func() ([]ninep.Node, error) { return containerListByState(c, noCListOpts, "exited") }),
						dynamicDir("dead", func() ([]ninep.Node, error) { return containerListByState(c, noCListOpts, "dead") }),
					),
				),
				staticDir("swarm",
					staticDir("services"),
					staticDir("nodes"),
					staticDir("configs"),
					staticDir("secrets"),
				),
				staticDir("stacks"),
				staticDir("networks"),
				staticDir("volumes"),
				staticDir("system"),
			),
		},
	}
	return fs, err
}

func (f *Fs) Close() error {
	if f.C != nil {
		return f.C.Close()
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////////

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
