package dockerfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/google/shlex"
	"github.com/jeffh/cfs/ninep"
)

type Fs struct {
	C *client.Client

	ninep.SimpleFileSystem
}

func NewFs() (*Fs, error) {
	// c, err := client.NewClientWithOpts(client.FromEnv)
	c, err := client.NewEnvClient()

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

		mounts := make([]string, len(ct.Mounts))
		for i, m := range ct.Mounts {
			mounts[i] = fmt.Sprintf(
				"%s %s %v %v %s %s rw=%v %s",
				m.Type,
				m.Name,
				m.Source,
				m.Destination,
				m.Driver,
				m.Mode,
				m.RW,
				m.Propagation,
			)
		}

		return staticDir(name,
			staticStringFile("id", createdAt, ct.ID),
			staticStringFile("names", createdAt, strings.Join(ct.Names, "\n")),
			staticStringFile("image", createdAt, fmt.Sprintf("%s\n%s\n", ct.Image, ct.ImageID)),
			staticStringFile("cmdline", createdAt, ct.Command),
			staticStringFile("writable_size", createdAt, fmt.Sprintf("%d", ct.SizeRw)),
			staticStringFile("rootfs_size", createdAt, fmt.Sprintf("%d", ct.SizeRootFs)),
			staticStringFile("state", createdAt, ct.State),
			staticStringFile("status", createdAt, ct.Status),
			staticStringFile("ports", createdAt, strings.Join(ports, "\n")),
			staticStringFile("mounts", createdAt, strings.Join(mounts, "\n")),
			staticDirWithTime("net", createdAt,
				staticStringFile("host_mode", createdAt, ct.HostConfig.NetworkMode),
				dynamicDirWithTime("configs", createdAt, func() ([]ninep.Node, error) {
					netCfgs := make([]ninep.Node, 0, len(ct.NetworkSettings.Networks))
					for endpoint, cfg := range ct.NetworkSettings.Networks {
						var ipamCfg ninep.Node
						if cfg.IPAMConfig != nil {
							ipamCfg = staticDirWithTime("ipam_config", createdAt,
								staticStringFile("ipv4_address", createdAt, cfg.IPAMConfig.IPv4Address),
								staticStringFile("ipv6_address", createdAt, cfg.IPAMConfig.IPv6Address),
								staticStringFile("link_local_ips", createdAt, strings.Join(cfg.IPAMConfig.LinkLocalIPs, "\n")),
							)
						}
						netCfgs = append(netCfgs, staticDir(
							endpoint,
							ipamCfg,
							staticStringFile("links", createdAt, strings.Join(cfg.Links, "\n")),
							staticStringFile("aliases", createdAt, strings.Join(cfg.Aliases, "\n")),
							staticStringFile("network_id", createdAt, cfg.NetworkID),
							staticStringFile("endpoint_id", createdAt, cfg.EndpointID),
							staticStringFile("gateway", createdAt, cfg.Gateway),
							staticStringFile("ip_addr", createdAt, fmt.Sprintf("%s/%d", cfg.IPAddress, cfg.IPPrefixLen)),
							staticStringFile("ipv6_gateway", createdAt, cfg.IPv6Gateway),
							staticStringFile("global_ipv6_address", createdAt, fmt.Sprintf("%s/%d", cfg.GlobalIPv6Address, cfg.GlobalIPv6PrefixLen)),
							staticStringFile("mac_address", createdAt, cfg.MacAddress),
							// dynamicDirWithTime("driver_options", createdAt, func() ([]ninep.Node, error) {
							// 	res := make([]ninep.Node, 0, len(cfg.DriverOpts))
							// 	for key, value := range cfg.DriverOpts {
							// 		res = append(res, staticStringFile(key, createdAt, value))
							// 	}
							// 	return res, nil
							// }),
						))
					}
					return netCfgs, nil
				}),
			),
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
		opts.Filters.Add("status", state)
		return containerListAs(c, opts, containerById)
	}

	var noCListOpts types.ContainerListOptions
	noCListOpts.Filters = filters.NewArgs()

	fs := &Fs{
		c,
		ninep.SimpleFileSystem{
			Root: ninep.StaticRootDir(
				staticDir("images",
					dynamicCtlFile("ctl", func(r io.Reader, w io.Writer) {
						buf := [2048]byte{}
						b := buf[:]
						for {
							b = buf[:]
							n, err := r.Read(b)
							b = b[:n]

							args, err := shlex.Split(string(b))
							if err != nil {
								fmt.Printf("error: %s\n", err)
								fmt.Fprintf(w, "error: %s\n", err)
								continue
							}
							if len(args) == 0 {
								goto finished
							}
							switch args[0] {
							case "pull":
								if len(args) > 1 {
									res, err := c.ImagePull(context.Background(), args[1], types.ImagePullOptions{})
									if err != nil {
										fmt.Printf("error: %s\n", err)
										fmt.Fprintf(w, "error: %s\n", err)
										goto finished
									}
									// io.Copy(ioutil.Discard, res)
									_, err = io.Copy(os.Stdout, res)
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
							default:
								fmt.Fprintf(w, "error: unrecognized command: %v", args[0])
							}
						finished:
							if err != nil {
								fmt.Printf("error: %s\n", err)
								fmt.Fprintf(w, "error: %s\n", err)
								return
							}
							if n == 0 {
								return
							}
						}
					}),
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

func dynamicCtlFile(name string, thread func(r io.Reader, w io.Writer)) *ninep.SimpleFile {
	return ninep.CtlFile(name, 0777, time.Time{}, thread)
}
