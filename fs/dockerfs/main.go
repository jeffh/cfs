package dockerfs

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
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
			dynamicCtlFile("image", func(r io.Reader, w io.Writer) {
				wr := w.(*io.PipeWriter)
				imageIdsByLine, err := ioutil.ReadAll(r)
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
			}),
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

	resolveImageRef := func(ref string) string {
		if strings.Index(ref, ".") == -1 {
			if strings.Index(ref, "/") == -1 {
				ref = fmt.Sprintf("docker.io/library/%s", ref)
			} else {
				ref = fmt.Sprintf("docker.io/%s", ref)
			}
		}
		return ref
	}

	imageCtl := func(rdr io.Reader, w io.Writer) {
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
				fmt.Fprintf(w, " search QUERY      - makes the docker host search for docker images from a remote registry\n")
				fmt.Fprintf(w, " delete IMAGE_NAME - makes the docker host delete a local docker image\n")
				fmt.Fprintf(w, " exit              - tells the fs to close the ctl file. Useful when you want to wait for a command to finish\n")
				fmt.Fprintf(w, " help              - returns this help\n")
			case "pull", "fetch":
				if len(args) > 1 {
					ref := resolveImageRef(args[1])
					res, err := c.ImagePull(context.Background(), ref, types.ImagePullOptions{})
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					}
					io.Copy(ioutil.Discard, res)
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
			case "push":
				if len(args) > 1 {
					ref := resolveImageRef(args[1])
					res, err := c.ImagePush(context.Background(), ref, types.ImagePushOptions{})
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					}
					io.Copy(ioutil.Discard, res)
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
					_, err := c.ImageRemove(context.Background(), imageId, types.ImageRemoveOptions{})
					if err != nil {
						fmt.Printf("error: %s\n", err)
						fmt.Fprintf(w, "error: %s\n", err)
						goto finished
					} else {
						fmt.Fprintf(w, "ok\n")
					}
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

	imageCreateCtl := func(r io.Reader, w io.Writer) {
		wr := w.(*io.PipeWriter)
		_, err := c.ImageLoad(context.Background(), r, true)
		if err != nil {
			wr.CloseWithError(err)
		}
		fmt.Fprintf(w, "ok\n")
	}

	containerCtl := func(r io.Reader, w io.Writer) {
	}

	fs := &Fs{
		c,
		ninep.SimpleFileSystem{
			Root: ninep.StaticRootDir(
				staticDir("images",
					dynamicCtlFile("ctl", imageCtl),
					dynamicCtlFile("create", imageCreateCtl),
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
					dynamicCtlFile("ctl", containerCtl),
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
				staticDir("system",
					dynamicDir("info", func() ([]ninep.Node, error) {
						now := time.Now()
						info, err := c.Info(context.Background())
						if err != nil {
							return nil, err
						}
						files := make([]ninep.Node, 0, 53)
						files = append(files, staticStringFile("id", now, info.ID))

						intStr := func(v int) string { return fmt.Sprintf("%d", v) }
						files = append(files, staticStringFile("stats", now, ninep.KeyValues(map[string]string{
							"num_containers":         intStr(info.Containers),
							"num_containers_running": intStr(info.ContainersRunning),
							"num_containers_paused":  intStr(info.ContainersPaused),
							"num_containers_stopped": intStr(info.ContainersStopped),
							"num_images":             intStr(info.Images),
							"num_fds":                intStr(info.NFd),
							"num_goroutines":         intStr(info.NGoroutines),
							"num_events_listeners":   intStr(info.NEventsListener),
							"num_cpus":               intStr(info.NCPU),
							"mem_total":              fmt.Sprintf("%d", info.MemTotal),
						})))

						files = append(files, staticStringFile("driver", now, fmt.Sprintf(
							"%s\n%s\n%s\n%s\n",
							info.Driver,
							info.LoggingDriver,
							info.CgroupDriver,
							ninep.KeyPairs(info.DriverStatus),
						)))
						files = append(files, staticStringFile("status", now, ninep.KeyPairs(info.SystemStatus)))

						boolStr := func(v bool) string {
							if v {
								return "true"
							} else {
								return "false"
							}
						}
						files = append(files, staticStringFile("flags", now, ninep.KeyValues(map[string]string{
							"memory_limit":         boolStr(info.MemoryLimit),
							"swap_limit":           boolStr(info.SwapLimit),
							"kernel_memory":        boolStr(info.KernelMemory),
							"cpu_cfs_period":       boolStr(info.CPUCfsPeriod),
							"cpu_cfs_quota":        boolStr(info.CPUCfsQuota),
							"cpu_shares":           boolStr(info.CPUShares),
							"cpu_set":              boolStr(info.CPUSet),
							"ipv4_forwarding":      boolStr(info.IPv4Forwarding),
							"bridge_nf_ipv4tables": boolStr(info.BridgeNfIptables),
							"bridge_nf_ipv6tables": boolStr(info.BridgeNfIP6tables),
							"debug":                boolStr(info.Debug),
							"disable_oom_kill":     boolStr(info.OomKillDisable),
							"experimental_build":   boolStr(info.ExperimentalBuild),
							"live_restore_enabled": boolStr(info.LiveRestoreEnabled),
						})))
						files = append(files, staticDirWithTime("plugins", now,
							staticStringFile("volumes", now, strings.Join(info.Plugins.Volume, "\n")),
							staticStringFile("network", now, strings.Join(info.Plugins.Network, "\n")),
							staticStringFile("authorization", now, strings.Join(info.Plugins.Authorization, "\n")),
						))

						files = append(files, staticStringFile("about", now, ninep.KeyValues(map[string]string{
							"kernel_version":    info.KernelVersion,
							"os":                info.OperatingSystem,
							"os_type":           info.OSType,
							"arch":              info.Architecture,
							"index_server":      info.IndexServerAddress,
							"docker_root_dir":   info.DockerRootDir,
							"http_proxy":        info.HTTPProxy,
							"https_proxy":       info.HTTPSProxy,
							"no_proxy":          info.NoProxy,
							"name":              info.Name,
							"server_version":    info.ServerVersion,
							"cluster_store":     info.ClusterStore,
							"cluster_advertise": info.ClusterAdvertise,
							"default_runtime":   info.DefaultRuntime,
						})))
						return files, nil
					}),
				),
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

func dynamicFile(name string, modTime time.Time, content func() ([]byte, error)) *ninep.SimpleFile {
	return &ninep.SimpleFile{
		SimpleFileInfo: ninep.SimpleFileInfo{
			FIName:    name,
			FIMode:    os.ModeDir | 0777,
			FIModTime: modTime,
		},
		OpenFn: func() (ninep.FileHandle, error) {
			b, err := content()
			if err != nil {
				return nil, err
			}
			return &ninep.ReadOnlyMemoryFileHandle{b}, nil
		},
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
