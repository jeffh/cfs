package dockerfs

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
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

	const (
		IMAGES_HELP = `Files in this directory:

help - that's this file!
ctl    - perform most docker operations on images. Terminal-like. Write 'help\n' to this file to see options.
build  - allows you to build a docker image by writing key-value pairs of
			arguments, followed by a newline, then followed by a tar of the build
			context, including the Dockerfile.
load   - allows you to load a docker image if you have one to write to directly (from docker export)
export - allows you to download docker images by writing one per line and reading this files' contents.
ids    - lists containers by their ids (content hashes)
labels - lists containers by their human-friendly labels stored locally
tags   - lists containers by their human-friendly labels from the remote registry
repos  - lists containers by their repository name + tags/shas`

		CONTAINERS_HELP = `Files in this directory:

help  - that's this file!
ctl   - performs most dockers operations on containers. Terminal-like. Write 'help\n' to this file to see options.
ids   - list all containers by their ids.
names - list all containers by their names.
state - list a subset of containers by their current state.`
	)

	var noCListOpts types.ContainerListOptions
	noCListOpts.Filters = filters.NewArgs()

	fs := &Fs{
		c,
		ninep.SimpleFileSystem{
			Root: ninep.StaticRootDir(
				staticDir("images",
					staticStringFile("help", time.Time{}, IMAGES_HELP),
					dynamicCtlFile("ctl", imagesCtl(c)),
					dynamicCtlFile("load", imageLoadCtl(c)),
					dynamicCtlFile("build", imageBuildCtl(c)),
					dynamicCtlFile("export", imageExportCtl(c)),
					dynamicDir("ids", func() ([]ninep.Node, error) {
						return imageListAs(c, func(r []ninep.Node, img types.ImageSummary) []ninep.Node {
							return append(r, imageDir(c, img.ID, img))
						})
					}),
					dynamicDir("labels", func() ([]ninep.Node, error) {
						return imageListAs(c, func(r []ninep.Node, img types.ImageSummary) []ninep.Node {
							for label := range img.Labels {
								r = append(r, imageDir(c, label, img))
							}
							return r
						})
					}),
					dynamicDir("tags", func() ([]ninep.Node, error) {
						return imageListAs(c, func(r []ninep.Node, img types.ImageSummary) []ninep.Node {
							for _, tag := range img.RepoTags {
								r = append(r, imageDir(c, tag, img))
							}
							return r
						})
					}),
					dynamicDirTree("repos", func() ([]ninep.Node, error) {
						return imageListAs(c, func(r []ninep.Node, img types.ImageSummary) []ninep.Node {
							for _, digest := range img.RepoDigests {
								r = append(r, imageDir(c, digest, img))
							}
							for _, label := range img.RepoTags {
								r = append(r, imageDir(c, label, img))
							}
							return r
						})
					}),
				),
				staticDir("containers",
					staticStringFile("help", time.Time{}, CONTAINERS_HELP),
					dynamicCtlFile("ctl", containersCtl(c, noCListOpts)),
					dynamicDir("ids", func() ([]ninep.Node, error) {
						return containerListAs(c, noCListOpts, containerById)
					}),
					dynamicDir("names", func() ([]ninep.Node, error) {
						return containerListAs(c, noCListOpts, func(c *client.Client, r []ninep.Node, cntr types.Container) []ninep.Node {
							for _, name := range cntr.Names {
								r = append(r, containerDir(c, name[1:], cntr))
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
				// staticDir("swarm",
				// 	staticDir("services"),
				// 	staticDir("nodes"),
				// 	staticDir("configs"),
				// 	staticDir("secrets"),
				// ),
				// staticDir("stacks"),
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
