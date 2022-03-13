package dockerfs

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/google/shlex"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

func containerDir(c *client.Client, name string, ct types.Container) ninep.Node {
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

	containerLogs := func(opt types.ContainerLogsOptions) func(m ninep.OpenMode, r io.Reader, w io.Writer) {
		return func(m ninep.OpenMode, r io.Reader, w io.Writer) {
			wr := w.(*io.PipeWriter)
			out, err := c.ContainerLogs(context.Background(), ct.ID, opt)
			if err != nil {
				wr.CloseWithError(err)
				return
			}
			defer out.Close()
			_, err = io.Copy(wr, out)
			if err != nil {
				wr.CloseWithError(err)
				return
			}
		}
	}

	names := make([]string, len(ct.Names))
	for i, name := range ct.Names {
		names[i] = name[1:]
	}

	containerID := ct.ID
	return staticDir(name,
		staticStringFile("id", createdAt, ct.ID),
		staticStringFile("names", createdAt, strings.Join(names, "\n")),
		staticStringFile("image_id", createdAt, fmt.Sprintf("%s\n%s\n", ct.ImageID, ct.Image)),
		staticStringFile("cmdline", createdAt, ct.Command),
		staticStringFile("writable_size", createdAt, fmt.Sprintf("%d", ct.SizeRw)),
		staticStringFile("rootfs_size", createdAt, fmt.Sprintf("%d", ct.SizeRootFs)),
		staticStringFile("state", createdAt, ct.State),
		staticStringFile("status", createdAt, ct.Status),
		staticStringFile("ports", createdAt, strings.Join(ports, "\n")),
		staticStringFile("mounts", createdAt, strings.Join(mounts, "\n")),
		dynamicCtlFile("logs", containerLogs(types.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Tail:       "all",
		})),
		dynamicCtlFile("stats", func(m ninep.OpenMode, r io.Reader, w io.Writer) {
			wr := w.(*io.PipeWriter)
			out, err := c.ContainerStats(context.Background(), ct.ID, true)
			if err != nil {
				wr.CloseWithError(err)
				return
			}
			defer out.Body.Close()
			decoder := json.NewDecoder(out.Body)

			uint64sStr := func(vs []uint64) string {
				strs := make([]string, len(vs))
				for i, v := range vs {
					strs[i] = fmt.Sprintf("%d", v)
				}
				return strings.Join(strs, ",")
			}

			var stat types.Stats
			for decoder.More() {
				err := decoder.Decode(&stat)
				if err != nil {
					wr.CloseWithError(err)
					return
				}

				_, err = fmt.Fprintf(wr, "%s\n", kvp.KeyPairs([][2]string{
					{"read_timestamp", fmt.Sprintf("%s", stat.Read)},
					{"cpu_usage.total", fmt.Sprintf("%d", stat.CPUStats.CPUUsage.TotalUsage)},
					{"cpu_usage.per_cpu", uint64sStr(stat.CPUStats.CPUUsage.PercpuUsage)},
					{"cpu_usage.in_kernel_mode", fmt.Sprintf("%d", stat.CPUStats.CPUUsage.UsageInKernelmode)},
					{"cpu_usage.in_user_mode", fmt.Sprintf("%d", stat.CPUStats.CPUUsage.UsageInUsermode)},
					{"cpu_usage.system_usage.linux", fmt.Sprintf("%d", stat.CPUStats.SystemUsage)},
					{"cpu_usage.throttling.periods.linux", fmt.Sprintf("%d", stat.CPUStats.ThrottlingData.Periods)},
					{"cpu_usage.throttling.throttled_periods.linux", fmt.Sprintf("%d", stat.CPUStats.ThrottlingData.ThrottledPeriods)},
					{"cpu_usage.throttling.throttled_time.ns.linux", fmt.Sprintf("%d", stat.CPUStats.ThrottlingData.ThrottledTime)},
					{"memory_usage.usage.linux", fmt.Sprintf("%d", stat.MemoryStats.Usage)},
					{"memory_usage.max_usage.linux", fmt.Sprintf("%d", stat.MemoryStats.MaxUsage)},
					{"memory_usage.fail_count.linux", fmt.Sprintf("%d", stat.MemoryStats.Failcnt)},
					{"memory_usage.limit.linux", fmt.Sprintf("%d", stat.MemoryStats.Limit)},
					{"memory_usage.commit", fmt.Sprintf("%d", stat.MemoryStats.Commit)},
					{"memory_usage.commit_peak.linux", fmt.Sprintf("%d", stat.MemoryStats.CommitPeak)},
					{"memory_usage.private_working_set", fmt.Sprintf("%d", stat.MemoryStats.PrivateWorkingSet)},
				}))
				if err != nil {
					wr.CloseWithError(err)
					return
				}
			}
		}),
		dynamicCtlFile("stats.json", func(m ninep.OpenMode, r io.Reader, w io.Writer) {
			wr := w.(*io.PipeWriter)
			out, err := c.ContainerStats(context.Background(), ct.ID, true)
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
		}),
		dynamicCtlFile("ctl", func(m ninep.OpenMode, r io.Reader, w io.Writer) {
			okOrErr := func(wr *io.PipeWriter, err error) bool {
				if err != nil {
					wr.CloseWithError(err)
					return true
				}
				fmt.Fprintf(wr, "ok\n")
				return false
			}
			for {
				rl := &ninep.LineReader{R: r}
				wr := w.(*io.PipeWriter)
				line, readErr := rl.ReadLine()
				args, err := shlex.Split(line)
				if err != nil {
					fmt.Printf("error: %s\n", err)
					fmt.Fprintf(wr, "error: %s\n", err)
					continue
				}
				if len(args) == 0 {
					goto finished
				}
				switch args[0] {
				case "help":
					fmt.Fprintf(w, "COMMANDS:\n\n")
					fmt.Fprintf(w, " start       - Starts this container\n")
					fmt.Fprintf(w, " stop        - Stops this container, or forcefully kill if it takes too long\n")
					fmt.Fprintf(w, " restart     - Restarts this container, forcefully killing if it takes too long\n")
					fmt.Fprintf(w, " kill SIGNAL - Sends this container a signal\n")
					fmt.Fprintf(w, " pause       - Pauses the main process of the container without terminating it\n")
					fmt.Fprintf(w, " unpause     - Unpauses the main process of the container without terminating it\n")
					fmt.Fprintf(w, " exit        - tells the fs to close the ctl file. Useful when you want to wait for a command to finish\n")
					fmt.Fprintf(w, " help        - returns this help\n")
				case "start":
					if okOrErr(wr, c.ContainerStart(context.Background(), containerID, types.ContainerStartOptions{})) {
						return
					}
				case "stop":
					if okOrErr(wr, c.ContainerStop(context.Background(), containerID, nil)) {
						return
					}
				case "kill":
					signal := "SIGKILL"
					if len(args) > 1 {
						signal = args[1]
					}
					if okOrErr(wr, c.ContainerKill(context.Background(), containerID, signal)) {
						return
					}
				case "pause":
					if okOrErr(wr, c.ContainerPause(context.Background(), containerID)) {
						return
					}
				case "unpause":
					if okOrErr(wr, c.ContainerUnpause(context.Background(), containerID)) {
						return
					}
				case "restart":
					if okOrErr(wr, c.ContainerRestart(context.Background(), containerID, nil)) {
						return
					}
				}
			finished:
				if err != nil {
					fmt.Printf("error: %s\n", err)
					fmt.Fprintf(wr, "error: %s\n", err)
					return
				}
				if line == "" {
					return
				}

				if readErr != nil {
					fmt.Printf("error: %s\n", readErr)
					fmt.Fprintf(wr, "error: %s\n", readErr)
					return
				}
			}
		}),
		dynamicCtlFile("stdout", containerLogs(types.ContainerLogsOptions{ShowStdout: true, Tail: "all"})),
		dynamicCtlFile("stderr", containerLogs(types.ContainerLogsOptions{ShowStderr: true, Tail: "all"})),
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

func containerById(c *client.Client, r []ninep.Node, cntr types.Container) []ninep.Node {
	return append(r, containerDir(c, cntr.ID, cntr))
}

func containerListAs(c *client.Client, opts types.ContainerListOptions, fn func(*client.Client, []ninep.Node, types.Container) []ninep.Node) ([]ninep.Node, error) {
	opts.All = true
	cntrs, err := c.ContainerList(context.Background(), opts)
	if err != nil {
		return nil, err
	}

	children := make([]ninep.Node, 0, len(cntrs))
	for _, cntr := range cntrs {
		children = fn(c, children, cntr)
	}
	return children, nil
}

func containerListByState(c *client.Client, opts types.ContainerListOptions, state string) ([]ninep.Node, error) {
	opts.Filters.Add("status", state)
	return containerListAs(c, opts, containerById)
}

func containersCtl(c *client.Client, opts types.ContainerListOptions) func(ninep.OpenMode, io.Reader, io.Writer) {
	return func(m ninep.OpenMode, rdr io.Reader, w io.Writer) {
		r := ninep.LineReader{R: rdr}
		wr := w.(*io.PipeWriter)
		var lastContainerID string
		findContainerId := func(args []string) (string, bool) {
			size := len(args)
			containerID := lastContainerID
			if size >= 2 {
				containerID = args[1]
			}

			lastContainerID = containerID

			return containerID, containerID != ""
		}
		okOrErr := func(wr *io.PipeWriter, err error) bool {
			if err != nil {
				wr.CloseWithError(err)
				return true
			}
			fmt.Fprintf(wr, "%s\n", lastContainerID)
			fmt.Fprintf(wr, "ok\n")
			return false
		}
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
				fmt.Fprintf(w, " create [FLAGS] IMAGE       - Runs a docker container image with a given, optional command. Pass '-help' to see more options\n")
				fmt.Fprintf(w, " start CONTAINER_ID         - Starts a created or stopped container\n")
				fmt.Fprintf(w, " stop CONTAINER_ID          - Stops a running container, eventually killing if the container takes too long\n")
				fmt.Fprintf(w, " kill CONTAINER_ID [SIGNAL] - Sends a signal to the container's main process (defaults to 'SIGKILL')\n")
				fmt.Fprintf(w, " restart CONTAINER_ID       - Restarts a running container, killing if the container takes too long to stop\n")
				fmt.Fprintf(w, " pause CONTAINER_ID         - Pauses a running container's main process\n")
				fmt.Fprintf(w, " unpause CONTAINER_ID       - Unpauses a running container's main process\n")
				fmt.Fprintf(w, " prune                      - Removes inactive containers\n")
				fmt.Fprintf(w, " exit                       - tells the fs to close the ctl file. Useful when you want to wait for a command to finish\n")
				fmt.Fprintf(w, " help                       - returns this help\n")
			case "create":
				var (
					img           string
					env           string
					containerName string
					volumes       string
					entryPoint    string
					onbuild       string
					shell         string
					exposedPorts  string

					timeout int

					cfg     container.Config
					hostCfg container.HostConfig
					netCfg  network.NetworkingConfig
				)
				fset := flag.NewFlagSet(args[0], flag.ContinueOnError)
				fset.StringVar(&cfg.Hostname, "hostname", "", "the hostname of the container")
				fset.StringVar(&cfg.Domainname, "domainname", "", "the domainname of the container")
				fset.StringVar(&cfg.User, "user", "", "the user that runs the cmd inside the container. also, supports user:group")
				fset.BoolVar(&cfg.AttachStdin, "stdin", false, "Attaches to stdin for possible user interaction")
				fset.BoolVar(&cfg.AttachStdout, "stdout", false, "Attaches to stdout")
				fset.BoolVar(&cfg.AttachStderr, "stderr", false, "Attaches to stderr")
				// fset.BoolVar(&cfg.Tty, "tty", "", "Attach stdin, stdout, stderr to terminal tty")
				fset.BoolVar(&cfg.OpenStdin, "open-stdin", false, "Opens stdin")
				fset.BoolVar(&cfg.StdinOnce, "once-stdin", false, "If true, close stdin after the 1 attached client disconnects.")
				fset.StringVar(&env, "e", "", "a comma-separated list of environment variables to use")
				fset.StringVar(&containerName, "name", "", "a unique name to give a container. Default generates a new container name.")
				fset.StringVar(&volumes, "volumes", "", "a list of volumes to attach to the container, separated by commas")
				fset.StringVar(&cfg.WorkingDir, "working-dir", "", "Current directory (PWD) in the command will be launched")
				fset.StringVar(&entryPoint, "entry-point", "", "Entrypoint to run when starting the container")
				fset.BoolVar(&cfg.NetworkDisabled, "disable-network", false, "Is network disabled")
				fset.StringVar(&cfg.MacAddress, "mac-addr", "", "Mac Address of the container")
				fset.StringVar(&onbuild, "onbuild", "", "ONBUILD metadata that were defined on the image Dockerfile, comma separated")
				fset.StringVar(&cfg.StopSignal, "stop-signal", "", "Signal to stop a container")
				fset.IntVar(&timeout, "stop-timeout", 0, "Timeout (in seconds) to stop a container")
				fset.StringVar(&shell, "shell", "", "Shell for shell-form of RUN, CMD, ENTRYPOINT, commas separated")
				fset.StringVar(&exposedPorts, "exposed-ports", "", "List of exposed ports, commas separated")
				fset.BoolVar(&hostCfg.AutoRemove, "auto-remove", false, "Automatically remove container when it exits")
				fset.BoolVar(&hostCfg.Privileged, "privileged", false, "Is the container in privileged mode")
				fset.BoolVar(&hostCfg.PublishAllPorts, "published-all-ports", false, "Should docker publish all exposed port for the container")
				fset.BoolVar(&hostCfg.ReadonlyRootfs, "readyonly-root-fs", false, "Is the container root filesystem in read-only")

				err := fset.Parse(args[1:])
				if err != nil {
					wr.CloseWithError(err)
				}
				args = fset.Args()
				size := len(args)
				if size >= 1 {
					img = args[0]
					if size >= 2 {
						cmd = args[1]
					}

					if env != "" {
						cfg.Env = strslice.StrSlice(strings.Split(env, ","))
					}

					if entryPoint != "" {
						cfg.Entrypoint = strslice.StrSlice(strings.Split(entryPoint, ","))
					}

					cfg.Image = img
					if volumes != "" {
						m := make(map[string]struct{})
						for _, name := range strings.Split(volumes, ",") {
							m[name] = struct{}{}
						}
						cfg.Volumes = m
					}

					if onbuild != "" {
						cfg.OnBuild = strings.Split(onbuild, ",")
					}
					// TODO: set ExposedPorts
					// TODO: set labels
					if timeout != 0 {
						cfg.StopTimeout = &timeout
					}

					if shell != "" {
						cfg.Shell = strings.Split(shell, ",")
					}

					if exposedPorts != "" {
						exposed, bindings, err := nat.ParsePortSpecs(strings.Split(exposedPorts, ","))
						if err != nil {
							wr.CloseWithError(err)
							return
						}
						cfg.ExposedPorts = exposed
						hostCfg.PortBindings = bindings
					}

					// TODO(jeff): support specs.Platform
					res, err := c.ContainerCreate(context.Background(), &cfg, &hostCfg, &netCfg, nil, containerName)
					if err != nil {
						wr.CloseWithError(err)
					}
					fmt.Fprintf(wr, "%s\n", res.ID)
					for _, warn := range res.Warnings {
						fmt.Fprintf(wr, "%s\n", warn)
					}
					lastContainerID = res.ID

				} else {
					w.Write([]byte("error: missing image to run"))
				}
			case "start":
				if containerID, ok := findContainerId(args); ok {
					if okOrErr(wr, c.ContainerStart(context.Background(), containerID, types.ContainerStartOptions{})) {
						return
					}
				} else {
					w.Write([]byte("error: missing image to run"))
				}
			case "stop":
				if containerID, ok := findContainerId(args); ok {
					if okOrErr(wr, c.ContainerStop(context.Background(), containerID, nil)) {
						return
					}
				} else {
					w.Write([]byte("error: missing image to run"))
				}
			case "kill":
				signal := "SIGKILL"
				if len(args) > 2 {
					signal = args[2]
				}
				if containerID, ok := findContainerId(args); ok {
					if okOrErr(wr, c.ContainerKill(context.Background(), containerID, signal)) {
						return
					}
				} else {
					w.Write([]byte("error: missing image to run"))
				}
			case "pause":
				if containerID, ok := findContainerId(args); ok {
					if okOrErr(wr, c.ContainerPause(context.Background(), containerID)) {
						return
					}
				} else {
					w.Write([]byte("error: missing image to run"))
				}
			case "unpause":
				if containerID, ok := findContainerId(args); ok {
					if okOrErr(wr, c.ContainerUnpause(context.Background(), containerID)) {
						return
					}
				} else {
					w.Write([]byte("error: missing image to run"))
				}
			case "restart":
				if containerID, ok := findContainerId(args); ok {
					if okOrErr(wr, c.ContainerRestart(context.Background(), containerID, nil)) {
						return
					}
				} else {
					w.Write([]byte("error: missing image to run"))
				}
			case "prune":
				var res types.ContainersPruneReport
				res, err = c.ContainersPrune(context.Background(), filters.NewArgs())
				if err != nil {
					goto finished
				} else {
					fmt.Fprintf(w, "space_reclaimed %d\n", res.SpaceReclaimed)
					for _, ctnr := range res.ContainersDeleted {
						fmt.Fprintf(w, "deleted %s\n", ctnr)
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
