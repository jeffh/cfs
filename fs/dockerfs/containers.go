package dockerfs

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/go-connections/nat"
	"github.com/jeffh/cfs/ninep"
	"github.com/jeffh/cfs/ninep/kvp"
)

func containerFileContents(fileType string, inspect types.ContainerJSON) (string, error) {
	var content string
	switch fileType {
	case "containerStatus":
		content = inspect.State.Status
	case "containerPID":
		content = fmt.Sprintf("%d", inspect.State.Pid)
	case "containerStartedAt":
		content = inspect.State.StartedAt
	case "containerFinishedAt":
		content = inspect.State.FinishedAt
	case "containerHealthStatus":
		if inspect.State.Health != nil {
			content = inspect.State.Health.Status
		}
	case "containerHealthFailingStreak":
		if inspect.State.Health != nil {
			content = fmt.Sprintf("%d", inspect.State.Health.FailingStreak)
		}
	case "containerHealthLogs":
		if inspect.State.Health != nil {
			var logs []string
			for _, log := range inspect.State.Health.Log {
				if log.ExitCode == 0 {
					logs = append(logs, fmt.Sprintf("%s\tHEALTHY\t%s", log.Start.Format(time.RFC3339), log.Output))
				} else {
					logs = append(logs, fmt.Sprintf("%s\tUNHEALTHY\t%s", log.Start.Format(time.RFC3339), log.Output))
				}
			}
			content = strings.Join(logs, "\n")
		}
	case "containerImage":
		content = fmt.Sprintf("%s\n%s", inspect.Config.Image, inspect.Image)
	case "containerName":
		content = strings.TrimPrefix(inspect.Name, "/")
	case "containerRestartCount":
		content = fmt.Sprintf("%d", inspect.RestartCount)
	case "containerPlatform":
		content = inspect.Platform
	case "containerJSON":
		b, err := json.MarshalIndent(inspect, "", "  ")
		if err != nil {
			return "", err
		}
		content = string(b)
	case "containerLabels":
		var b strings.Builder
		for k, v := range inspect.Config.Labels {
			fmt.Fprintf(&b, "%s\n", kvp.KeyPair(k, v))
		}
		content = b.String()
	case "containerEnv":
		var b strings.Builder
		for _, env := range inspect.Config.Env {
			// Environment variables are already in KEY=value format
			fmt.Fprintf(&b, "%s\n", env)
		}
		content = b.String()
	case "containerPorts":
		var b strings.Builder
		// Group ports by container port
		type portBindings struct {
			Container nat.Port
			Host      []nat.PortBinding
		}
		portMap := make(map[string][]portBindings)
		for containerPort, p := range inspect.NetworkSettings.Ports {
			portMap[containerPort.Port()] = append(portMap[containerPort.Port()], portBindings{containerPort, p})
		}

		// Sort container ports for consistent output
		containerPorts := slices.Collect(maps.Keys(portMap))
		sort.Strings(containerPorts)

		// Build output
		pairsBuf := [4][2]string{}
		for _, containerPort := range containerPorts {
			bindings := portMap[containerPort]
			for _, binding := range bindings {
				pairs := pairsBuf[:0]
				pairs = append(pairs,
					[2]string{"container_proto", binding.Container.Proto()},
					[2]string{"container_port", binding.Container.Port()},
				)

				if len(binding.Host) > 0 {
					for _, h := range binding.Host {
						pairs = pairs[:2]
						if h.HostIP != "" {
							pairs = append(pairs, [2]string{"host_ip", h.HostIP})
						}
						pairs = append(pairs, [2]string{"host_port", h.HostPort})
						fmt.Fprintf(&b, "%s\n", kvp.NonEmptyKeyPairs(pairs))
					}
				} else {
					fmt.Fprintf(&b, "%s\n", kvp.NonEmptyKeyPairs(pairs))
				}
			}
		}
		content = b.String()
	case "containerMounts":
		var b strings.Builder
		for _, mount := range inspect.Mounts {
			pairs := [][2]string{
				{"type", string(mount.Type)},
				{"name", mount.Name},
				{"source", mount.Source},
				{"destination", mount.Destination},
				{"driver", mount.Driver},
				{"mode", mount.Mode},
				{"rw", strconv.FormatBool(mount.RW)},
				{"propagation", string(mount.Propagation)},
			}
			fmt.Fprintf(&b, "%s\n", kvp.NonEmptyKeyPairs(pairs))
		}
		content = b.String()
	}
	return content, nil
}

func handleContainerFile(f *Fs, fileType string, containerID string, flag ninep.OpenMode) (ninep.FileHandle, error) {
	if !flag.IsReadable() {
		return nil, ninep.ErrWriteNotAllowed
	}

	inspect, err := f.C.ContainerInspect(context.Background(), containerID)
	if err != nil {
		return nil, err
	}

	content, err := containerFileContents(fileType, inspect)
	if err != nil {
		return nil, err
	}
	return &ninep.ReadOnlyMemoryFileHandle{Contents: []byte(content)}, nil
}
