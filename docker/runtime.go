package docker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/beaker/unique"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"

	"github.com/beaker/runtime"
)

const (
	// This label is set on all containers that the runtime is responsible for.
	managedLabel = "beaker.org/managed"

	// This environment variable controls which GPU devices are exposed to the runtime.
	// If set, the runtime passes this environment variable to all containers that it creates.
	// Containers may not specify this environment variable.
	// e.g. "0", "0,1", "all", "GPU-0a5c0cf4-eb7d-4fdd-40ea-4ac6803659ab".
	visibleDevicesEnv = "NVIDIA_VISIBLE_DEVICES"
	pathDneError      = "path does not exist"
)

// Runtime wraps the Docker runtime in a common interface.
type Runtime struct {
	client *client.Client
}

// NewRuntime creates a new Docker-backed Runtime.
func NewRuntime() (*Runtime, error) {
	client, err := client.NewClientWithOpts(client.WithAPIVersionNegotiation(), client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &Runtime{client}, nil
}

// Close implements the io.Closer interface.
func (r *Runtime) Close() error {
	return r.client.Close()
}

// PullImage pulls a Docker image and prints progress to stdout unless quiet is set.
func (r *Runtime) PullImage(
	ctx context.Context,
	image *runtime.DockerImage,
	quiet bool,
) error {
	registryAuth, err := encodeRegistryAuth(image.Auth)
	if err != nil {
		return fmt.Errorf("encoding registry auth: %w", err)
	}

	// Start the pull operation. The pull operation is not complete until the reader has been drained.
	out, err := r.client.ImagePull(ctx, image.Tag, types.ImagePullOptions{RegistryAuth: registryAuth})
	if err != nil {
		return err
	}

	if quiet {
		_, err = io.Copy(ioutil.Discard, out)
	} else {
		err = jsonmessage.DisplayJSONMessagesStream(out, os.Stdout, os.Stdout.Fd(), true, nil)
	}
	if err != nil {
		r.Close()
		return err
	}
	return r.Close()
}

// CreateContainer creates a new container. Call Start to run it.
func (r *Runtime) CreateContainer(
	ctx context.Context,
	opts *runtime.ContainerOpts,
) (runtime.Container, error) {
	// Prevent collisions on protected variables and labels.
	if _, ok := opts.Env[visibleDevicesEnv]; ok {
		return nil, fmt.Errorf("forbidden environment variable: %s", visibleDevicesEnv)
	}
	if _, ok := opts.Labels[managedLabel]; ok {
		return nil, fmt.Errorf("forbidden label: %s", managedLabel)
	}

	cconf := &container.Config{
		Image:      opts.Image.Tag,
		Entrypoint: opts.Command,
		Cmd:        opts.Arguments,
		User:       opts.User,
		WorkingDir: opts.WorkingDir,
	}
	hconf := &container.HostConfig{}

	if opts.Interactive {
		cconf.OpenStdin = true
		cconf.AttachStdin = true
		cconf.AttachStdout = true
		cconf.AttachStderr = true
		cconf.Tty = true

		// Init inserts a tiny init-process into the container as the main process
		// and handles reaping of all processes when the container exits.
		// Details here: https://docs.docker.com/config/containers/multi-service_container
		init := true
		hconf.Init = &init
	}

	cconf.Labels = make(map[string]string, len(opts.Labels)+1)
	cconf.Labels[managedLabel] = "true"
	for k, v := range opts.Labels {
		cconf.Labels[k] = v
	}

	cconf.Env = make([]string, 0, len(opts.Env))
	for k, v := range opts.Env {
		cconf.Env = append(cconf.Env, k+"="+v)
	}

	hconf.Mounts = make([]mount.Mount, len(opts.Mounts))
	for i, m := range opts.Mounts {
		source, err := filepath.Abs(m.HostPath)
		if err != nil {
			return nil, fmt.Errorf("translating to absolute path: %w", err)
		}
		hconf.Mounts[i] = mount.Mount{
			Type:     mount.TypeBind,
			Source:   source,
			Target:   m.ContainerPath,
			ReadOnly: m.ReadOnly,
		}
	}

	// Set hardware limits.
	if mem := opts.Memory; mem != 0 {
		const minimum = 4 * 1024 * 1024
		if mem < minimum {
			mem = minimum
		}
		hconf.Resources.Memory = mem
	}
	if opts.CPUCount != 0 {
		hconf.Resources.NanoCPUs = int64(opts.CPUCount * 1000000000)
	}
	if len(opts.GPUs) != 0 {
		hconf.Resources.DeviceRequests = []container.DeviceRequest{{
			DeviceIDs:    opts.GPUs,
			Driver:       "nvidia",
			Capabilities: [][]string{{"gpu"}},
		}}
	} else {
		// If there aren't any GPUs requested, explicitly set NVIDIA_VISIBLE_DEVICES to none.
		// If we don't do this, all of the hosts GPUs will be accessible, see:
		// https://github.com/allenai/beaker-service/issues/1416.
		//
		// It's worth noting that we tried to use the `DeviceRequest` API above, both
		// by requesting no `DeviceIds` and by explicitly setting `Count` to `0`. Neither
		// route worked and we're not sure why, and there's little to no documentation about
		// this approach.
		//
		// In fact, NVIDIA's docs explicity mention the NVIDIA_VISIBLE_DEVICES mechanism,
		// but make no mention of `DeviceRequest`s. So for now this might very well be
		// the canonical solution, even if it feels a little brittle.
		// See: https://github.com/NVIDIA/nvidia-container-runtime
		cconf.Env = append(cconf.Env, fmt.Sprintf("%s=none", visibleDevicesEnv))
	}

	// Docker's auto-generated names frequently collide, so generate a random one.
	name := opts.Name
	if name == "" {
		name = unique.NewID().String()
	}

	c, err := r.client.ContainerCreate(ctx, cconf, hconf, nil, nil, name)
	if err != nil {
		msg := err.Error()
		if i := strings.Index(msg, pathDneError); i != -1 {
			// Sanitize mounting errors for cleaner presentation.
			return nil, errors.New(msg[i:])
		}
		return nil, err
	}

	return WrapContainer(r.client, c.ID), nil
}

// ListContainers enumerates all containers.
func (r *Runtime) ListContainers(ctx context.Context) ([]runtime.Container, error) {
	filters := filters.NewArgs()
	filters.Add("label", managedLabel)
	body, err := r.client.ContainerList(ctx, types.ContainerListOptions{
		Filters: filters,
		All:     true,
	})
	if err != nil {
		return nil, err
	}

	containers := make([]runtime.Container, len(body))
	for i, c := range body {
		containers[i] = WrapContainer(r.client, c.ID)
	}
	return containers, nil
}

func encodeRegistryAuth(auth *runtime.RegistryAuth) (string, error) {
	if auth == nil {
		return "", nil
	}

	authJSON, err := json.Marshal(types.AuthConfig{
		ServerAddress: auth.ServerAddress,
		Username:      auth.Username,
		Password:      auth.Password,
	})
	if err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(authJSON), nil
}
