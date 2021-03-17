package docker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/beaker/unique"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"

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

// CreateContainer creates a new container. Call Start to run it.
func (r *Runtime) CreateContainer(
	ctx context.Context,
	opts *runtime.ContainerOpts,
) (runtime.Container, error) {
	if err := pull(ctx, r.client, opts.Image); err != nil {
		return nil, fmt.Errorf("pulling image: %w", err)
	}

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

func pull(ctx context.Context, client *client.Client, image *runtime.DockerImage) error {
	registryAuth, err := encodeRegistryAuth(image.Auth)
	if err != nil {
		return fmt.Errorf("encoding registry auth: %w", err)
	}

	// Start the pull operation. The pull operation is not complete until the reader has been drained.
	r, err := client.ImagePull(ctx, image.Tag, types.ImagePullOptions{RegistryAuth: registryAuth})
	if err != nil {
		return err
	}
	if _, err := io.Copy(ioutil.Discard, r); err != nil {
		return fmt.Errorf("draining reader: %w", err)
	}
	if err := r.Close(); err != nil {
		return err
	}
	return nil
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
