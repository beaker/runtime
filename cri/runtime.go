package cri

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"

	"github.com/beaker/runtime"
	"github.com/beaker/unique"
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
	conn   *grpc.ClientConn
	client cri.RuntimeServiceClient
}

// NewRuntime creates a new cri-backed Runtime.
func NewRuntime(ctx context.Context, address string) (*Runtime, error) {
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("cri: couldn't connect to %q: %w", address, err)
	}

	return &Runtime{
		conn:   conn,
		client: cri.NewRuntimeServiceClient(conn),
	}, nil
}

// Close implements the io.Closer interface.
func (r *Runtime) Close() error {
	return r.conn.Close()
}

// PullImage pulls a Docker image and prints progress to stdout unless quiet is set.
func (r *Runtime) PullImage(
	ctx context.Context,
	image *runtime.DockerImage,
	policy runtime.PullPolicy,
	quiet bool,
) error {
	return runtime.ErrNotImplemented
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

	// TODO: Set UID and GID via LinuxContainerSecurityContext.
	// TODO: Apply a namespace via LinuxContainerSecurityContext.
	cconf := &cri.ContainerConfig{
		Metadata:   &cri.ContainerMetadata{Name: opts.Name},
		Image:      &cri.ImageSpec{Image: opts.Image.Tag},
		Command:    opts.Command,
		Args:       opts.Arguments,
		WorkingDir: opts.WorkingDir,
		Linux:      &cri.LinuxContainerConfig{},
	}

	// Generate a random name if none was provided.
	if cconf.Metadata.Name == "" {
		cconf.Metadata.Name = unique.NewID().String()
	}

	if opts.Interactive {
		cconf.Stdin = true
		cconf.Tty = true
	}

	cconf.Labels = make(map[string]string, len(opts.Labels)+1)
	cconf.Labels[managedLabel] = "true"
	for k, v := range opts.Labels {
		cconf.Labels[k] = v
	}

	for k, v := range opts.Env {
		cconf.Envs = append(cconf.Envs, &cri.KeyValue{Key: k, Value: v})
	}

	cconf.Mounts = make([]*cri.Mount, len(opts.Mounts))
	for i, m := range opts.Mounts {
		source, err := filepath.Abs(m.HostPath)
		if err != nil {
			return nil, fmt.Errorf("translating to absolute path: %w", err)
		}
		cconf.Mounts[i] = &cri.Mount{
			HostPath:      source,
			ContainerPath: m.ContainerPath,
			Readonly:      m.ReadOnly,
		}
	}

	// Set hardware limits.
	cconf.Linux.Resources = &cri.LinuxContainerResources{}
	if mem := opts.Memory; mem != 0 {
		const minimum = 4 * 1024 * 1024
		if mem < minimum {
			mem = minimum
		}
		cconf.Linux.Resources.MemoryLimitInBytes = mem
	}
	if opts.CPUShares != 0 {
		cconf.Linux.Resources.CpuShares = opts.CPUShares
	} else if opts.CPUCount != 0 {
		// CPU period and quota are in microseconds.
		cconf.Linux.Resources.CpuPeriod = 100000
		cconf.Linux.Resources.CpuQuota = int64(opts.CPUCount * 100000)
	}
	if len(opts.GPUs) != 0 {
		// TODO: Mount GPU device. Compare whatever GKE does under the hood.
		return nil, fmt.Errorf("GPUs are not yet supported on CRI (%w)", runtime.ErrNotImplemented)
	} else {
		// If there aren't any GPUs requested, explicitly set NVIDIA_VISIBLE_DEVICES to none.
		// If we don't do this, all of the hosts GPUs will be accessible, see:
		// https://github.com/allenai/beaker-service/issues/1416.
		cconf.Envs = append(cconf.Envs, &cri.KeyValue{Key: visibleDevicesEnv, Value: "none"})
	}
	if opts.IsEvictable() {
		cconf.Linux.Resources.OomScoreAdj = 1000
	}

	c, err := r.client.CreateContainer(ctx, &cri.CreateContainerRequest{Config: cconf})
	if err != nil {
		msg := err.Error()
		if i := strings.Index(msg, pathDneError); i != -1 {
			// Sanitize mounting errors for cleaner presentation.
			return nil, errors.New(msg[i:])
		}
		return nil, err
	}

	return r.Container(c.ContainerId), nil
}

// ListContainers enumerates all containers.
func (r *Runtime) ListContainers(ctx context.Context) ([]runtime.Container, error) {
	return nil, runtime.ErrNotImplemented
}

// Container creates an interface to an existing container.
func (r *Runtime) Container(id string) runtime.Container {
	return &Container{r.client, id}
}
