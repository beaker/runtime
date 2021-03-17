package runtime

import (
	"context"
	"io"
	"time"

	"github.com/beaker/runtime/logging"
)

//go:generate mockery --dir . --all

// Runtime abstracts the specifics of interacting with the underlying container
// runtime (e.g. Docker) for execution.
type Runtime interface {
	io.Closer

	CreateContainer(ctx context.Context, opts *ContainerOpts) (Container, error)
	ListContainers(ctx context.Context) ([]Container, error)
}

// ContainerOpts allows a caller to specify options during container creation.
type ContainerOpts struct {
	// (optional) Name to give the container; randomly generated if absent.
	Name string

	Image     *DockerImage
	Command   []string
	Arguments []string
	Env       map[string]string
	Labels    map[string]string
	Mounts    []Mount

	// Attach STDIN/STDOUT/STDERR and shell into the container.
	Interactive bool

	// Resource limits
	Memory   int64 // In bytes
	CPUCount float64
	GPUs     []string // IDs or indices.

	// (optional) User that will run commands inside the container. Also supports "user:group".
	// If not provided, the container is run as root.
	User string

	// (optional) WorkingDir where the command will be launched.
	WorkingDir string
}

// DockerImage specifies a Docker-based container image.
type DockerImage struct {
	// (required) Tag is a docker image refspec, as a tag or resolvable image hash.
	Tag string

	// (optional) Auth contains credentials for private registry access.
	Auth *RegistryAuth
}

// RegistryAuth describes credentials for private Docker registry access.
type RegistryAuth struct {
	ServerAddress string
	Username      string
	Password      string
}

// Mount describes a file or directory mounted into a container.
type Mount struct {
	HostPath      string
	ContainerPath string
	ReadOnly      bool
}

// Container is a containerized process.
type Container interface {
	Name() string
	Start(ctx context.Context) error
	Info(ctx context.Context) (*ContainerInfo, error)
	Logs(ctx context.Context, since time.Time) (logging.LogReader, error)
	Stats(ctx context.Context) (*ContainerStats, error)
	Stop(ctx context.Context, timeout *time.Duration) error
	Remove(ctx context.Context) error
}

// ContainerInfo describes a container's details.
type ContainerInfo struct {
	Labels map[string]string

	CreatedAt time.Time
	StartedAt time.Time
	EndedAt   time.Time

	Status   ContainerStatus
	Message  string
	ExitCode *int

	// Resource limits
	Memory   int64 // In bytes
	CPUCount float64
	// TODO: Add GPUs so caller doesn't have to parse labels.
}

// ContainerStatus describes the runtime status of a containerized process.
type ContainerStatus int

const (
	// StatusCreated indicates a container has been created, but not started.
	StatusCreated ContainerStatus = iota

	// StatusRunning indicates a container is currently running.
	StatusRunning

	// StatusExited indicates a container exited.
	StatusExited
)

// String converts the container status to a human-readable string, useful for diagnostics.
func (s ContainerStatus) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusExited:
		return "exited"
	default:
		panic("invalid container status")
	}
}

// ContainerStats provides point-in-time usage statistics for system resources.
type ContainerStats struct {
	// Time marks the system time at which stats were sampled.
	Time time.Time

	// Stats describes all tracked container statistics, keyed by type. Not all
	// keys are guaranteed to be present.
	Stats map[StatType]float64
}

// A StatType is an enumerated container statistic.
type StatType string

const (
	// CPUUsagePercentStat counts CPU usage as a percentage of the container's
	// limit. If the container has no limit, the percentage is relative to total
	// CPU capacity of the host.
	CPUUsagePercentStat = StatType("CPUUsagePercent")

	// MemoryUsageBytesStat counts memory usage in absolute bytes.
	MemoryUsageBytesStat = StatType("MemoryUsageBytes")

	// MemoryUsagePercentStat counts memory usage as a percentage of the
	// container's limit. If the container has no limit, the percentage is
	// relative to total available memory on the host.
	MemoryUsagePercentStat = StatType("MemoryUsagePercent")

	// NetworkRxBytesStat counts total bytes received over the network.
	NetworkRxBytesStat = StatType("NetworkRxBytes")

	// NetworkTxBytesStat counts total bytes sent over the network.
	NetworkTxBytesStat = StatType("NetworkTxBytes")

	// BlockReadBytesStat counts total bytes read from block devices.
	BlockReadBytesStat = StatType("BlockReadBytes")

	// BlockWriteBytesStat counts total bytes written to block devices.
	BlockWriteBytesStat = StatType("BlockWriteBytes")
)
