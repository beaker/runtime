package runtime

import (
	"context"
	"io"
	"time"

	"github.com/beaker/runtime/logging"
)

//go:generate mockery --dir . --all

// PullPolicy enumerates options for pulling images.
type PullPolicy string

const (
	// PullAlways indicates an image should always be pulled even if already present.
	PullAlways PullPolicy = "always"

	// PullIfMissing pulls an image only if it doesn't exist locally. The local
	// image will not be updated if the remote version has changed.
	PullIfMissing PullPolicy = "missing"

	// PullNever validates that an image exists locally. It does not attempt to
	// pull the remote version, even if the image is missing.
	PullNever PullPolicy = "never"
)

// Runtime abstracts the specifics of interacting with the underlying container
// runtime (e.g. Docker) for execution.
type Runtime interface {
	io.Closer

	PullImage(ctx context.Context, image *DockerImage, policy PullPolicy, quiet bool) error
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

	// Memory is a hard limit on the amount of memory a container can use.
	// Expressed as a number of bytes.
	Memory int64

	// SharedMemory is the size of /dev/shm in bytes.
	SharedMemory int64

	// CPUCount is a hard limit on the number of CPUs a container can use.
	CPUCount float64

	// CPUShares limit the amount of CPU a container can use relative to other containers.
	// Each container defaults to 1024 shares. During periods of CPU contention, CPU is limited
	// in proportion to the number of shares a container has e.g. a container with 2048
	// shares can use twice as much CPU as one with 1024 shares.
	//
	// CPUShares are ignored in the Kubernetes runtime.
	// CPUShares take precedence over CPUCount in the Docker and CRI runtimes.
	CPUShares int64

	// GPUs assigned to the container as IDs or indices.
	GPUs []string

	// (optional) User that will run commands inside the container. Also supports "user:group".
	// If not provided, the container is run as root.
	User string

	// (optional) WorkingDir where the command will be launched.
	WorkingDir string
}

// IsEvictable returns true if a container is evictable. Evictable containers are the first to be killed
// during periods of memory contention.
func (o *ContainerOpts) IsEvictable() bool {
	return o.Memory == 0 && o.CPUCount == 0 && o.CPUShares == 0 && len(o.GPUs) == 0
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
