package cri

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/beaker/runtime"
	"github.com/beaker/runtime/logging"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	cri "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

// Container wraps a CRI container.
type Container struct {
	client cri.RuntimeServiceClient
	id     string
}

// Name returns the container's unique ID.
func (c *Container) Name() string {
	return c.id
}

// Start calls the entrypoint in a created container.
func (c *Container) Start(ctx context.Context) error {
	_, err := c.client.StartContainer(ctx, &cri.StartContainerRequest{ContainerId: c.id})
	return translateErr(err)
}

// Info returns a container's details.
func (c *Container) Info(ctx context.Context) (*runtime.ContainerInfo, error) {
	resp, err := c.client.ContainerStatus(ctx, &cri.ContainerStatusRequest{
		ContainerId: c.id,
		Verbose:     true,
	})
	if err != nil {
		return nil, translateErr(err)
	}

	result, err := containerInfo(resp.Status, resp.Info)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func containerInfo(
	status *cri.ContainerStatus,
	info map[string]string,
) (runtime.ContainerInfo, error) {
	var result runtime.ContainerInfo
	result.Labels = status.Labels
	result.CreatedAt = time.Unix(0, status.CreatedAt)
	if status.StartedAt != 0 {
		result.CreatedAt = time.Unix(0, status.StartedAt)
	}
	if status.FinishedAt != 0 {
		result.EndedAt = time.Unix(0, status.FinishedAt)
	}

	switch status.State {
	case cri.ContainerState_CONTAINER_CREATED:
		result.Status = runtime.StatusCreated
	case cri.ContainerState_CONTAINER_RUNNING:
		result.Status = runtime.StatusRunning
	case cri.ContainerState_CONTAINER_EXITED:
		result.Status = runtime.StatusExited
		result.ExitCode = new(int)
		*result.ExitCode = int(status.ExitCode)
	}

	if jsonConfig, ok := info["info"]; ok {
		var jsonInfo struct {
			SandboxID string              `json:"sandboxID"`
			PID       int                 `json:"pid"`
			Config    cri.ContainerConfig `json:"config"`
		}

		if err := json.Unmarshal([]byte(jsonConfig), &jsonInfo); err != nil {
			return runtime.ContainerInfo{}, fmt.Errorf("cri: couldn't parse container config: %w", err)
		}

		res := jsonInfo.Config.Linux.GetResources()
		result.Memory = res.GetMemoryLimitInBytes()
		result.CPUCount = float64(res.GetCpuQuota()) / float64(res.GetCpuPeriod())
	}

	return result, nil
}

// Logs returns logging.LogReader which can be used to read log messages
// starting at the given time (inclusive). Set time to zero to read the full log.
func (c *Container) Logs(ctx context.Context, since time.Time) (logging.LogReader, error) {
	resp, err := c.client.ContainerStatus(ctx, &cri.ContainerStatusRequest{ContainerId: c.id})
	if err != nil {
		return nil, translateErr(err)
	}

	logPath := resp.GetStatus().GetLogPath()
	r, err := os.Open(logPath)
	if err != nil {
		return nil, fmt.Errorf("couldn't open log file %q: %w", logPath, err)
	}

	return NewLogReader(r, since), nil
}

// Stop sends a SIGTERM to a container to instruct it to exit. If a timeout is
// provided and elapses, the container is forcibly stopped with SIGKILL.
func (c *Container) Stop(ctx context.Context, timeout *time.Duration) error {
	_, err := c.client.StopContainer(ctx, &cri.StopContainerRequest{
		ContainerId: c.id,
		Timeout:     int64(timeout.Seconds()),
	})
	return translateErr(err)
}

// Remove kills and removes a container with no grace period.
func (c *Container) Remove(ctx context.Context) error {
	_, err := c.client.RemoveContainer(ctx, &cri.RemoveContainerRequest{ContainerId: c.id})
	return translateErr(err)
}

// Stats scrapes stats information about the container and returns it.
// This includes information about memory, cpu, network and block IO.
func (c *Container) Stats(ctx context.Context) (*runtime.ContainerStats, error) {
	return nil, runtime.ErrNotImplemented
}

func translateErr(err error) error {
	if err == nil {
		return nil
	}
	if status, ok := status.FromError(err); ok && status.Code() == codes.NotFound {
		return runtime.ErrNotFound
	}
	return err
}
