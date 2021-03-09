package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	log "github.com/sirupsen/logrus"
	"golang.org/x/term"

	"github.com/beaker/runtime"
	"github.com/beaker/runtime/logging"
)

// Container wraps a Docker container in the common runtime interface.
type Container struct {
	client *client.Client
	id     string
}

// WrapContainer creates an interface to an existing container.
func WrapContainer(client *client.Client, id string) *Container {
	return &Container{client, id}
}

// Name returns the container's unique ID.
func (c *Container) Name() string {
	return c.id
}

// Start calls the entrypoint in a created container.
func (c *Container) Start(ctx context.Context) error {
	return c.client.ContainerStart(ctx, c.id, types.ContainerStartOptions{})
}

// Info returns a container's details.
func (c *Container) Info(ctx context.Context) (*runtime.ContainerInfo, error) {
	body, err := c.client.ContainerInspect(ctx, c.id)
	if err != nil {
		return nil, translateErr(err)
	}
	if body.State == nil {
		panic("container state should never be nil")
	}

	res := body.HostConfig.Resources
	info := runtime.ContainerInfo{
		Labels:   body.Config.Labels,
		CPUCount: float64(res.NanoCPUs) / 1000000000,
		Memory:   res.Memory,
	}

	if info.CreatedAt, err = parseTime(body.Created); err != nil {
		return nil, fmt.Errorf("create time: %w", err)
	}
	if info.StartedAt, err = parseTime(body.State.StartedAt); err != nil {
		return nil, fmt.Errorf("start time: %w", err)
	}
	if info.EndedAt, err = parseTime(body.State.FinishedAt); err != nil {
		return nil, fmt.Errorf("end time: %w", err)
	}

	// Translate container status. The logic here is based on Kubernetes.
	// At time of writing: "k8s.io/kubernetes/pkg/kubelet/dockershim"
	if body.State.Running {
		info.Status = runtime.StatusRunning
	} else if info.EndedAt.IsZero() {
		// Container never started. The container is started on creation so this case is unexpected.
		log.Warnf("Found unstarted container: %s", c.id)

		info.Status = runtime.StatusExited
		info.Message = body.State.Error
		if body.State.ExitCode != 0 {
			// Set end time since this container is dead.
			info.EndedAt = info.StartedAt
			info.Message = addContext(info.Message, "failed start")
		}
	} else { // Container ended.
		info.Status = runtime.StatusExited
		info.Message = body.State.Error
		info.ExitCode = &body.State.ExitCode
		if body.State.OOMKilled {
			info.Message = addContext(info.Message, "out of memory")
		}
	}

	return &info, nil
}

func addContext(message string, context string) string {
	if message == "" {
		return context
	}
	return context + ": " + message
}

// Logs returns logging.LogReader which can be used to read log messages
// starting at the given time (inclusive). Set time to zero to read the full log.
func (c *Container) Logs(ctx context.Context, since time.Time) (logging.LogReader, error) {
	var sinceStr string
	if !since.IsZero() {
		sinceStr = since.Format(time.RFC3339Nano)
	}

	r, err := c.client.ContainerLogs(ctx, c.id, types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Since:      sinceStr,
		Timestamps: true,
	})
	if err != nil {
		return nil, translateErr(err)
	}
	return NewLogReader(r), nil
}

func parseTime(s string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, s)
}

func translateErr(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "No such container") {
		return runtime.ErrNotFound
	}
	return err
}

// Stop sends a SIGTERM to a container to instruct it to exit. If a timeout is
// provided and elapses, the container is forcibly stopped with SIGKILL.
func (c *Container) Stop(ctx context.Context, timeout *time.Duration) error {
	err := c.client.ContainerStop(ctx, c.id, timeout)
	return translateErr(err)
}

// Remove kills and removes a container with no grace period.
func (c *Container) Remove(ctx context.Context) error {
	err := c.client.ContainerRemove(ctx, c.id, types.ContainerRemoveOptions{Force: true})
	return translateErr(err)
}

// stats handling largely inspired by docker CLI's stats handler. see:
// https://github.com/docker/cli/blob/968ce1ae4d45722c6ae70aa1dff6ee28d88e976a/cli/command/container/stats_helpers.go

// Stats scrapes stats information about the container and returns it.
// This includes information about memory, cpu, network and block IO.
func (c *Container) Stats(ctx context.Context) (*runtime.ContainerStats, error) {
	response, err := c.client.ContainerStats(ctx, c.id, false)
	if err != nil {
		return nil, translateErr(err)
	}

	dec := json.NewDecoder(response.Body)
	var stats *types.StatsJSON
	if err = dec.Decode(&stats); err != nil {
		return nil, fmt.Errorf("decoding stats failed: %w", err)
	}

	previousCPU := stats.PreCPUStats.CPUUsage.TotalUsage
	previousSystem := stats.PreCPUStats.SystemUsage
	blkRead, blkWrite := calculateBlockIO(stats.BlkioStats)
	netRx, netTx := calculateNetwork(stats.Networks)
	memLimit := float64(stats.MemoryStats.Limit)
	mem := calculateMemUsageUnixNoCache(stats.MemoryStats)
	s := runtime.ContainerStats{
		Time: time.Now(),
		Stats: map[runtime.StatType]float64{
			runtime.CPUUsagePercentStat:    calculateCPUPercentUnix(previousCPU, previousSystem, stats),
			runtime.MemoryUsagePercentStat: calculateMemPercentUnixNoCache(memLimit, mem),
			runtime.MemoryUsageBytesStat:   mem,
			runtime.NetworkRxBytesStat:     netRx,
			runtime.NetworkTxBytesStat:     netTx,
			runtime.BlockReadBytesStat:     float64(blkRead),
			runtime.BlockWriteBytesStat:    float64(blkWrite),
		},
	}
	return &s, nil
}

func calculateCPUPercentUnix(previousCPU, previousSystem uint64, v *types.StatsJSON) float64 {
	cpuPercent := 0.0
	// calculate the change for the cpu usage of the container in between readings
	cpuDelta := float64(v.CPUStats.CPUUsage.TotalUsage) - float64(previousCPU)
	// calculate the change for the entire system between readings
	systemDelta := float64(v.CPUStats.SystemUsage) - float64(previousSystem)

	onlineCPUs := float64(len(v.CPUStats.CPUUsage.PercpuUsage))
	if systemDelta > 0.0 && cpuDelta > 0.0 {
		cpuPercent = (cpuDelta / systemDelta) * onlineCPUs * 100.0
	}
	return cpuPercent
}

func calculateBlockIO(blkio types.BlkioStats) (uint64, uint64) {
	var blkRead, blkWrite uint64
	for _, bioEntry := range blkio.IoServiceBytesRecursive {
		if len(bioEntry.Op) == 0 {
			continue
		}
		switch bioEntry.Op[0] {
		case 'r', 'R':
			blkRead = blkRead + bioEntry.Value
		case 'w', 'W':
			blkWrite = blkWrite + bioEntry.Value
		}
	}
	return blkRead, blkWrite
}

func calculateNetwork(network map[string]types.NetworkStats) (float64, float64) {
	var rx, tx float64

	for _, v := range network {
		rx += float64(v.RxBytes)
		tx += float64(v.TxBytes)
	}
	return rx, tx
}

// calculateMemUsageUnixNoCache calculate memory usage of the container.
// Page cache is intentionally excluded to avoid misinterpretation of the output.
func calculateMemUsageUnixNoCache(mem types.MemoryStats) float64 {
	return float64(mem.Usage - mem.Stats["cache"])
}

func calculateMemPercentUnixNoCache(limit float64, usedNoCache float64) float64 {
	// MemoryStats.Limit will never be 0 unless the container is not running and we haven't
	// got any data from cgroup
	if limit != 0 {
		return usedNoCache / limit * 100.0
	}
	return 0
}

// Attach connects to a created container with an interactive prompt.
// This is a borrowed and cleaned up version of the Docker CLI implementation.
func (c *Container) Attach(ctx context.Context) error {
	const tty = true // TODO: Detect or config param to set TTY.

	resp, err := c.client.ContainerAttach(ctx, c.id, types.ContainerAttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
	if err != nil {
		return err
	}
	defer resp.Close()

	if err := c.Start(ctx); err != nil {
		return err
	}

	if tty {
		if err := c.resizeTTY(ctx); err != nil {
			return fmt.Errorf("couldn't set terminal dimensions: %w", err)
		}
		c.monitorTTYSize(ctx)
	}

	resultC, errC := c.client.ContainerWait(ctx, c.id, "")
	if err := streamIO(ctx, resp, tty); err != nil {
		return err
	}

	// The user has exited the shell. Wait for the container to end to allow
	// time to clean up background processes.
	select {
	case result := <-resultC:
		if result.Error != nil {
			return errors.New(result.Error.Message)
		}
		if result.StatusCode != 0 {
			return fmt.Errorf("exited with code %d", result.StatusCode)
		}
		return nil

	case waitErr := <-errC:
		return waitErr
	}
}

// monitorTTYSize monitors the outer shell and resizes the container's TTY to match.
// https://github.com/docker/cli/blob/fff164c22e8dc904291fecb62307312fd4ca153e/cli/command/container/tty.go#L71
func (c *Container) monitorTTYSize(ctx context.Context) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGWINCH)

	// This routine leaks, but that's OK for now since the process will likely
	// end shortly after the attached shell exits.
	go func() {
		for range sigchan {
			c.resizeTTY(ctx)
		}
	}()
}

func (c *Container) resizeTTY(ctx context.Context) error {
	h, w, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return err
	}
	return c.client.ContainerResize(ctx, c.id, types.ResizeOptions{Height: uint(h), Width: uint(w)})
}

// streamIO proxies STDIN/STDOUT for a hijacked TCP connection.
func streamIO(ctx context.Context, resp types.HijackedResponse, tty bool) error {
	// Set input terminal to raw mode so keystrokes are sent directly.
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return fmt.Errorf("unable to set up input stream: %w", err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	// Proxy input.
	go func() {
		io.Copy(resp.Conn, os.Stdin)
		_ = resp.CloseWrite()
	}()

	outputDone := make(chan error, 1)
	go func() {
		var err error
		if tty {
			_, err = io.Copy(os.Stdout, resp.Reader)
		} else {
			// TODO: Is this needed?
			_, err = stdcopy.StdCopy(os.Stdout, os.Stderr, resp.Reader)
		}
		outputDone <- err
	}()

	// Ensure terminal output is cleared on restore.
	defer func() { fmt.Println() }()

	select {
	case err := <-outputDone:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
