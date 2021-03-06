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
	"golang.org/x/term"

	"github.com/beaker/runtime"
	"github.com/beaker/runtime/logging"
)

// Container wraps a Docker container in the common runtime interface.
type Container struct {
	client *client.Client
	id     string
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
	switch {
	case body.State.Running:
		info.Status = runtime.StatusRunning

	case !info.EndedAt.IsZero():
		// Container ended.
		info.Status = runtime.StatusExited
		info.Message = body.State.Error
		info.ExitCode = &body.State.ExitCode
		if body.State.OOMKilled {
			info.Message = addContext(info.Message, "out of memory")
		}

	case body.State.ExitCode != 0:
		// Container failed to start. It's dead.
		info.Status = runtime.StatusExited
		info.EndedAt = info.StartedAt
		info.Message = addContext(body.State.Error, "failed start")

	default:
		// Container hasn't started yet.
		info.Status = runtime.StatusCreated
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

// Attach hijacks the IO streams of a container.
// This must be called before the container is started.
func (c *Container) Attach(ctx context.Context) (types.HijackedResponse, error) {
	return c.client.ContainerAttach(ctx, c.id, types.ContainerAttachOptions{
		Stream: true,
		Stdin:  true,
		Stdout: true,
		Stderr: true,
	})
}

// Stream connects to a container with an interactive prompt.
// Use Attach to get the hijacked response.
// This must be called after the container is started.
// This is a borrowed and cleaned up version of the Docker CLI implementation.
func (c *Container) Stream(ctx context.Context, resp types.HijackedResponse) error {
	const tty = true // TODO: Detect or config param to set TTY.

	if tty {
		c.monitorTTYSize(ctx, "")
	}

	resultC, errC := c.client.ContainerWait(ctx, c.id, "")
	if err := streamIO(ctx, resp, tty); err != nil {
		return err
	}

	// Resize the TTY when reattaching so that the prompt shows up.
	if tty {
		c.resizeTTY(ctx, "")
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
// Optionally takes an execution ID to resize. If omitted, the root TTY is resized.
func (c *Container) monitorTTYSize(ctx context.Context, exec string) {
	// The Docker CLI includes a few retries for the initial resize to give the
	// process time to start. Duplicating Docker's retry logic here.
	if err := c.resizeTTY(ctx, exec); err != nil {
		go func() {
			var err error
			for retry := 0; retry < 5; retry++ {
				time.Sleep(10 * time.Millisecond)
				if err = c.resizeTTY(ctx, exec); err == nil {
					break
				}
			}
			if err != nil {
				fmt.Fprintln(os.Stderr, "failed to resize tty, using default size")
			}
		}()
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGWINCH)

	// This routine leaks, but that's OK for now since the process will likely
	// end shortly after the attached shell exits.
	go func() {
		for range sigchan {
			c.resizeTTY(ctx, exec)
		}
	}()
}

func (c *Container) resizeTTY(ctx context.Context, exec string) error {
	w, h, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return err
	}
	opts := types.ResizeOptions{Height: uint(h), Width: uint(w)}
	if exec != "" {
		return c.client.ContainerExecResize(ctx, exec, opts)
	} else {
		return c.client.ContainerResize(ctx, c.id, opts)
	}
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

type ExecOpts struct {
	// (required) Command and arguments.
	Command []string

	// (optional) Environment variables.
	Env map[string]string

	// (optional) User that will run the command.
	// Defaults to the user of the container's root process.
	User string

	// (optional) WorkingDir where the command will be launched.
	// Defaults to the container's working dir.
	WorkingDir string
}

func (c *Container) Exec(ctx context.Context, opts *ExecOpts) error {
	const tty = true // TODO: Detect or config param to set TTY.

	env := make([]string, 0, len(opts.Env))
	for k, v := range opts.Env {
		env = append(env, k+"="+v)
	}

	exec, err := c.client.ContainerExecCreate(ctx, c.id, types.ExecConfig{
		User:         opts.User,
		Tty:          tty,
		AttachStdin:  true,
		AttachStderr: true,
		AttachStdout: true,
		Env:          env,
		WorkingDir:   opts.WorkingDir,
		Cmd:          opts.Command,
	})
	if err != nil {
		return err
	}

	resp, err := c.client.ContainerExecAttach(ctx, exec.ID, types.ExecStartCheck{
		Tty: tty,
	})
	if err != nil {
		return err
	}
	defer resp.Close()

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		errCh <- func() error {
			return streamIO(ctx, resp, tty)
		}()
	}()

	if tty {
		c.monitorTTYSize(ctx, exec.ID)
	}

	if err := <-errCh; err != nil {
		return err
	}

	result, err := c.client.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("exited with code %d", result.ExitCode)
	}
	return nil
}
