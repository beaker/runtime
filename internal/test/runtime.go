package test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/beaker/runtime"
	"github.com/beaker/runtime/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var busybox = &runtime.DockerImage{Tag: "docker.io/busybox:latest"}

func intPtr(i int) *int { return &i }

// awaitExit waits a short time for a container to exit and fails the test if it doesn't.
func awaitExit(container runtime.Container) (*runtime.ContainerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer cancel()
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-ticker.C:
			info, err := container.Info(ctx)
			if err != nil {
				return nil, err
			}
			if info.Status == runtime.StatusExited {
				return info, nil
			}
		}
	}
}

// RuntimeSuite implements a full test suite for a container runtime. Each
// implementation should invoke this suite as part of their tests.
//
// func TestRuntime(t *testing.T) {
//   rt := /* create a runtime */
//   suite.Run(t, NewRuntimeSuite(rt))
// }
type RuntimeSuite struct {
	suite.Suite

	ctx context.Context
	rt  runtime.Runtime
}

// NewRuntimeSuite creates a test suite for a specific runtime.
func NewRuntimeSuite(rt runtime.Runtime) *RuntimeSuite {
	return &RuntimeSuite{
		ctx: context.Background(),
		rt:  rt,
	}
}

// TestCreateInspect tests container creation and introspection.
func (s *RuntimeSuite) TestCreateInspect() {
	t, ctx := s.T(), s.ctx

	t.Run("Minimal", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		defer ctr.Remove(ctx)

		assert.NotZero(t, ctr.Name())

		info, err := ctr.Info(ctx)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{"beaker.org/managed": "true"}, info.Labels)
		assert.NotZero(t, info.CreatedAt)
		assert.Zero(t, info.StartedAt)
		assert.Zero(t, info.EndedAt)
		assert.Equal(t, runtime.StatusCreated, info.Status)
		assert.Zero(t, info.Message)
		assert.Nil(t, info.ExitCode)
		assert.Equal(t, int64(0), info.Memory)
		assert.Equal(t, 0.0, info.CPUCount)
	})

	t.Run("Full", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{
			Name:      "TestImage",
			Image:     busybox, // TODO: Find a way to test registry creds.
			Command:   []string{"/bin/sh", "-c"},
			Arguments: []string{"exit 1"},
			Env:       map[string]string{"PLANET": "Earth"},
			Labels:    map[string]string{"key": "value"},
			Mounts: []runtime.Mount{{
				HostPath:      "/tmp",
				ContainerPath: "/dummy",
				ReadOnly:      true,
			}},
			Memory:   4 * 1024 * 1024,
			CPUCount: 2,
		})
		require.NoError(t, err)
		defer ctr.Remove(ctx)

		assert.NotZero(t, ctr.Name())

		info, err := ctr.Info(ctx)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{
			"beaker.org/managed": "true",
			"key":                "value",
		}, info.Labels)
		assert.NotZero(t, info.CreatedAt)
		assert.Zero(t, info.StartedAt)
		assert.Zero(t, info.EndedAt)
		assert.Equal(t, runtime.StatusCreated, info.Status)
		assert.Zero(t, info.Message)
		assert.Nil(t, info.ExitCode)
		assert.Equal(t, int64(4*1024*1024), info.Memory)
		assert.Equal(t, 2.0, info.CPUCount)
	})

	t.Run("Running", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		defer ctr.Remove(ctx)

		require.NoError(t, ctr.Start(ctx))

		info, err := ctr.Info(ctx)
		require.NoError(t, err)
		assert.NotZero(t, info.CreatedAt)
		assert.True(t, info.StartedAt.After(info.CreatedAt), "Container can only start after creation.")
		assert.Zero(t, info.EndedAt, "Container should not have ended.")
		assert.Equal(t, runtime.StatusRunning, info.Status, "Container should have running status.")
		assert.Nil(t, info.ExitCode, "Container should not have exited.")
	})

	t.Run("Ended", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{
			Image:   busybox,
			Command: []string{"/bin/sh", "-c", "exit 1"},
		})
		require.NoError(t, err)
		defer ctr.Remove(ctx)

		require.NoError(t, ctr.Start(ctx))

		info, err := awaitExit(ctr)
		require.NoError(t, err)
		assert.NotZero(t, info.CreatedAt)
		assert.True(t, info.StartedAt.After(info.CreatedAt), "Container can only start after creation.")
		assert.True(t, info.EndedAt.After(info.StartedAt), "Container can only end after starting.")
		assert.Equal(t, runtime.StatusExited, info.Status, "Container should have exited status.")
		assert.Equal(t, intPtr(1), info.ExitCode, "Container should have an exit code.")
	})

	t.Run("NotFound", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		require.NoError(t, ctr.Remove(ctx))
		_, err = ctr.Info(ctx)
		assert.Equal(t, runtime.ErrNotFound, err)
	})
}

// TestListContainers validates container enumeration.
func (s *RuntimeSuite) TestListContainers() {
	t, ctx := s.T(), s.ctx

	t.Run("Empty", func(t *testing.T) {
		list, err := s.rt.ListContainers(ctx)
		require.NoError(t, err)
		assert.Empty(t, list)
	})

	t.Run("MultipleResults", func(t *testing.T) {
		ctr1, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		defer ctr1.Remove(ctx)
		ctr2, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		defer ctr2.Remove(ctx)
		ctr3, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		defer ctr3.Remove(ctx)

		list, err := s.rt.ListContainers(ctx)
		require.NoError(t, err)
		require.Len(t, list, 3)
		assert.ElementsMatch(t,
			[]string{ctr1.Name(), ctr2.Name(), ctr3.Name()},
			[]string{list[0].Name(), list[1].Name(), list[2].Name()})
	})

	t.Run("DeleteContainer", func(t *testing.T) {
		ctr1, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		ctr1.Remove(ctx)
		ctr2, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		defer ctr2.Remove(ctx)

		list, err := s.rt.ListContainers(ctx)
		require.NoError(t, err)
		require.Len(t, list, 1)
		assert.Equal(t, ctr2.Name(), list[0].Name())
	})
}

// TestContainerLogs validates terminal output from a container.
func (s *RuntimeSuite) TestContainerLogs() {
	t, ctx := s.T(), s.ctx

	t.Run("NoLogs", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{Image: busybox})
		require.NoError(t, err)
		defer ctr.Remove(ctx)
		require.NoError(t, ctr.Start(ctx))
		_, err = awaitExit(ctr)
		require.NoError(t, err)

		r, err := ctr.Logs(ctx, time.Time{})
		require.NoError(t, err)
		defer r.Close()

		line, err := r.ReadMessage()
		assert.Equal(t, io.EOF, err)
		assert.Nil(t, line)
	})

	t.Run("MultipleLines", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{
			Image:     busybox,
			Command:   []string{"sh", "-c"},
			Arguments: []string{"echo Foo; echo -n Bar"},
		})
		require.NoError(t, err)
		defer ctr.Remove(ctx)
		require.NoError(t, ctr.Start(ctx))
		_, err = awaitExit(ctr)
		require.NoError(t, err)

		r, err := ctr.Logs(ctx, time.Time{})
		require.NoError(t, err)
		defer r.Close()

		line1, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, logging.Stdout, line1.Stream)
		assert.NotZero(t, line1.Time)
		assert.Equal(t, "Foo\n", line1.Text)
		line2, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, logging.Stdout, line2.Stream)
		assert.False(t, line1.Time.After(line2.Time), "Lines must be ordered in time.")
		assert.Equal(t, "Bar", line2.Text)
		_, err = r.ReadMessage()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("Stderr", func(t *testing.T) {
		ctr, err := s.rt.CreateContainer(ctx, &runtime.ContainerOpts{
			Image:     busybox,
			Command:   []string{"sh", "-c"},
			Arguments: []string{">&2 echo Error!"},
		})
		require.NoError(t, err)
		defer ctr.Remove(ctx)
		require.NoError(t, ctr.Start(ctx))
		_, err = awaitExit(ctr)
		require.NoError(t, err)

		r, err := ctr.Logs(ctx, time.Time{})
		require.NoError(t, err)
		defer r.Close()

		line, err := r.ReadMessage()
		require.NoError(t, err)
		assert.Equal(t, logging.Stderr, line.Stream)
		assert.NotZero(t, line.Time, "Lines must be ordered in time.")
		assert.Equal(t, "Error!\n", line.Text)
		_, err = r.ReadMessage()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("LongLine", func(t *testing.T) {
		t.Skip("TODO: Test truncation or splitting of very long log lines.")
	})
}

// TestContainerStop validates different ways of terminating a container.
func (s *RuntimeSuite) TestContainerStop() {
	t, ctx := s.T(), s.ctx
	spinForever := &runtime.ContainerOpts{
		Image:     busybox,
		Command:   []string{"sh", "-c"},
		Arguments: []string{"while true; do echo $(date); sleep 1; done"},
	}

	t.Run("InstaKill", func(t *testing.T) {
		var zero time.Duration
		ctr, err := s.rt.CreateContainer(ctx, spinForever)
		require.NoError(t, err)
		defer ctr.Remove(ctx)
		require.NoError(t, ctr.Start(ctx))

		start := time.Now()
		require.NoError(t, ctr.Stop(ctx, &zero))
		end := time.Now()

		// This timing may be fragile if the runtime is slow. If so, change it to log a warning.
		assert.True(t, end.Sub(start) < 3*time.Second, "Container should exit quickly.")
	})

	t.Run("DelayedKill", func(t *testing.T) {
		delay := 5 * time.Second // This is really long for a test, but Docker is slow.
		ctr, err := s.rt.CreateContainer(ctx, spinForever)
		require.NoError(t, err)
		defer ctr.Remove(ctx)
		require.NoError(t, ctr.Start(ctx))

		start := time.Now()
		require.NoError(t, ctr.Stop(ctx, &delay))
		end := time.Now()
		assert.True(t, delay < end.Sub(start), "Container shouldn't end until time elapses.")
	})

	t.Run("GracefulExit", func(t *testing.T) {
		t.Skip("TODO: Test exiting a container in response to SIGTERM")
	})
}
