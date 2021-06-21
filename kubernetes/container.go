package kubernetes

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/beaker/runtime"
	"github.com/beaker/runtime/logging"
)

// Container wraps a Kubernetes container in the common runtime container interface.
// Note that standalone containers do not exist in Kubernetes; all containers
// are wrapped in a pod.
type Container struct {
	client *kubernetes.Clientset

	namespace     string
	podName       string
	containerName string

	// Underlying runtime and container
	runtimeLock sync.Mutex
	runtime     runtime.Runtime
	container   runtime.Container
}

// Name returns the container's unique ID.
func (c *Container) Name() string {
	return c.podName
}

// Start does nothing on Kubernetes since containers are automatically started on creation.
func (c *Container) Start(ctx context.Context) error {
	return nil
}

// Info returns a container's details.
func (c *Container) Info(ctx context.Context) (*runtime.ContainerInfo, error) {
	pod, err := c.client.CoreV1().Pods(c.namespace).Get(ctx, c.podName, metav1.GetOptions{})
	if k8serror.IsNotFound(err) {
		return nil, runtime.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("getting pod: %w", err)
	}

	var state corev1.ContainerState
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == c.containerName {
			state = status.State
			break
		}
	}

	info := &runtime.ContainerInfo{
		Labels:    pod.Annotations,
		CreatedAt: pod.CreationTimestamp.Time,
	}

	for _, ctr := range pod.Spec.Containers {
		if ctr.Name != c.containerName {
			continue
		}
		info.CPUCount = float64(ctr.Resources.Limits.Cpu().MilliValue()) / 1000
		info.Memory = ctr.Resources.Limits.Memory().Value()
		break
	}

	switch {
	case state.Waiting != nil:
		info.Status = runtime.StatusRunning
		info.Message = state.Waiting.Reason
		if state.Waiting.Message != "" {
			info.Message += ": " + state.Waiting.Message
		}
	case state.Running != nil:
		info.StartedAt = state.Running.StartedAt.Time

		info.Status = runtime.StatusRunning
	case state.Terminated != nil:
		info.StartedAt = state.Terminated.StartedAt.Time
		info.EndedAt = state.Terminated.FinishedAt.Time

		info.Status = runtime.StatusExited
		info.Message = state.Terminated.Reason
		if state.Terminated.Message != "" {
			info.Message += ": " + state.Terminated.Message
		}
		exitCode := int(state.Terminated.ExitCode)
		info.ExitCode = &exitCode
	case pod.Status.Phase == "Failed":
		// Fall back to pod status if container status is not available. This captures some pod-level
		// failures such as eviction due to out of memory.
		info.Status = runtime.StatusExited
		info.Message = pod.Status.Reason
		if pod.Status.Message != "" {
			info.Message += ": " + pod.Status.Message
		}
	default:
		// If no state is specified, assume that the container is running. From the K8s reference:
		// > ContainerState holds a possible state of container. Only one of its members may be specified.
		// > If none of them is specified, the default one is ContainerStateWaiting.
		// https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.15/#containerstate-v1-core
		info.Status = runtime.StatusRunning
		log.WithFields(log.Fields{
			"phase":      pod.Status.Phase,
			"conditions": pod.Status.Conditions,
			"message":    pod.Status.Message,
			"reason":     pod.Status.Reason,
		}).Debug("No container state found; assumed 'running'")
	}
	return info, nil
}

// Logs returns logging.LogReader which can be used to read log messages
// starting at the given time (inclusive). Set time to zero to read the full log.
func (c *Container) Logs(ctx context.Context, since time.Time) (logging.LogReader, error) {
	// It's more efficient and reliable to pull logs from CRI than to use the
	// k8s API. This is possible because we can guarantee we're on the same host.
	if err := c.resolveContainer(ctx); err != nil {
		return nil, err
	}
	return c.container.Logs(ctx, since)
}

// Stop sends a SIGTERM to a container to instruct it to exit. If a timeout is
// provided and elapses, the container is forcibly stopped with SIGKILL.
func (c *Container) Stop(ctx context.Context, timeout *time.Duration) error {
	// The k8s API offers no way to stop a container or pod without removal. Use CRI.
	if err := c.resolveContainer(ctx); err != nil {
		return err
	}
	return c.container.Stop(ctx, timeout)
}

// Remove removes a pod with no grace period.
func (c *Container) Remove(ctx context.Context) error {
	var zero int64
	opts := metav1.DeleteOptions{GracePeriodSeconds: &zero}
	if err := c.client.CoreV1().Pods(c.namespace).Delete(ctx, c.podName, opts); err != nil {
		if k8serror.IsNotFound(err) {
			return runtime.ErrNotFound
		}
		return fmt.Errorf("deleting pod: %w", err)
	}

	pdbs := c.client.PolicyV1beta1().PodDisruptionBudgets(c.namespace)
	if err := pdbs.Delete(ctx, c.podName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("deleting pod disruption budget: %w", err)
	}

	return nil
}

// Stats scrapes stats information about the container and returns it.
// This includes information about memory, cpu, network and block IO.
func (c *Container) Stats(ctx context.Context) (*runtime.ContainerStats, error) {
	if err := c.resolveContainer(ctx); err != nil {
		return nil, err
	}
	return c.container.Stats(ctx)
}

// containerWrapper is implemented by lower-level runtimes to directly access
// containers without going through the public interface.
type containerWrapper interface {
	Container(id string) runtime.Container
}

// resolveContainer finds the container using Kubernetes' underlying runtime.
//
// If no matching pod exists, ErrNotFound is returned.
// If the pod exists but the container doesn't, ErrNotStarted is returned.
func (c *Container) resolveContainer(ctx context.Context) error {
	// We lock optimistically here assuming that the container will usually be
	// populated. It's OK if two callers race to get the container. One will
	// win and the losing container will be discarded.
	c.runtimeLock.Lock()
	container := c.container
	c.runtimeLock.Unlock()
	if container != nil {
		return nil
	}

	pods := c.client.CoreV1().Pods(c.namespace)
	pod, err := pods.Get(ctx, c.podName, metav1.GetOptions{})
	if err != nil {
		if k8serror.IsNotFound(err) {
			return runtime.ErrNotFound
		}
		return fmt.Errorf("finding pod: %w", err)
	}

	var containerID string
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == c.containerName {
			parts := strings.SplitN(c.containerName, "://", 2)
			if len(parts) < 2 {
				containerID = parts[0] // Empty string or unusual format.
			} else {
				containerID = parts[1] // URI form "containerd://<id>"
			}
			break
		}
	}

	if containerID == "" {
		return runtime.ErrNotStarted
	}

	c.runtimeLock.Lock()
	defer c.runtimeLock.Unlock()

	wrapper, ok := c.runtime.(containerWrapper)
	if !ok {
		return fmt.Errorf("underlying runtime doesn't support direct container access (%w)", runtime.ErrNotImplemented)
	}
	c.container = wrapper.Container(containerID)
	return nil
}
