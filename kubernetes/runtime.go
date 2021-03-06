package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // Google Cloud Platform auth plugin for out of cluster authentication.
	"k8s.io/client-go/rest"

	"github.com/beaker/runtime"
	"github.com/beaker/runtime/cri"
)

const (
	containerName         = "task"
	nodeLabel             = "beaker.org/node"
	sharedMemoryVolume    = "shared-memory"
	sharedMemoryMountPath = "/dev/shm"
)

const gpuResource = corev1.ResourceName("nvidia.com/gpu")

// Valid label values must be 63 characters or less and must be empty or begin
// and end with an alphanumeric character ([a-z0-9A-Z]) with dashes (-),
// underscores (_), dots (.), and alphanumerics between.
//
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
var labelRegex = regexp.MustCompile("^([a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?)?$")

// Runtime wraps the Kubernetes runtime in a common interface.
// The runtime must be used from within Kubernetes cluster.
// All methods are scoped to the current node.
type Runtime struct {
	client    *kubernetes.Clientset
	runtime   runtime.Runtime
	namespace string
	node      string
}

// NewInClusterRuntime creates a new Kubernetes-backed Runtime from a process running
// in a Kubernetes cluster. The runtime is scoped to the current node.
func NewInClusterRuntime(ctx context.Context, namespace string, node string) (*Runtime, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("getting kubeconfig: %w", err)
	}
	restConfig.Timeout = 60 * time.Second

	client, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("creating kubernetes client: %w", err)
	}

	if _, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{}); err != nil {
		return nil, fmt.Errorf("getting namespace %s: %w", namespace, err)
	}

	// TODO: This hard-coded to match our current GKE config, but we should pass
	// the runtime via configuration params instead.
	criRuntime, err := cri.NewRuntime(ctx, "unix:///run/containerd/containerd.sock")
	if err != nil {
		return nil, err
	}

	return &Runtime{
		client:    client,
		runtime:   criRuntime,
		namespace: namespace,
		node:      node,
	}, nil
}

// Close implements the io.Closer interface.
func (r *Runtime) Close() error {
	return nil
}

// PullImage is a no-op on Kubernetes; images are pulled implicitly on container creation.
func (r *Runtime) PullImage(
	ctx context.Context,
	image *runtime.DockerImage,
	policy runtime.PullPolicy,
	quiet bool,
) error {
	return nil
}

// CreateContainer creates a new container. The container is started implicitly.
func (r *Runtime) CreateContainer(
	ctx context.Context,
	opts *runtime.ContainerOpts,
) (runtime.Container, error) {
	if opts.Interactive {
		return nil, errors.New("interactive shells are not implemented for Kubernetes")
	}
	if opts.User != "" {
		return nil, errors.New("users configuration is not implemented for Kubernetes")
	}
	if opts.WorkingDir != "" {
		return nil, errors.New("working directory configuration is not implemented for Kubernetes")
	}

	labels := map[string]string{nodeLabel: r.node}
	annos := make(map[string]string, len(opts.Labels))
	for k, v := range opts.Labels {
		annos[k] = v

		// We copy annotations to labels for convenience. Labels can be  used as
		// query filters in kubectl while annotations can't.
		if k != nodeLabel && labelRegex.Match([]byte(v)) {
			labels[k] = v
		}
	}

	var env []corev1.EnvVar
	for name, value := range opts.Env {
		env = append(env, corev1.EnvVar{
			Name:  name,
			Value: value,
		})
	}

	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	for i, mount := range opts.Mounts {
		name := fmt.Sprintf("volume-%d", i)
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: mount.HostPath,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      name,
			MountPath: mount.ContainerPath,
			ReadOnly:  mount.ReadOnly,
		})
	}
	if opts.SharedMemory != 0 {
		volumes = append(volumes, corev1.Volume{
			Name: sharedMemoryVolume,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium:    corev1.StorageMediumMemory,
					SizeLimit: resource.NewQuantity(opts.SharedMemory, resource.DecimalExponent),
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      sharedMemoryVolume,
			MountPath: sharedMemoryMountPath,
		})
	}

	// Set requests and limits for all non-zero values. We set requests to half
	// of limits to give utilization tracking a hint without impacting scheduling.
	requests := corev1.ResourceList{}
	limits := corev1.ResourceList{}
	if opts.IsEvictable() {
		// There are 3 QoS classes in Kubernetes: Guaranteed, Burstable, and BestEffort.
		// Pods with Guaranteed QoS are the first to be scheduled and the last to be evicted.
		// To be Guaranteed, every container in the pod must specify a request and limit
		// for CPU and memory.
		// Pods that are of the BestEffort category are the first to be evicted.
		// To be BestEffort, none of the containers in the pod can specify requests or limits.
		// Source: https://docs.docker.com/config/containers/resource_constraints/

		// If the pod is evictable, don't specify any requests or limits so that it
		// gets BestEffort QoS.
	} else {
		if opts.Memory != 0 {
			// Use a small request to avoid scheduling issues on small nodes.
			// If the request exceeds what is available on the node, K8s won't schedule the pod.
			// Since we assign the pod to a specific node, this behavior is undesired.
			// To get around it, we set the request to a value small enough that the node will
			// always be able to accomodate it.
			requests[corev1.ResourceMemory] = *resource.NewQuantity(opts.Memory/10, resource.DecimalSI)
			limits[corev1.ResourceMemory] = *resource.NewQuantity(opts.Memory, resource.DecimalSI)
		}
		if opts.CPUCount != 0 {
			milli := int64(opts.CPUCount * 1000)
			// Use a small request to avoid scheduling issues on small nodes.
			// See the comment for memory for an explanation of why this is necessary.
			requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(milli/10), resource.DecimalSI)
			limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(milli), resource.DecimalSI)
		}
		if count := len(opts.GPUs); count != 0 {
			// Kubernetes offers no way to bind to specific GPUs, but guarantees
			// that they will only be mapped to one container. Just use the count.
			limits[gpuResource] = *resource.NewQuantity(int64(count), resource.DecimalSI)
		}
	}

	podSpec := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: annos,
			Name:        opts.Name,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					// The pause image does nothing. Its purpose is to keep the
					// pod alive after the task container has exited. We will
					// explicitly delete the pod when needed.
					Image: "gcr.io/google-containers/pause",
					Name:  "pause",
				},
				{
					Command:      opts.Command,
					Args:         opts.Arguments,
					Env:          env,
					Image:        opts.Image.Tag,
					Name:         containerName,
					VolumeMounts: volumeMounts,
					Resources:    corev1.ResourceRequirements{Requests: requests, Limits: limits},
				},
			},
			NodeName:      r.node,
			RestartPolicy: "Never",
			Volumes:       volumes,
		},
	}

	pod, err := r.client.CoreV1().Pods(r.namespace).Create(ctx, podSpec, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("creating pod: %w", err)
	}

	minAvailable := intstr.FromInt(1)
	pdbSpec := &policyv1beta1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name: pod.ObjectMeta.Name,
		},
		Spec: policyv1beta1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: pod.ObjectMeta.Labels,
			},
		},
	}

	pdbs := r.client.PolicyV1beta1().PodDisruptionBudgets(r.namespace)
	if _, err = pdbs.Create(ctx, pdbSpec, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("creating pod disruption budget: %w", err)
	}

	return &Container{
		client:        r.client,
		runtime:       r.runtime,
		namespace:     r.namespace,
		podName:       pod.Name,
		containerName: containerName,
	}, nil
}

// ListContainers enumerates all containers.
func (r *Runtime) ListContainers(ctx context.Context) ([]runtime.Container, error) {
	pods, err := r.client.CoreV1().Pods(r.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", nodeLabel, r.node),
	})
	if err != nil {
		return nil, fmt.Errorf("listing pods: %w", err)
	}

	var containers []runtime.Container
	for _, pod := range pods.Items {
		containers = append(containers, &Container{
			client:        r.client,
			runtime:       r.runtime,
			namespace:     r.namespace,
			podName:       pod.Name,
			containerName: containerName,
		})
	}
	return containers, nil
}
