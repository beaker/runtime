package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/allenai/bytefmt"
	dockerclient "github.com/docker/docker/client"
	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp" // Google Cloud Platform auth plugin for out of cluster authentication.
	"k8s.io/client-go/rest"

	"github.com/beaker/runtime"
	"github.com/beaker/runtime/docker"
)

const (
	containerName         = "task"
	nodeLabel             = "beaker.org/node"
	sharedMemoryEnvVar    = "BEAKER_FEATURE_SHARED_MEMORY_OVERRIDE"
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
	docker    *dockerclient.Client
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

	// Negotiate the docker version since GKE could be running an older version of Docker
	dockerClient, err := dockerclient.NewClientWithOpts(dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return nil, err
	}

	return &Runtime{
		client:    client,
		docker:    dockerClient,
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
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	for name, value := range opts.Env {
		// TODO: This should really be in the spec. See #911 for issue tracking longer term fix.
		if name == sharedMemoryEnvVar && strings.ToLower(value) == "true" {
			limit := opts.Memory / 2 // Use half of available memory as limit
			if limit == 0 {
				limit = bytefmt.GiB // Arbitrary value to ensure predictable behavior.
			}
			volumes = append(volumes, corev1.Volume{
				Name: sharedMemoryVolume,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium:    corev1.StorageMediumMemory,
						SizeLimit: resource.NewQuantity(limit, resource.DecimalExponent),
					},
				},
			})

			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      sharedMemoryVolume,
				MountPath: sharedMemoryMountPath,
			})
		}
		env = append(env, corev1.EnvVar{
			Name:  name,
			Value: value,
		})
	}

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

	// Set requests and limits for all non-zero values. We set requests to half
	// of limits to give utilization tracking a hint without impacting scheduling.
	requests := corev1.ResourceList{}
	limits := corev1.ResourceList{}
	if opts.Memory != 0 {
		// Default to small request to avoid scheduling issues on small nodes
		requests[corev1.ResourceMemory] = *resource.NewQuantity(opts.Memory/10, resource.DecimalSI)
		limits[corev1.ResourceMemory] = *resource.NewQuantity(opts.Memory, resource.DecimalSI)
	}
	if opts.CPUCount != 0 {
		milli := int64(opts.CPUCount * 1000)
		// Default to small request to avoid scheduling issues on small nodes
		requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(milli/10), resource.DecimalSI)
		limits[corev1.ResourceCPU] = *resource.NewMilliQuantity(int64(milli), resource.DecimalSI)
	}
	if count := len(opts.GPUs); count != 0 {
		// Kubernetes offers no way to bind to specific GPUs, but guarantees
		// that they will only be mapped to one container. Just use the count.
		limits[gpuResource] = *resource.NewQuantity(int64(count), resource.DecimalSI)
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
		docker:        r.dockerContainer(pod),
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
			docker:        r.dockerContainer(&pod),
			namespace:     r.namespace,
			podName:       pod.Name,
			containerName: containerName,
		})
	}
	return containers, nil
}

func (r *Runtime) dockerContainer(pod *corev1.Pod) *docker.Container {
	name := fmt.Sprintf("k8s_%s_%s_%s_%s_0", containerName, pod.Name, r.namespace, pod.UID)
	return docker.WrapContainer(r.docker, name)
}
