// Package workerconfig holds the worker and daemon configuration constants
// and helpers shared by the pps master (which builds worker RC specs) and
// pachd itself. It is the local-mode remnant of the former
// pkg/deploy/assets package: everything that generated Kubernetes manifests
// has been removed.
package workerconfig

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"github.com/pachyderm/pachyderm/src/server/pps/server/githook"
	v1 "k8s.io/api/core/v1"
)

const (
	// UploadConcurrencyLimitEnvVar is the environment variable for the upload concurrency limit.
	UploadConcurrencyLimitEnvVar = "STORAGE_UPLOAD_CONCURRENCY_LIMIT"

	// PutFileConcurrencyLimitEnvVar is the environment variable for the PutFile concurrency limit.
	PutFileConcurrencyLimitEnvVar = "STORAGE_PUT_FILE_CONCURRENCY_LIMIT"

	// DefaultUploadConcurrencyLimit is the default maximum number of concurrent object storage uploads.
	DefaultUploadConcurrencyLimit = 100

	// DefaultPutFileConcurrencyLimit is the default maximum number of concurrent files that can be uploaded over GRPC or downloaded from external sources (ex. HTTP or blob storage).
	DefaultPutFileConcurrencyLimit = 100

	// PrometheusPort hosts the prometheus stats for scraping.
	PrometheusPort = 656
)

// GetSecretEnvVars returns the environment variable specs for the storage secret.
func GetSecretEnvVars(storageBackend string) []v1.EnvVar {
	var envVars []v1.EnvVar
	if storageBackend != "" {
		envVars = append(envVars, v1.EnvVar{
			Name:  obj.StorageBackendEnvVar,
			Value: storageBackend,
		})
	}
	trueVal := true
	for _, e := range obj.EnvVarToSecretKey {
		envVars = append(envVars, v1.EnvVar{
			Name: e.Key,
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: client.StorageSecretName,
					},
					Key:      e.Value,
					Optional: &trueVal,
				},
			},
		})
	}
	return envVars
}

// GithookService returns the Service object for the githook. In a Kubernetes
// deployment this was created as a load balancer by the deploy tooling; the
// local runtime consumes the same object from its service registry.
func GithookService(namespace string) *v1.Service {
	name := "githook"
	return &v1.Service{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Service",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Labels:    map[string]string{"app": name, "suite": "pachyderm"},
			Namespace: namespace,
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeLoadBalancer,
			Selector: map[string]string{
				"app": "pachd",
			},
			Ports: []v1.ServicePort{
				{
					TargetPort: intstr.FromInt(githook.GitHookPort),
					Name:       "api-git-port",
					Port:       githook.ExternalPort(),
				},
			},
		},
	}
}
