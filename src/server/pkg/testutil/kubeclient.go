package testutil

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	kubeCore "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	zero int64
)

// GetKubeClient connects to the Kubernetes API server either from inside the
// cluster or from a test binary running on a machine with kubectl (it will
// connect to the same cluster as kubectl). When PACHD_ADDRESS points at a
// loopback address (local mode), it returns a shim that proxies Secrets to
// the daemon's pps Secret API (which lands in the daemon's in-memory k8s
// store) and serves every other resource from an empty fake, so tests never
// touch a real cluster.
func GetKubeClient(t testing.TB) kube.Interface {
	if addr := os.Getenv("PACHD_ADDRESS"); addr != "" {
		host, _, err := net.SplitHostPort(addr)
		if err == nil && (host == "localhost" || (net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback())) {
			pachClient, err := client.NewForTest()
			require.NoError(t, err)
			return &localKubeClient{Clientset: fake.NewSimpleClientset(), c: pachClient}
		}
	}
	var config *rest.Config
	var err error
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host != "" {
		config, err = rest.InClusterConfig()
	} else {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules,
			&clientcmd.ConfigOverrides{})
		config, err = kubeConfig.ClientConfig()
	}
	require.NoError(t, err)
	k, err := kube.NewForConfig(config)
	require.NoError(t, err)
	return k
}

// LocalMode reports whether the suite is pointed at a local-mode pachd
// (PACHD_ADDRESS on loopback), where k8s-only features (pipeline services,
// auth) are not available and their tests must be skipped.
func LocalMode() bool {
	if addr := os.Getenv("PACHD_ADDRESS"); addr != "" {
		host, _, err := net.SplitHostPort(addr)
		if err == nil && (host == "localhost" || (net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback())) {
			return true
		}
	}
	return false
}

// localKubeClient is the local-mode shim: an empty fake clientset whose
// Secrets are proxied to the pachd at PACHD_ADDRESS via the pps Secret API.
type localKubeClient struct {
	*fake.Clientset
	c *client.APIClient
}

func (l *localKubeClient) CoreV1() kubeCore.CoreV1Interface {
	return &localCoreV1{CoreV1Interface: l.Clientset.CoreV1(), c: l.c}
}

type localCoreV1 struct {
	kubeCore.CoreV1Interface
	c *client.APIClient
}

func (l *localCoreV1) Secrets(ns string) kubeCore.SecretInterface {
	return &proxySecrets{c: l.c, ns: ns}
}

// proxySecrets implements v1core.SecretInterface against the pps Secret API.
// The pps API stores secrets in the same k8s store the pipeline workers read,
// so secrets created here are visible to resolveEnv and secret volume mounts.
type proxySecrets struct {
	c  *client.APIClient
	ns string
}

func (s *proxySecrets) Create(secret *v1.Secret) (*v1.Secret, error) {
	data, err := json.Marshal(secret)
	if err != nil {
		return nil, err
	}
	if err := s.c.CreateSecret(data); err != nil {
		return nil, err
	}
	return secret, nil
}

func (s *proxySecrets) Update(secret *v1.Secret) (*v1.Secret, error) {
	return s.Create(secret)
}

func (s *proxySecrets) Delete(name string, _ *metav1.DeleteOptions) error {
	return s.c.DeleteSecret(name)
}

func (s *proxySecrets) DeleteCollection(_ *metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	list, err := s.List(listOpts)
	if err != nil {
		return err
	}
	for _, item := range list.Items {
		if err := s.c.DeleteSecret(item.Name); err != nil {
			return err
		}
	}
	return nil
}

func (s *proxySecrets) Get(name string, _ metav1.GetOptions) (*v1.Secret, error) {
	info, err := s.c.InspectSecret(name)
	if err != nil {
		return nil, err
	}
	return &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: info.Secret.Name, Namespace: s.ns}}, nil
}

func (s *proxySecrets) List(_ metav1.ListOptions) (*v1.SecretList, error) {
	infos, err := s.c.ListSecret()
	if err != nil {
		return nil, err
	}
	list := &v1.SecretList{}
	for _, info := range infos {
		list.Items = append(list.Items, v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: info.Secret.Name, Namespace: s.ns}})
	}
	return list, nil
}

func (s *proxySecrets) Watch(_ metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (s *proxySecrets) Patch(name string, pt types.PatchType, data []byte, _ ...string) (*v1.Secret, error) {
	secret, err := s.Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, secret); err != nil {
		return nil, err
	}
	return s.Create(secret)
}

// DeletePachdPod deletes the pachd pod in a test cluster (restarting it, e.g.
// to retart the PPS master)
func DeletePachdPod(t testing.TB) {
	kubeClient := GetKubeClient(t)
	podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(
		metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
				map[string]string{"app": "pachd", "suite": "pachyderm"},
			)),
		})
	require.NoError(t, err)
	require.Equal(t, 1, len(podList.Items))
	require.NoError(t, kubeClient.CoreV1().Pods(v1.NamespaceDefault).Delete(
		podList.Items[0].ObjectMeta.Name, &metav1.DeleteOptions{}))

	// Make sure pachd goes down
	startTime := time.Now()
	require.NoError(t, backoff.Retry(func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": "pachd", "suite": "pachyderm"},
				)),
			})
		if err != nil {
			return err
		}
		if len(podList.Items) == 0 {
			return nil
		}
		if time.Since(startTime) > 10*time.Second {
			return nil
		}
		return errors.Errorf("waiting for old pachd pod to be killed")
	}, backoff.NewTestingBackOff()))

	// Make sure pachd comes back up
	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": "pachd", "suite": "pachyderm"},
				)),
			})
		if err != nil {
			return err
		}
		if len(podList.Items) == 0 {
			return errors.Errorf("no pachd pod up yet")
		}
		return nil
	})

	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		podList, err := kubeClient.CoreV1().Pods(v1.NamespaceDefault).List(
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"app": "pachd", "suite": "pachyderm"},
				)),
			})
		if err != nil {
			return err
		}
		if len(podList.Items) == 0 {
			return errors.Errorf("no pachd pod up yet")
		}
		if podList.Items[0].Status.Phase != v1.PodRunning {
			return errors.Errorf("pachd not running yet")
		}
		return err
	})
}

// DeletePipelineRC deletes the RC belonging to the pipeline 'pipeline'. This
// can be used to test PPS's robustness
func DeletePipelineRC(t testing.TB, pipeline string) {
	kubeClient := GetKubeClient(t)
	rcs, err := kubeClient.CoreV1().ReplicationControllers(v1.NamespaceDefault).List(
		metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
				map[string]string{"pipelineName": pipeline},
			)),
		})
	require.NoError(t, err)
	require.Equal(t, 1, len(rcs.Items))
	require.NoError(t, kubeClient.CoreV1().ReplicationControllers(v1.NamespaceDefault).Delete(
		rcs.Items[0].ObjectMeta.Name, &metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
		}))
	require.NoErrorWithinTRetry(t, 30*time.Second, func() error {
		rcs, err := kubeClient.CoreV1().ReplicationControllers(v1.NamespaceDefault).List(
			metav1.ListOptions{
				LabelSelector: metav1.FormatLabelSelector(metav1.SetAsLabelSelector(
					map[string]string{"pipelineName": pipeline},
				)),
			})
		if err != nil {
			return err
		}
		if len(rcs.Items) != 0 {
			return errors.Errorf("RC %q not deleted yet", pipeline)
		}
		return nil
	})
}

// PachdDeployment finds the corresponding deployment for pachd in the
// kubernetes namespace and returns it.
func PachdDeployment(t testing.TB, namespace string) *apps.Deployment {
	k := GetKubeClient(t)
	result, err := k.AppsV1().Deployments(namespace).Get("pachd", metav1.GetOptions{})
	require.NoError(t, err)
	return result
}

func podRunningAndReady(e watch.Event) (bool, error) {
	if e.Type == watch.Deleted {
		return false, errors.New("received DELETE while watching pods")
	}
	pod, ok := e.Object.(*v1.Pod)
	if !ok {
		return false, errors.Errorf("unexpected object type in watch.Event")
	}
	return pod.Status.Phase == v1.PodRunning, nil
}

// WaitForPachdReady finds the pachd pods within the kubernetes namespace and
// blocks until they are all ready.
func WaitForPachdReady(t testing.TB, namespace string) {
	k := GetKubeClient(t)
	deployment := PachdDeployment(t, namespace)
	for {
		newDeployment, err := k.AppsV1().Deployments(namespace).Get(deployment.Name, metav1.GetOptions{})
		require.NoError(t, err)
		if newDeployment.Status.ObservedGeneration >= deployment.Generation && newDeployment.Status.Replicas == *newDeployment.Spec.Replicas {
			break
		}
		time.Sleep(time.Second * 5)
	}
	watch, err := k.CoreV1().Pods(namespace).Watch(metav1.ListOptions{
		LabelSelector: "app=pachd",
	})
	defer watch.Stop()
	require.NoError(t, err)
	readyPods := make(map[string]bool)
	for event := range watch.ResultChan() {
		ready, err := podRunningAndReady(event)
		require.NoError(t, err)
		if ready {
			pod, ok := event.Object.(*v1.Pod)
			if !ok {
				t.Fatal("event.Object should be an object")
			}
			readyPods[pod.Name] = true
			if len(readyPods) == int(*deployment.Spec.Replicas) {
				break
			}
		}
	}
}
