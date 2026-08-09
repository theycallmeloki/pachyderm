package testutil

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/require"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"

	apps "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	kube "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	kubeCore "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
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
	var config *restclient.Config
	var err error
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host != "" {
		config, err = restclient.InClusterConfig()
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

func (l *localCoreV1) Services(ns string) kubeCore.ServiceInterface {
	return &proxyServices{kubeObjectStore: kubeObjectStore{kind: "services", ns: ns}}
}

func (l *localCoreV1) ReplicationControllers(ns string) kubeCore.ReplicationControllerInterface {
	return &proxyRCs{kubeObjectStore: kubeObjectStore{kind: "rcs", ns: ns}}
}

func (l *localCoreV1) Pods(ns string) kubeCore.PodInterface {
	return &proxyPods{kubeObjectStore: kubeObjectStore{kind: "pods", ns: ns}}
}

// localKubeHTTPBase is the daemon HTTP API URL that serves the local-mode
// k8s object store (services, RCs, pods) that the daemon itself created.
func localKubeHTTPBase() string {
	host, _, err := net.SplitHostPort(os.Getenv("PACHD_ADDRESS"))
	if err != nil {
		return ""
	}
	port := os.Getenv("PACHD_SERVICE_PORT_API_HTTP_PORT")
	if port == "" {
		port = "30652" // pachd's HTTP API port (see cmd/pachd/main.go)
	}
	return "http://" + net.JoinHostPort(host, port) + "/v1/local/kube/"
}

// kubeObjectStore is the read-only HTTP client for the daemon's in-memory
// k8s object store. It is read-only by design: suite tests create resources
// through the pps API, which lands in the same store the daemon serves.
type kubeObjectStore struct {
	kind string
	ns   string
}

func (p *kubeObjectStore) list(opts metav1.ListOptions) ([]byte, error) {
	u := localKubeHTTPBase() + p.kind + "?namespace=" + url.QueryEscape(p.ns)
	if opts.LabelSelector != "" {
		u += "&labelSelector=" + url.QueryEscape(opts.LabelSelector)
	}
	resp, err := http.Get(u)
	if err != nil {
		return nil, errors.Wrapf(err, "could not list %s from local kube store", p.kind)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("local kube store returned %d for %s list", resp.StatusCode, p.kind)
	}
	return ioutil.ReadAll(resp.Body)
}

func (p *kubeObjectStore) notSupported(verb string) error {
	return errors.Errorf("local kube store does not support %s of %s from the test shim", verb, p.kind)
}

func (p *kubeObjectStore) notFound(name string) error {
	return kubeerrors.NewNotFound(schema.GroupResource{Group: "", Resource: p.kind}, name)
}

// errRequest returns a rest client request that fails cleanly if executed,
// for PodInterface/ServiceExpansion methods that the suite never calls in
// local mode (pod logs go through the pps GetLogs API).
func errRequest() *restclient.Request {
	c, err := restclient.UnversionedRESTClientFor(&restclient.Config{Host: "http://127.0.0.1:1"})
	if err != nil {
		panic(err) // unreachable: the config is static
	}
	return c.Get()
}

// proxyServices implements v1core.ServiceInterface against the daemon store.
type proxyServices struct {
	kubeObjectStore
}

func (s *proxyServices) Create(*v1.Service) (*v1.Service, error) {
	return nil, s.notSupported("creating")
}
func (s *proxyServices) Update(*v1.Service) (*v1.Service, error) {
	return nil, s.notSupported("updating")
}
func (s *proxyServices) UpdateStatus(*v1.Service) (*v1.Service, error) {
	return nil, s.notSupported("updating")
}
func (s *proxyServices) Delete(name string, _ *metav1.DeleteOptions) error {
	return s.notFound(name)
}
func (s *proxyServices) Get(name string, _ metav1.GetOptions) (*v1.Service, error) {
	list, err := s.List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Name == name {
			return &list.Items[i], nil
		}
	}
	return nil, s.notFound(name)
}
func (s *proxyServices) List(opts metav1.ListOptions) (*v1.ServiceList, error) {
	raw, err := s.list(opts)
	if err != nil {
		return nil, err
	}
	var out v1.ServiceList
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func (s *proxyServices) Watch(_ metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}
func (s *proxyServices) Patch(name string, _ types.PatchType, _ []byte, _ ...string) (*v1.Service, error) {
	return nil, s.notFound(name)
}
func (s *proxyServices) ProxyGet(_, _, _, _ string, _ map[string]string) restclient.ResponseWrapper {
	return errRequest()
}

// proxyRCs implements v1core.ReplicationControllerInterface.
type proxyRCs struct {
	kubeObjectStore
}

func (r *proxyRCs) Create(*v1.ReplicationController) (*v1.ReplicationController, error) {
	return nil, r.notSupported("creating")
}
func (r *proxyRCs) Update(*v1.ReplicationController) (*v1.ReplicationController, error) {
	return nil, r.notSupported("updating")
}
func (r *proxyRCs) UpdateStatus(*v1.ReplicationController) (*v1.ReplicationController, error) {
	return nil, r.notSupported("updating")
}
func (r *proxyRCs) Delete(name string, _ *metav1.DeleteOptions) error {
	return r.notFound(name)
}
func (r *proxyRCs) DeleteCollection(_ *metav1.DeleteOptions, _ metav1.ListOptions) error {
	return r.notSupported("deleting")
}
func (r *proxyRCs) Get(name string, _ metav1.GetOptions) (*v1.ReplicationController, error) {
	list, err := r.List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Name == name {
			return &list.Items[i], nil
		}
	}
	return nil, r.notFound(name)
}
func (r *proxyRCs) List(opts metav1.ListOptions) (*v1.ReplicationControllerList, error) {
	raw, err := r.list(opts)
	if err != nil {
		return nil, err
	}
	var out v1.ReplicationControllerList
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func (r *proxyRCs) Watch(_ metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}
func (r *proxyRCs) Patch(name string, _ types.PatchType, _ []byte, _ ...string) (*v1.ReplicationController, error) {
	return nil, r.notFound(name)
}
func (r *proxyRCs) GetScale(name string, _ metav1.GetOptions) (*autoscalingv1.Scale, error) {
	rc, err := r.Get(name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return rcScale(rc), nil
}
func (r *proxyRCs) UpdateScale(name string, scale *autoscalingv1.Scale) (*autoscalingv1.Scale, error) {
	u := localKubeHTTPBase() + "rcs/" + url.PathEscape(name) + "/scale?namespace=" + url.QueryEscape(r.ns)
	body, err := json.Marshal(struct {
		Replicas int32 `json:"replicas"`
	}{Replicas: scale.Spec.Replicas})
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(u, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrapf(err, "could not update scale of %s in local kube store", r.kind)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("local kube store returned %d for %s scale", resp.StatusCode, r.kind)
	}
	var out autoscalingv1.Scale
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

func rcScale(rc *v1.ReplicationController) *autoscalingv1.Scale {
	replicas := int32(0)
	if rc.Spec.Replicas != nil {
		replicas = *rc.Spec.Replicas
	}
	return &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: rc.Name, Namespace: rc.Namespace},
		Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
	}
}

// proxyPods implements v1core.PodInterface.
type proxyPods struct {
	kubeObjectStore
}

func (p *proxyPods) Create(*v1.Pod) (*v1.Pod, error) {
	return nil, p.notSupported("creating")
}
func (p *proxyPods) Update(*v1.Pod) (*v1.Pod, error) {
	return nil, p.notSupported("updating")
}
func (p *proxyPods) UpdateStatus(*v1.Pod) (*v1.Pod, error) {
	return nil, p.notSupported("updating")
}
func (p *proxyPods) Delete(name string, _ *metav1.DeleteOptions) error {
	return p.notFound(name)
}
func (p *proxyPods) DeleteCollection(_ *metav1.DeleteOptions, _ metav1.ListOptions) error {
	return p.notSupported("deleting")
}
func (p *proxyPods) Get(name string, _ metav1.GetOptions) (*v1.Pod, error) {
	list, err := p.List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		if list.Items[i].Name == name {
			return &list.Items[i], nil
		}
	}
	return nil, p.notFound(name)
}
func (p *proxyPods) List(opts metav1.ListOptions) (*v1.PodList, error) {
	raw, err := p.list(opts)
	if err != nil {
		return nil, err
	}
	var out v1.PodList
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
func (p *proxyPods) Watch(_ metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}
func (p *proxyPods) Patch(name string, _ types.PatchType, _ []byte, _ ...string) (*v1.Pod, error) {
	return nil, p.notFound(name)
}
func (p *proxyPods) Bind(*v1.Binding) error {
	return p.notSupported("binding")
}
func (p *proxyPods) Evict(*policy.Eviction) error {
	return p.notSupported("evicting")
}
func (p *proxyPods) GetLogs(string, *v1.PodLogOptions) *restclient.Request {
	return errRequest()
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
