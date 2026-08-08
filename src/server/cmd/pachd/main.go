package main

import (
	"os/signal"
	"syscall"
	gotls "crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"runtime/pprof"
	"strconv"

	"github.com/pachyderm/pachyderm/src/client"
	adminclient "github.com/pachyderm/pachyderm/src/client/admin"
	authclient "github.com/pachyderm/pachyderm/src/client/auth"
	debugclient "github.com/pachyderm/pachyderm/src/client/debug"
	eprsclient "github.com/pachyderm/pachyderm/src/client/enterprise"
	healthclient "github.com/pachyderm/pachyderm/src/client/health"
	pfsclient "github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/discovery"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/client/pkg/grpcutil"
	"github.com/pachyderm/pachyderm/src/client/pkg/shard"
	"github.com/pachyderm/pachyderm/src/client/pkg/tracing"
	ppsclient "github.com/pachyderm/pachyderm/src/client/pps"
	transactionclient "github.com/pachyderm/pachyderm/src/client/transaction"
	"github.com/pachyderm/pachyderm/src/client/version"
	"github.com/pachyderm/pachyderm/src/client/version/versionpb"
	adminserver "github.com/pachyderm/pachyderm/src/server/admin/server"
	authserver "github.com/pachyderm/pachyderm/src/server/auth/server"
	debugserver "github.com/pachyderm/pachyderm/src/server/debug/server"
	eprsserver "github.com/pachyderm/pachyderm/src/server/enterprise/server"
	"github.com/pachyderm/pachyderm/src/server/health"
	pach_http "github.com/pachyderm/pachyderm/src/server/http"
	"github.com/pachyderm/pachyderm/src/server/pfs/s3"
	pfs_server "github.com/pachyderm/pachyderm/src/server/pfs/server"
	cache_pb "github.com/pachyderm/pachyderm/src/server/pkg/cache/groupcachepb"
	cache_server "github.com/pachyderm/pachyderm/src/server/pkg/cache/server"
	"github.com/pachyderm/pachyderm/src/server/pkg/cmdutil"
	col "github.com/pachyderm/pachyderm/src/server/pkg/collection"
	"github.com/pachyderm/pachyderm/src/server/pkg/deploy/assets"
	"github.com/pachyderm/pachyderm/src/server/pkg/hashtree"
	"github.com/pachyderm/pachyderm/src/server/pkg/localclient"
	"github.com/pachyderm/pachyderm/src/server/pkg/localetcd"
	logutil "github.com/pachyderm/pachyderm/src/server/pkg/log"
	"github.com/pachyderm/pachyderm/src/server/pkg/metrics"
	"github.com/pachyderm/pachyderm/src/server/pkg/netutil"
	"github.com/pachyderm/pachyderm/src/server/pkg/serviceenv"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	pps_server "github.com/pachyderm/pachyderm/src/server/pps/server"
	"github.com/pachyderm/pachyderm/src/server/pps/server/githook"
	txnserver "github.com/pachyderm/pachyderm/src/server/transaction/server"

	etcd "github.com/coreos/etcd/clientv3"
	units "github.com/docker/go-units"
	"github.com/pachyderm/pachyderm/src/client/pkg/tls"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

const (
	defaultTreeCacheSize = 8
)

var mode string
var readiness bool

func init() {
	flag.StringVar(&mode, "mode", "full", "Pachd currently supports two modes: full and sidecar.  The former includes everything you need in a full pachd node.  The latter runs only PFS, the Auth service, and a stripped-down version of PPS.")
	flag.BoolVar(&readiness, "readiness", false, "Run readiness check.")
	flag.Parse()
}

func main() {
	log.SetFormatter(logutil.FormatterFunc(logutil.Pretty))

	switch {
	case readiness:
		cmdutil.Main(doReadinessCheck, &serviceenv.PachdFullConfiguration{})
	case mode == "full":
		cmdutil.Main(doFullMode, &serviceenv.PachdFullConfiguration{})
	case mode == "sidecar":
		cmdutil.Main(doSidecarMode, &serviceenv.PachdFullConfiguration{})
	case mode == "local":
		runLocal()
	default:
		fmt.Printf("unrecognized mode: %s\n", mode)
	}
}

// runLocal boots pachd in local (single-node, k8s-free) mode: it starts an
// embedded etcd and sets the env vars that the required config fields
// (ETCD_SERVICE_HOST/PORT) are loaded from, then runs the normal config
// loading and hands off to doLocalMode.
func runLocal() {
	localDir := os.Getenv("PACH_LOCAL_DIR")
	if localDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			fmt.Printf("could not determine home directory: %v\n", err)
			os.Exit(1)
		}
		localDir = filepath.Join(home, ".pachyderm-local")
	}
	if err := os.MkdirAll(localDir, 0700); err != nil {
		fmt.Printf("could not create local data directory %q: %v\n", localDir, err)
		os.Exit(1)
	}
	os.Setenv("PACH_LOCAL_DIR", localDir)

	etcdPort := 2379
	if port := os.Getenv("PACH_LOCAL_ETCD_PORT"); port != "" {
		p, err := strconv.Atoi(port)
		if err != nil {
			fmt.Printf("invalid PACH_LOCAL_ETCD_PORT %q: %v\n", port, err)
			os.Exit(1)
		}
		etcdPort = p
	}
	etcdEnv, err := localetcd.Start(filepath.Join(localDir, "etcd"), uint16(etcdPort))
	if err != nil {
		fmt.Printf("could not start embedded etcd: %v\n", err)
		os.Exit(1)
	}
	defer etcdEnv.Close()
	os.Setenv("ETCD_SERVICE_HOST", "127.0.0.1")
	os.Setenv("ETCD_SERVICE_PORT", strconv.Itoa(etcdPort))
	// PACHD_POD_NAME is normally injected by k8s' Downward API; give the
	// daemon a stable name in local mode.
	os.Setenv("PACHD_POD_NAME", "pachd-local")

	// pachctl connects to 0.0.0.0:30650 by default, so serve the API there.
	if os.Getenv("PORT") == "" {
		os.Setenv("PORT", strconv.Itoa(grpcutil.DefaultPachdNodePort))
	}
	// Workers share one gRPC port (each binds its own loopback IP). Port 80
	// requires root; pick a high port unless the user overrides it.
	if os.Getenv("PPS_WORKER_GRPC_PORT") == "" {
		os.Setenv("PPS_WORKER_GRPC_PORT", "1650")
	}
	// Use the filesystem object store rooted in the local data directory.
	if os.Getenv("STORAGE_BACKEND") == "" {
		os.Setenv("STORAGE_BACKEND", pfs_server.LocalBackendEnvVar)
	}
	// Pachyderm's default auxiliary ports (600-657) are below 1024 and some
	// hosts block them even for unprivileged processes, so local mode remaps
	// every one of them to the 306xx range.
	if os.Getenv("HTTP_PORT") == "" {
		os.Setenv("HTTP_PORT", "30652")
	}
	if os.Getenv("PEER_PORT") == "" {
		os.Setenv("PEER_PORT", "30653")
	}
	if os.Getenv("SAML_PORT") == "" {
		os.Setenv("SAML_PORT", "30654")
	}
	// The fork's GitHookPort is 655, so the githook is reached at
	// GitHookPort+30000 = 30655 by pachctl and the test suite.
	if os.Getenv("GITHOOK_PORT") == "" {
		os.Setenv("GITHOOK_PORT", "30655")
	}
	if os.Getenv("PROMETHEUS_PORT") == "" {
		os.Setenv("PROMETHEUS_PORT", "30656")
	}
	if os.Getenv("OIDC_PORT") == "" {
		os.Setenv("OIDC_PORT", "30657")
	}
	if os.Getenv("S3GATEWAY_PORT") == "" {
		os.Setenv("S3GATEWAY_PORT", "30600")
	}
	if os.Getenv("PACH_ROOT") == "" {
		os.Setenv("PACH_ROOT", filepath.Join(localDir, "storage"))
	}
	if err := os.MkdirAll(os.Getenv("PACH_ROOT"), 0700); err != nil {
		fmt.Printf("could not create storage root %q: %v\n", os.Getenv("PACH_ROOT"), err)
		os.Exit(1)
	}
	// Workers talk to the daemon's external API server (there is no per-pod
	// sidecar in local mode), so the object API must be served there too.
	if os.Getenv("EXPOSE_OBJECT_API") == "" {
		os.Setenv("EXPOSE_OBJECT_API", "true")
	}
	// These are normally injected by the k8s deployment; they are required by
	// the worker pod spec construction.
	if os.Getenv(assets.UploadConcurrencyLimitEnvVar) == "" {
		os.Setenv(assets.UploadConcurrencyLimitEnvVar, strconv.Itoa(assets.DefaultUploadConcurrencyLimit))
	}
	if os.Getenv(assets.PutFileConcurrencyLimitEnvVar) == "" {
		os.Setenv(assets.PutFileConcurrencyLimitEnvVar, strconv.Itoa(assets.DefaultPutFileConcurrencyLimit))
	}
	cmdutil.Main(doLocalMode, &serviceenv.PachdFullConfiguration{})
}

func doReadinessCheck(config interface{}) error {
	env := serviceenv.InitPachOnlyEnv(serviceenv.NewConfiguration(config))
	return env.GetPachClient(context.Background()).Health()
}

func doSidecarMode(config interface{}) (retErr error) {
	defer func() {
		if retErr != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		}
	}()
	switch logLevel := os.Getenv("LOG_LEVEL"); logLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "info", "":
		log.SetLevel(log.InfoLevel)
	default:
		log.Errorf("Unrecognized log level %s, falling back to default of \"info\"", logLevel)
		log.SetLevel(log.InfoLevel)
	}
	// must run InstallJaegerTracer before InitWithKube (otherwise InitWithKube
	// may create a pach client before tracing is active, not install the Jaeger
	// gRPC interceptor in the client, and not propagate traces)
	if endpoint := tracing.InstallJaegerTracerFromEnv(); endpoint != "" {
		log.Printf("connecting to Jaeger at %q", endpoint)
	} else {
		log.Printf("no Jaeger collector found (JAEGER_COLLECTOR_SERVICE_HOST not set)")
	}
	env := serviceenv.InitWithKube(serviceenv.NewConfiguration(config))
	debug.SetGCPercent(env.GCPercent)
	if env.EtcdPrefix == "" {
		env.EtcdPrefix = col.DefaultPrefix
	}
	clusterID, err := getClusterID(env.GetEtcdClient())
	if err != nil {
		return errors.Wrapf(err, "getClusterID")
	}
	var reporter *metrics.Reporter
	if env.Metrics {
		reporter = metrics.NewReporter(clusterID, env)
	}
	pfsCacheSize, err := strconv.Atoi(env.PFSCacheSize)
	if err != nil {
		return errors.Wrapf(err, "atoi")
	}
	if pfsCacheSize == 0 {
		pfsCacheSize = defaultTreeCacheSize
	}
	treeCache, err := hashtree.NewCache(pfsCacheSize)
	if err != nil {
		return errors.Wrapf(err, "lru.New")
	}
	server, err := grpcutil.NewServer(context.Background(), false)
	if err != nil {
		return err
	}
	txnEnv := &txnenv.TransactionEnv{}
	if !env.StorageV2 {
		blockCacheBytes, err := units.RAMInBytes(env.BlockCacheBytes)
		if err != nil {
			return errors.Wrapf(err, "units.RAMInBytes")
		}
		if err := logGRPCServerSetup("Block API", func() error {
			blockAPIServer, err := pfs_server.NewBlockAPIServer(env.StorageRoot, blockCacheBytes, env.StorageBackend, net.JoinHostPort(env.EtcdHost, env.EtcdPort), false)
			if err != nil {
				return err
			}
			pfsclient.RegisterObjectAPIServer(server.Server, blockAPIServer)
			return nil
		}); err != nil {
			return err
		}
	}
	memoryRequestBytes, err := units.RAMInBytes(env.MemoryRequest)
	if err != nil {
		return err
	}
	var pfsAPIServer pfs_server.APIServer
	if err := logGRPCServerSetup("PFS API", func() error {
		pfsAPIServer, err = pfs_server.NewAPIServer(
			env,
			txnEnv,
			path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
			treeCache,
			env.StorageRoot,
			memoryRequestBytes,
		)
		if err != nil {
			return err
		}
		pfsclient.RegisterAPIServer(server.Server, pfsAPIServer)
		return nil
	}); err != nil {
		return err
	}
	var ppsAPIServer pps_server.APIServer
	if err := logGRPCServerSetup("PPS API", func() error {
		ppsAPIServer, err = pps_server.NewSidecarAPIServer(
			env,
			txnEnv,
			path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
			env.Namespace,
			env.IAMRole,
			reporter,
			env.PPSWorkerPort,
			env.HTTPPort,
			env.PeerPort,
		)
		if err != nil {
			return err
		}
		ppsclient.RegisterAPIServer(server.Server, ppsAPIServer)
		return nil
	}); err != nil {
		return err
	}
	var authAPIServer authserver.APIServer
	if err := logGRPCServerSetup("Auth API", func() error {
		authAPIServer, err = authserver.NewAuthServer(
			env,
			txnEnv,
			path.Join(env.EtcdPrefix, env.AuthEtcdPrefix),
			false,
			false,
		)
		if err != nil {
			return err
		}
		authclient.RegisterAPIServer(server.Server, authAPIServer)
		return nil
	}); err != nil {
		return err
	}
	var transactionAPIServer txnserver.APIServer
	if err := logGRPCServerSetup("Transaction API", func() error {
		transactionAPIServer, err = txnserver.NewAPIServer(
			env,
			txnEnv,
			path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
		)
		if err != nil {
			return err
		}
		transactionclient.RegisterAPIServer(server.Server, transactionAPIServer)
		return nil
	}); err != nil {
		return err
	}
	if err := logGRPCServerSetup("Enterprise API", func() error {
		enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
			env, path.Join(env.EtcdPrefix, env.EnterpriseEtcdPrefix))
		if err != nil {
			return err
		}
		eprsclient.RegisterAPIServer(server.Server, enterpriseAPIServer)
		return nil
	}); err != nil {
		return err
	}
	if err := logGRPCServerSetup("Health", func() error {
		healthclient.RegisterHealthServer(server.Server, health.NewHealthServer())
		return nil
	}); err != nil {
		return err
	}
	if err := logGRPCServerSetup("Debug", func() error {
		debugclient.RegisterDebugServer(server.Server, debugserver.NewDebugServer(
			env,
			env.PachdPodName,
			nil,
		))
		return nil
	}); err != nil {
		return err
	}
	txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer, ppsAPIServer)
	// The sidecar only needs to serve traffic on the peer port, as it only serves
	// traffic from the user container (the worker binary and occasionally user
	// pipelines)
	if _, err := server.ListenTCP("", env.PeerPort); err != nil {
		return err
	}
	return server.Wait()
}

func doFullMode(config interface{}) (retErr error) {
	defer func() {
		if retErr != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		}
	}()
	setPachdLogLevel()
	env := serviceenv.InitWithKube(serviceenv.NewConfiguration(config))
	return runFullMode(env)
}

// doLocalMode runs the full pachd server without Kubernetes: etcd is embedded
// (started before config load in runLocal), storage is local, and pipeline
// workers are spawned as processes by the in-process localclient runtime.
func doLocalMode(config interface{}) (retErr error) {
	defer func() {
		if retErr != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
		}
	}()
	setPachdLogLevel()
	env := serviceenv.InitServiceEnv(serviceenv.NewConfiguration(config))
	workerBinary := os.Getenv("PACH_WORKER_BINARY")
	if workerBinary == "" {
		workerBinary = filepath.Join(filepath.Dir(os.Args[0]), "worker")
	}
	rt, err := localclient.NewRuntime(localclient.Options{
		WorkerBinary: workerBinary,
		EtcdHost:     env.EtcdHost,
		EtcdPort:     env.EtcdPort,
		PachdPort:    env.Port,
		WorkerPort:   env.PPSWorkerPort,
		LocalDir:     os.Getenv("PACH_LOCAL_DIR"),
		StorageRoot:  env.StorageRoot,
		Runtime:      os.Getenv("PACH_WORKER_RUNTIME"),
		DaemonPodName: env.PachdPodName,
	})
	if err != nil {
		return errors.Wrapf(err, "could not start local worker runtime")
	}
	env.InstallLocalKube(localclient.NewClientset(rt))
	// Kill worker processes when the daemon exits (normally or via signal), so
	// they don't linger and hold their loopback IPs hostage for the next run.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		rt.Close()
		os.Exit(0)
	}()
	return runFullMode(env)
}

// setPachdLogLevel applies the LOG_LEVEL env var to the logrus logger and
// installs the Jaeger tracer if configured.
func setPachdLogLevel() {
	switch logLevel := os.Getenv("LOG_LEVEL"); logLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "info", "":
		log.SetLevel(log.InfoLevel)
	default:
		log.Errorf("Unrecognized log level %s, falling back to default of \"info\"", logLevel)
		log.SetLevel(log.InfoLevel)
	}
	// must run InstallJaegerTracer before pach client initialization
	if endpoint := tracing.InstallJaegerTracerFromEnv(); endpoint != "" {
		log.Printf("connecting to Jaeger at %q", endpoint)
	} else {
		log.Printf("no Jaeger collector found (JAEGER_COLLECTOR_SERVICE_HOST not set)")
	}
}

// runFullMode sets up and runs every pachd server (external gRPC, internal
// gRPC, HTTP, githook, S3 gateway, prometheus). It is shared by k8s mode
// (doFullMode) and local mode (doLocalMode).
func runFullMode(env *serviceenv.ServiceEnv) (retErr error) {
	debug.SetGCPercent(env.GCPercent)
	if env.EtcdPrefix == "" {
		env.EtcdPrefix = col.DefaultPrefix
	}
	clusterID, err := getClusterID(env.GetEtcdClient())
	if err != nil {
		return errors.Wrapf(err, "getClusterID")
	}
	var reporter *metrics.Reporter
	if env.Metrics {
		reporter = metrics.NewReporter(clusterID, env)
	}
	// (bryce) Do we have to use etcd client v2 here for sharder? Might want to re-visit this later.
	etcdAddress := fmt.Sprintf("http://%s", net.JoinHostPort(env.EtcdHost, env.EtcdPort))
	etcdClientV2 := getEtcdClient(etcdAddress)
	ip, err := netutil.ExternalIP()
	if err != nil {
		return errors.Wrapf(err, "error getting pachd external ip")
	}
	address := net.JoinHostPort(ip, fmt.Sprintf("%d", env.PeerPort))
	sharder := shard.NewSharder(
		etcdClientV2,
		env.NumShards,
		env.Namespace,
	)
	go func() {
		if err := sharder.AssignRoles(address); err != nil {
			log.Printf("error from sharder.AssignRoles: %s", grpcutil.ScrubGRPC(err))
		}
	}()
	router := shard.NewRouter(
		sharder,
		grpcutil.NewDialer(
			grpc.WithInsecure(),
		),
		address,
	)
	pfsCacheSize, err := strconv.Atoi(env.PFSCacheSize)
	if err != nil {
		return errors.Wrapf(err, "atoi")
	}
	if pfsCacheSize == 0 {
		pfsCacheSize = defaultTreeCacheSize
	}
	treeCache, err := hashtree.NewCache(pfsCacheSize)
	if err != nil {
		return errors.Wrapf(err, "lru.New")
	}
	kubeNamespace := env.Namespace
	requireNoncriticalServers := !env.RequireCriticalServersOnly
	// Setup External Pachd GRPC Server.
	externalServer, err := grpcutil.NewServer(context.Background(), true)
	if err != nil {
		return err
	}
	if err := logGRPCServerSetup("External Pachd", func() error {
		txnEnv := &txnenv.TransactionEnv{}
		memoryRequestBytes, err := units.RAMInBytes(env.MemoryRequest)
		if err != nil {
			return err
		}
		var pfsAPIServer pfs_server.APIServer
		if err := logGRPCServerSetup("PFS API", func() error {
			pfsAPIServer, err = pfs_server.NewAPIServer(env, txnEnv, path.Join(env.EtcdPrefix, env.PFSEtcdPrefix), treeCache, env.StorageRoot, memoryRequestBytes)
			if err != nil {
				return err
			}
			pfsclient.RegisterAPIServer(externalServer.Server, pfsAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var ppsAPIServer pps_server.APIServer
		if err := logGRPCServerSetup("PPS API", func() error {
			ppsAPIServer, err = pps_server.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
				kubeNamespace,
				env.WorkerImage,
				env.WorkerSidecarImage,
				env.WorkerImagePullPolicy,
				env.StorageRoot,
				env.StorageBackend,
				env.StorageHostPath,
				env.CacheRoot,
				env.IAMRole,
				env.ImagePullSecret,
				env.NoExposeDockerSocket,
				reporter,
				env.WorkerUsesRoot,
				env.PPSWorkerPort,
				env.Port,
				env.HTTPPort,
				env.PeerPort,
				env.GCPercent,
			)
			if err != nil {
				return err
			}
			ppsclient.RegisterAPIServer(externalServer.Server, ppsAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if !env.StorageV2 {
			if env.ExposeObjectAPI {
				if err := logGRPCServerSetup("Block API", func() error {
					// Generally the object API should not be exposed publicly, but
					// TestGarbageCollection uses it and it may help with debugging
					blockAPIServer, err := pfs_server.NewBlockAPIServer(
						env.StorageRoot,
						0 /* = blockCacheBytes (disable cache) */, env.StorageBackend,
						etcdAddress,
						true /* duplicate */)
					if err != nil {
						return err
					}
					pfsclient.RegisterObjectAPIServer(externalServer.Server, blockAPIServer)
					return nil
				}); err != nil {
					return err
				}
			}
		}
		var authAPIServer authserver.APIServer
		if err := logGRPCServerSetup("Auth API", func() error {
			authAPIServer, err = authserver.NewAuthServer(
				env, txnEnv, path.Join(env.EtcdPrefix, env.AuthEtcdPrefix), true, requireNoncriticalServers)
			if err != nil {
				return err
			}
			authclient.RegisterAPIServer(externalServer.Server, authAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var transactionAPIServer txnserver.APIServer
		if err := logGRPCServerSetup("Transaction API", func() error {
			transactionAPIServer, err = txnserver.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
			)
			if err != nil {
				return err
			}
			transactionclient.RegisterAPIServer(externalServer.Server, transactionAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Enterprise API", func() error {
			enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
				env, path.Join(env.EtcdPrefix, env.EnterpriseEtcdPrefix))
			if err != nil {
				return err
			}
			eprsclient.RegisterAPIServer(externalServer.Server, enterpriseAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Admin API", func() error {
			adminclient.RegisterAPIServer(externalServer.Server, adminserver.NewAPIServer(address, env.StorageRoot, &adminclient.ClusterInfo{
				ID:           clusterID,
				DeploymentID: env.DeploymentID,
			}))
			return nil
		}); err != nil {
			return err
		}
		healthServer := health.NewHealthServer()
		if err := logGRPCServerSetup("Health", func() error {
			healthclient.RegisterHealthServer(externalServer.Server, healthServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Version API", func() error {
			versionpb.RegisterAPIServer(externalServer.Server, version.NewAPIServer(version.Version, version.APIServerOptions{}))
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Debug", func() error {
			debugclient.RegisterDebugServer(externalServer.Server, debugserver.NewDebugServer(
				env,
				env.PachdPodName,
				nil,
			))
			return nil
		}); err != nil {
			return err
		}
		txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer, ppsAPIServer)
		if _, err := externalServer.ListenTCP("", env.Port); err != nil {
			return err
		}
		healthServer.Ready()
		return nil
	}); err != nil {
		return err
	}
	// Setup Internal Pachd GRPC Server.
	internalServer, err := grpcutil.NewServer(context.Background(), false)
	if err != nil {
		return err
	}
	if err := logGRPCServerSetup("Internal Pachd", func() error {
		txnEnv := &txnenv.TransactionEnv{}
		cacheServer := cache_server.NewCacheServer(router, env.NumShards)
		go func() {
			if err := sharder.RegisterFrontends(address, []shard.Frontend{cacheServer}); err != nil {
				log.Printf("error from sharder.RegisterFrontend %s", grpcutil.ScrubGRPC(err))
			}
		}()
		go func() {
			if err := sharder.Register(address, []shard.Server{cacheServer}); err != nil {
				log.Printf("error from sharder.Register %s", grpcutil.ScrubGRPC(err))
			}
		}()
		cache_pb.RegisterGroupCacheServer(internalServer.Server, cacheServer)
		if !env.StorageV2 {
			blockCacheBytes, err := units.RAMInBytes(env.BlockCacheBytes)
			if err != nil {
				return errors.Wrapf(err, "units.RAMInBytes")
			}
			if err := logGRPCServerSetup("Block API", func() error {
				blockAPIServer, err := pfs_server.NewBlockAPIServer(
					env.StorageRoot, blockCacheBytes, env.StorageBackend, etcdAddress, false)
				if err != nil {
					return err
				}
				pfsclient.RegisterObjectAPIServer(internalServer.Server, blockAPIServer)
				return nil
			}); err != nil {
				return err
			}
		}
		memoryRequestBytes, err := units.RAMInBytes(env.MemoryRequest)
		if err != nil {
			return err
		}
		var pfsAPIServer pfs_server.APIServer
		if err := logGRPCServerSetup("PFS API", func() error {
			pfsAPIServer, err = pfs_server.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
				treeCache,
				env.StorageRoot,
				memoryRequestBytes,
			)
			if err != nil {
				return err
			}
			pfsclient.RegisterAPIServer(internalServer.Server, pfsAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var ppsAPIServer pps_server.APIServer
		if err := logGRPCServerSetup("PPS API", func() error {
			ppsAPIServer, err = pps_server.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.EtcdPrefix, env.PPSEtcdPrefix),
				kubeNamespace,
				env.WorkerImage,
				env.WorkerSidecarImage,
				env.WorkerImagePullPolicy,
				env.StorageRoot,
				env.StorageBackend,
				env.StorageHostPath,
				env.CacheRoot,
				env.IAMRole,
				env.ImagePullSecret,
				env.NoExposeDockerSocket,
				reporter,
				env.WorkerUsesRoot,
				env.PPSWorkerPort,
				env.Port,
				env.HTTPPort,
				env.PeerPort,
				env.GCPercent,
			)
			if err != nil {
				return err
			}
			ppsclient.RegisterAPIServer(internalServer.Server, ppsAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var authAPIServer authserver.APIServer
		if err := logGRPCServerSetup("Auth API", func() error {
			authAPIServer, err = authserver.NewAuthServer(
				env,
				txnEnv,
				path.Join(env.EtcdPrefix, env.AuthEtcdPrefix),
				false,
				requireNoncriticalServers,
			)
			if err != nil {
				return err
			}
			authclient.RegisterAPIServer(internalServer.Server, authAPIServer)
			return nil
		}); err != nil {
			return err
		}
		var transactionAPIServer txnserver.APIServer
		if err := logGRPCServerSetup("Transaction API", func() error {
			transactionAPIServer, err = txnserver.NewAPIServer(
				env,
				txnEnv,
				path.Join(env.EtcdPrefix, env.PFSEtcdPrefix),
			)
			if err != nil {
				return err
			}
			transactionclient.RegisterAPIServer(internalServer.Server, transactionAPIServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Enterprise API", func() error {
			enterpriseAPIServer, err := eprsserver.NewEnterpriseServer(
				env, path.Join(env.EtcdPrefix, env.EnterpriseEtcdPrefix))
			if err != nil {
				return err
			}
			eprsclient.RegisterAPIServer(internalServer.Server, enterpriseAPIServer)
			return nil
		}); err != nil {
			return err
		}
		healthServer := health.NewHealthServer()
		if err := logGRPCServerSetup("Health", func() error {
			healthclient.RegisterHealthServer(internalServer.Server, healthServer)
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Version API", func() error {
			versionpb.RegisterAPIServer(internalServer.Server, version.NewAPIServer(version.Version, version.APIServerOptions{}))
			return nil
		}); err != nil {
			return err
		}
		if err := logGRPCServerSetup("Admin API", func() error {
			adminclient.RegisterAPIServer(internalServer.Server, adminserver.NewAPIServer(address, env.StorageRoot, &adminclient.ClusterInfo{
				ID:           clusterID,
				DeploymentID: env.DeploymentID,
			}))
			return nil
		}); err != nil {
			return err
		}
		txnEnv.Initialize(env, transactionAPIServer, authAPIServer, pfsAPIServer, ppsAPIServer)
		if _, err := internalServer.ListenTCP("", env.PeerPort); err != nil {
			return err
		}
		healthServer.Ready()
		return nil
	}); err != nil {
		return err
	}
	// Create the goroutines for the servers.
	// Any server error is considered critical and will cause Pachd to exit.
	// The first server that errors will have its error message logged.
	errChan := make(chan error, 1)
	go waitForError("External Pachd GRPC Server", errChan, true, func() error {
		return externalServer.Wait()
	})
	go waitForError("Internal Pachd GRPC Server", errChan, true, func() error {
		return internalServer.Wait()
	})
	go waitForError("HTTP Server", errChan, requireNoncriticalServers, func() error {
		httpServer, err := pach_http.NewHTTPServer(address)
		if err != nil {
			return err
		}
		server := http.Server{
			Addr:    fmt.Sprintf(":%v", env.HTTPPort),
			Handler: httpServer,
		}

		certPath, keyPath, err := tls.GetCertPaths()
		if err != nil {
			log.Warnf("pfs-over-HTTP - TLS disabled: %v", err)
			return server.ListenAndServe()
		}

		cLoader := tls.NewCertLoader(certPath, keyPath, tls.CertCheckFrequency)
		err = cLoader.LoadAndStart()
		if err != nil {
			return errors.Wrapf(err, "couldn't load TLS cert for pfs-over-http: %v", err)
		}

		server.TLSConfig = &gotls.Config{GetCertificate: cLoader.GetCertificate}

		return server.ListenAndServeTLS(certPath, keyPath)
	})
	go waitForError("Githook Server", errChan, requireNoncriticalServers, func() error {
		return githook.RunGitHookServer(address, etcdAddress, path.Join(env.EtcdPrefix, env.PPSEtcdPrefix))
	})
	go waitForError("S3 Server", errChan, requireNoncriticalServers, func() error {
		server, err := s3.Server(env.S3GatewayPort, s3.NewMasterDriver(), func() (*client.APIClient, error) {
			return client.NewFromAddress(fmt.Sprintf("localhost:%d", env.PeerPort))
		})
		if err != nil {
			return err
		}
		certPath, keyPath, err := tls.GetCertPaths()
		if err != nil {
			log.Warnf("s3gateway TLS disabled: %v", err)
			return server.ListenAndServe()
		}
		cLoader := tls.NewCertLoader(certPath, keyPath, tls.CertCheckFrequency)
		// Read TLS cert and key
		err = cLoader.LoadAndStart()
		if err != nil {
			return errors.Wrapf(err, "couldn't load TLS cert for s3gateway: %v", err)
		}
		server.TLSConfig = &gotls.Config{GetCertificate: cLoader.GetCertificate}
		return server.ListenAndServeTLS(certPath, keyPath)
	})
	go waitForError("Prometheus Server", errChan, requireNoncriticalServers, func() error {
		promPort := assets.PrometheusPort
		if envPort := os.Getenv("PROMETHEUS_PORT"); envPort != "" {
			p, err := strconv.Atoi(envPort)
			if err != nil {
				return errors.Wrapf(err, "invalid PROMETHEUS_PORT %q", envPort)
			}
			promPort = p
		}
		http.Handle("/metrics", promhttp.Handler())
		return http.ListenAndServe(fmt.Sprintf(":%v", promPort), nil)
	})
	return <-errChan
}

func getEtcdClient(etcdAddress string) discovery.Client {
	return discovery.NewEtcdClient(etcdAddress)
}

const clusterIDKey = "cluster-id"

func getClusterID(client *etcd.Client) (string, error) {
	resp, err := client.Get(context.Background(),
		clusterIDKey)

	// if it's a key not found error then we create the key
	if resp.Count == 0 {
		// This might error if it races with another pachd trying to set the
		// cluster id so we ignore the error.
		client.Put(context.Background(), clusterIDKey, uuid.NewWithoutDashes())
	} else if err != nil {
		return "", err
	} else {
		// We expect there to only be one value for this key
		id := string(resp.Kvs[0].Value)
		return id, nil
	}

	return getClusterID(client)
}

func logGRPCServerSetup(name string, f func() error) (retErr error) {
	log.Printf("started setting up %v GRPC Server", name)
	defer func() {
		if retErr != nil {
			retErr = errors.Wrapf(retErr, "error setting up %v GRPC Server", name)
		} else {
			log.Printf("finished setting up %v GRPC Server", name)
		}
	}()
	return f()
}

func waitForError(name string, errChan chan error, required bool, f func() error) {
	if err := f(); !errors.Is(err, http.ErrServerClosed) {
		if !required {
			log.Errorf("error setting up and/or running %v: %v", name, err)
		} else {
			errChan <- errors.Wrapf(err, "error setting up and/or running %v (use --require-critical-servers-only deploy flag to ignore errors from noncritical servers)", name)
		}
	}
}
