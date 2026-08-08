// Package localetcd runs an embedded etcd server inside the pachd process.
// It is used by local (single-node, k8s-free) deployments so that a pachd
// daemon does not require a separately-deployed etcd cluster.
package localetcd

import (
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/coreos/pkg/capnslog"
	"golang.org/x/net/context"

	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
)

// Env wraps an embedded etcd server and its client.
type Env struct {
	Etcd       *embed.Etcd
	ClientURL  string
	EtcdClient *etcd.Client
}

// Start launches an embedded etcd server with its data directory under
// 'dataDir', listening on 127.0.0.1:'port'. The etcd v2 HTTP API is enabled,
// because pachd's sharder still uses it (see cmd/pachd/main.go). The returned
// Env's EtcdClient is ready to use; call Close to shut everything down.
func Start(dataDir string, port uint16) (*Env, error) {
	// etcd's WAL segment size is a global setting; tests already rely on this
	// pattern, and a smaller segment size keeps the local data dir compact.
	clientURL, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", port))
	if err != nil {
		return nil, errors.Wrapf(err, "could not parse etcd client URL")
	}

	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(dataDir, "etcd_data")
	cfg.WalDir = filepath.Join(dataDir, "etcd_wal")
	cfg.EnableV2 = true // pachd's sharder uses the etcd v2 HTTP API
	cfg.MaxTxnOps = 10000
	cfg.LPUrls = []url.URL{}
	cfg.LCUrls = []url.URL{*clientURL}
	cfg.LogOutput = "default"

	// Throw away noisy messages from etcd; the daemon's own logs are enough.
	capnslog.SetGlobalLogLevel(capnslog.CRITICAL)

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "could not start embedded etcd")
	}

	env := &Env{
		Etcd:      e,
		ClientURL: clientURL.String(),
	}
	// Wait for etcd to be ready before returning, so the daemon never races
	// etcd startup.
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Close()
		return nil, errors.New("embedded etcd failed to start within 60s")
	}
	select {
	case err := <-e.Err():
		if err != nil {
			e.Close()
			return nil, errors.Wrapf(err, "embedded etcd exited during startup")
		}
	default:
	}

	env.EtcdClient, err = etcd.New(etcd.Config{
		Context:     context.Background(),
		Endpoints:   []string{clientURL.String()},
		DialOptions: client.DefaultDialOptions(),
	})
	if err != nil {
		e.Close()
		return nil, errors.Wrapf(err, "could not connect to embedded etcd")
	}
	return env, nil
}

// Close shuts down the embedded etcd server and its client.
func (env *Env) Close() error {
	var retErr error
	if env.EtcdClient != nil {
		if err := env.EtcdClient.Close(); err != nil {
			retErr = err
		}
	}
	if env.Etcd != nil {
		env.Etcd.Close()
	}
	return retErr
}
