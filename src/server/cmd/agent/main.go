// Command agent runs the broker node agent (see src/server/pkg/agent): the
// light "kubelet" that lets pachd place pipeline workers on this node. It
// registers the node tags it serves with the pachd-side broker and spawns
// and kills worker processes on command. All logic lives in the agent
// package so the test suite can run the agent in-process; this binary is
// the flag/environment front end.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"

	"github.com/pachyderm/pachyderm/src/server/pkg/agent"
)

func main() {
	log.SetLevel(log.InfoLevel)

	var opts agent.Options
	flag.StringVar(&opts.BrokerAddr, "broker", os.Getenv("PACH_BROKER_ADDRESS"), "broker address (host:port); defaults to the discovered pachd's broker port")
	tags := flag.String("tag", os.Getenv("PACH_AGENT_TAG"), "comma-separated node tags this agent serves")
	flag.StringVar(&opts.LocalDir, "local-dir", envOr("PACH_AGENT_DIR", filepath.Join(os.TempDir(), "pach-agent")), "agent data dir (worker scratch, logs, pid file)")
	flag.StringVar(&opts.WorkerBinary, "worker-binary", os.Getenv("PACH_WORKER_BINARY"), "path to the pachd worker binary")
	flag.StringVar(&opts.PachdAddr, "pachd-address", os.Getenv("PACH_PEER_ADDRESS"), "pachd address (host:port) workers connect to; defaults to pachd's PEER_PORT (loopback)")
	flag.StringVar(&opts.Join, "join", os.Getenv("PACH_JOIN"), "join a specific pachd host (host or host:port); skips mDNS discovery")
	flag.IntVar(&opts.EtcdPort, "etcd-port", envInt("PACH_ETCD_PORT", 2379), "pachd's etcd port (used with --join/--pachd-address)")
	flag.IntVar(&opts.BrokerPort, "broker-port", envInt("PACH_BROKER_PORT", 30660), "pachd's broker port (used when --broker is not set)")
	flag.BoolVar(&opts.Discover, "discover", envBool("PACH_AGENT_DISCOVER", true), "discover pachd via mDNS when no address is given")
	flag.StringVar(&opts.Runtime, "runtime", envOr("PACH_WORKER_RUNTIME", "docker"), "how workers run: docker (default) or process")
	flag.StringVar(&opts.DockerBin, "docker", envOr("PACH_DOCKER_BIN", "docker"), "path to the docker binary (docker runtime)")
	flag.Parse()

	if *tags == "" {
		fmt.Fprintln(os.Stderr, "agent: at least one node tag is required (--tag or PACH_AGENT_TAG)")
		os.Exit(1)
	}
	if opts.WorkerBinary == "" {
		fmt.Fprintln(os.Stderr, "agent: worker binary is required (--worker-binary or PACH_WORKER_BINARY)")
		os.Exit(1)
	}
	for _, t := range strings.Split(*tags, ",") {
		if t = strings.TrimSpace(t); t != "" {
			opts.Tags = append(opts.Tags, t)
		}
	}
	if len(opts.Tags) == 0 {
		fmt.Fprintln(os.Stderr, "agent: no valid tags in --tag")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	if err := agent.Run(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "agent: %v\n", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}
