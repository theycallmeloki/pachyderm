package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	flag "github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pachyderm/pachyderm/v2/src/internal/cmdutil"
	"github.com/pachyderm/pachyderm/v2/src/internal/log"
	"github.com/pachyderm/pachyderm/v2/src/internal/pachd"
	"github.com/pachyderm/pachyderm/v2/src/internal/pctx"
	"github.com/pachyderm/pachyderm/v2/src/internal/proc"
	"github.com/pachyderm/pachyderm/v2/src/internal/serviceenv"
	_ "github.com/pachyderm/pachyderm/v2/src/internal/task/taskprotos"
)

var (
	mode          string
	readiness     bool
	notifySignals = []os.Signal{os.Interrupt}
)

func init() {
	flag.StringVar(&mode, "mode", "full", "Pachd currently supports four modes: full, enterprise, sidecar and paused. Full includes everything you need in a full pachd node. Enterprise runs the Enterprise Server. Sidecar runs only PFS, the Auth service, and a stripped-down version of PPS.  Paused runs all APIs other than PFS and PPS; it is intended to enable taking database backups.")
	flag.BoolVar(&readiness, "readiness", false, "Run readiness check.")
	flag.Parse()
}

func main() {
	log.InitPachdLogger()
	ctx, cancel := signal.NotifyContext(pctx.Background(""), notifySignals...)
	defer cancel()

	logMode := func(mode string) {
		log.Info(ctx, "pachd: starting", zap.String("mode", mode))
	}
	go proc.MonitorSelf(ctx)

	switch {
	case readiness:
		cmdutil.Main(ctx, doReadinessCheck, &serviceenv.GlobalConfiguration{})
	case mode == "full", mode == "", mode == "$(MODE)":
		// Because of the way Kubernetes environment substitution works,
		// a reference to an unset variable is not replaced with the
		// empty string, but instead the reference is passed unchanged;
		// because of this, '$(MODE)' should be recognized as an unset —
		// i.e., default — mode.
		logMode("full")
		cmdutil.Main(ctx, pachd.FullMode, &serviceenv.PachdFullConfiguration{})
	case mode == "enterprise":
		logMode("enterprise")
		cmdutil.Main(ctx, pachd.EnterpriseMode, &serviceenv.EnterpriseServerConfiguration{})
	case mode == "sidecar":
		logMode("sidecar")
		cmdutil.Main(ctx, pachd.SidecarMode, &serviceenv.PachdFullConfiguration{})
	case mode == "pachw":
		cmdutil.Main(ctx, pachd.PachwMode, &serviceenv.PachdFullConfiguration{})
	case mode == "paused":
		logMode("paused")
		cmdutil.Main(ctx, pachd.PausedMode, &serviceenv.PachdFullConfiguration{})
	default:
		log.Error(ctx, "pachd: unrecognized mode", zap.String("mode", mode))
		fmt.Printf("unrecognized mode: %s\n", mode)
	}
}

func doReadinessCheck(ctx context.Context, config interface{}) error {
	env := serviceenv.InitPachOnlyEnv(ctx, serviceenv.NewConfiguration(config))
	return env.GetPachClient(ctx).Health()
}
