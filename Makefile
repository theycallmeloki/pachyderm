#### VARIABLES
# TESTFLAGS: flags for test
# DOCKER_BUILD_FLAGS: flags for 'docker build'
####

RUN= # used by go tests to decide which tests to run (i.e. passed to -run)
# Don't set the version to the git hash in CI, as it breaks the go build cache.
ifdef CIRCLE_BRANCH
	export VERSION_ADDITIONAL = -CIbuild
	export GC_FLAGS = ""
else
	export VERSION_ADDITIONAL = -$(shell git log --pretty=format:%H | head -n 1)
	export GC_FLAGS = "all=-trimpath=${PWD}"
endif

export CLIENT_ADDITIONAL_VERSION=github.com/pachyderm/pachyderm/src/client/version.AdditionalVersion=$(VERSION_ADDITIONAL)
export LD_FLAGS=-X $(CLIENT_ADDITIONAL_VERSION)
export DOCKER_BUILD_FLAGS



CHLOGFILE = ${PWD}/../changelog.diff
export GOVERSION = $(shell cat etc/compile/GO_VERSION)
GORELSNAP = #--snapshot # uncomment --snapshot if you want to do a dry run.
SKIP = #\# # To skip push to docker and github remove # in front of #
GORELDEBUG = #--debug # uncomment --debug for verbose goreleaser output

ifeq ($(OS),Windows_NT)
	GOPATH = $(shell cygpath -u $(shell go env GOPATH))
else
	GOPATH = $(shell go env GOPATH)
endif

GOBIN = $(GOPATH)/bin

ifdef TRAVIS_BUILD_NUMBER
	# Upper bound for travis test timeout
	TIMEOUT = 3600s
else
ifndef TIMEOUT
	# You should be able to specify your own timeout, but by default we'll use the same bound as travis
	TIMEOUT = 3600s
endif
endif

install:
	# GOPATH/bin must be on your PATH to access these binaries:
	go install -ldflags "$(LD_FLAGS)" -gcflags "$(GC_FLAGS)" ./src/server/cmd/pachctl

install-clean:
	@# Need to blow away pachctl binary if its already there
	@rm -f $(GOBIN)/pachctl
	@make install

install-doc:
	go install -gcflags "$(GC_FLAGS)" ./src/server/cmd/pachctl-doc

doc-custom: install-doc install-clean
	./etc/build/doc.sh

doc-reference-refresh: install-doc install-clean
	./etc/build/reference_refresh.sh

doc:
	@make VERSION_ADDITIONAL= doc-custom

point-release:
	@./etc/build/make_changelog.sh $(CHLOGFILE)
	@VERSION_ADDITIONAL= ./etc/build/make_release.sh
	@echo "Release completed"

# Run via 'make VERSION_ADDITIONAL=-rc2 release-candidate' to specify a version string
release-candidate:
	@make custom-release

custom-release:
	echo "" > $(CHLOGFILE)
	@VERSION_ADDITIONAL=$(VERSION_ADDITIONAL) ./etc/build/make_release.sh "Custom"
	# Need to check for homebrew updates from release-pachctl-custom

# This is getting called from etc/build/make_release.sh
# Git tag is force pushed. We are assuming if the same build is done again, it is done with intent
release:
	@git tag -f -am "Release tag v$(VERSION)" v$(VERSION)
	$(SKIP) @git push -f origin v$(VERSION)
	@make release-helper
	@make release-pachctl
	@echo "Release $(VERSION) completed"

release-helper: release-docker-images docker-push docker-push-pipeline-build

release-docker-images:
	DOCKER_BUILDKIT=1 goreleaser release -p 1 $(GORELSNAP) $(GORELDEBUG) --skip-publish --rm-dist -f goreleaser/docker.yml
	DOCKER_BUILDKIT=1 goreleaser release -p 1 $(GORELSNAP) $(GORELDEBUG) --skip-publish --rm-dist -f goreleaser/docker-build-pipelines.yml

release-pachctl:
	@goreleaser release -p 1 $(GORELSNAP) $(GORELDEBUG) --release-notes=$(CHLOGFILE) --rm-dist -f goreleaser/pachctl.yml


docker-build-pipeline-build: install
	VERSION=$$(pachctl version --client-only) DOCKER_BUILDKIT=1 \
	  goreleaser release -p 1 --snapshot $(GORELDEBUG) --skip-publish --rm-dist -f goreleaser/docker-build-pipelines.yml

docker-build-proto:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_proto etc/proto

docker-build-netcat:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_netcat etc/netcat


docker-build-kafka:
	docker build -t kafka-demo etc/testing/kafka

docker-build-spout-test:
	docker build -t spout-test etc/testing/spout





docker-build-test-entrypoint:
	docker build $(DOCKER_BUILD_FLAGS) -t pachyderm_entrypoint etc/testing/entrypoint









# launch-release-vm is like launch-dev-vm but it doesn't build pachctl locally, and uses the same
# version of pachd associated with the current pachctl (useful if you want to start a VM with a
# point-release version of pachd, instead of whatever's in the current branch)







test-proto-static:
	./etc/proto/test_no_changes.sh || echo "Protos need to be recompiled; run 'DOCKER_BUILD_FLAGS=--no-cache make proto'."



proto: docker-build-proto
	./etc/proto/build.sh

# Run all the tests. Note! This is no longer the test entrypoint for travis
test: lint test-pfs-server test-cmds test-libs test-vault test-auth test-worker test-admin test-pps

test-pfs-server:
	./etc/testing/start_postgres.sh
	./etc/testing/pfs_server.sh $(TIMEOUT)

test-pfs-storage:
	./etc/testing/start_postgres.sh
	go test  -count=1 ./src/server/pkg/storage/... -timeout $(TIMEOUT)

test-pps: docker-build-spout-test docker-build-test-entrypoint
	@# Use the count flag to disable test caching for this test suite.
	go test -v -count=1 ./src/server -parallel 1 -timeout $(TIMEOUT) $(RUN)

test-cmds:
	go install -v ./src/testing/match
	CGOENABLED=0 go test -v -count=1 ./src/server/cmd/pachctl/cmd
	go test -v -count=1 ./src/server/pkg/deploy/cmds -timeout $(TIMEOUT)
	go test -v -count=1 ./src/server/pfs/cmds -timeout $(TIMEOUT)
	go test -v -count=1 ./src/server/pps/cmds -timeout $(TIMEOUT)
	go test -v -count=1 ./src/server/config -timeout $(TIMEOUT)
	@# TODO(msteffen) does this test leave auth active? If so it must run last
	go test -v -count=1 ./src/server/auth/cmds -timeout $(TIMEOUT)

test-transaction:
	go test -count=1 ./src/server/transaction/server/testing -timeout $(TIMEOUT)

test-client:
	go test -count=1 -cover $$(go list ./src/client/...)

test-object-clients:
	# The parallelism is lowered here because these tests run several pachd
	# deployments in kubernetes which may contest resources.
	go test -count=1 ./src/server/pkg/obj/testing -timeout $(TIMEOUT) -parallel=2

test-libs:
	go test -count=1 ./src/client/pkg/grpcutil -timeout $(TIMEOUT)
	go test -count=1 ./src/server/pkg/collection -timeout $(TIMEOUT) -vet=off
	go test -count=1 ./src/server/pkg/hashtree -timeout $(TIMEOUT)
	go test -count=1 ./src/server/pkg/cert -timeout $(TIMEOUT)
	go test -count=1 ./src/server/pkg/localcache -timeout $(TIMEOUT)
	go test -count=1 ./src/server/pkg/work -timeout $(TIMEOUT)

test-vault:
	kill $$(cat /tmp/vault.pid) || true
	./src/plugin/vault/etc/start-vault.sh
	./src/plugin/vault/etc/pach-auth.sh --activate
	./src/plugin/vault/etc/setup-vault.sh
	go test -v -count=1 ./src/plugin/vault -timeout $(TIMEOUT)
	./src/plugin/vault/etc/pach-auth.sh --delete-all

test-s3gateway-conformance:
	@if [ -z $$CONFORMANCE_SCRIPT_PATH ]; then \
	  echo "Missing environment variable 'CONFORMANCE_SCRIPT_PATH'"; \
	  exit 1; \
	fi
	$(CONFORMANCE_SCRIPT_PATH) --s3tests-config=etc/testing/s3gateway/s3tests.conf --ignore-config=etc/testing/s3gateway/ignore.conf --runs-dir=etc/testing/s3gateway/runs

test-s3gateway-integration:
	@if [ -z $$INTEGRATION_SCRIPT_PATH ]; then \
	  echo "Missing environment variable 'INTEGRATION_SCRIPT_PATH'"; \
	  exit 1; \
	fi
	$(INTEGRATION_SCRIPT_PATH) http://localhost:30600 --access-key=none --secret-key=none

test-s3gateway-unit:
	go test -v -count=1 ./src/server/pfs/s3 -timeout $(TIMEOUT)

test-fuse:
	CGOENABLED=0 go test -count=1 -cover $$(go list ./src/server/... | grep '/src/server/pfs/fuse')

test-local:
	CGOENABLED=0 go test -count=1 -cover -short $$(go list ./src/server/... | grep -v '/src/server/pfs/fuse') -timeout $(TIMEOUT)

test-auth:
	yes | pachctl delete all
	go test -v -count=1 ./src/server/auth/server/testing -timeout $(TIMEOUT) $(RUN)

test-admin:
	go test -v -count=1 ./src/server/admin/server -timeout $(TIMEOUT) $(RUN)

test-tls:
	./etc/testing/test_tls.sh

test-worker: test-worker-helper

test-worker-helper:
	go test -v -count=1 ./src/server/worker/ -timeout $(TIMEOUT)

clean:

compatibility:
	./etc/build/compatibility.sh


























lint:
	etc/testing/lint.sh

spellcheck:
	@mdspell doc/*.md doc/**/*.md *.md --en-us --ignore-numbers --ignore-acronyms --report --no-suggestions

.PHONY: \
	install \
	install-clean \
	install-doc \
	doc-custom \
	doc \
	point-release \
	release-candidate \
	custom-release \
	release \
	release-helper \
	release-docker-images \
	release-pachctl \
	docker-build-pipeline-build \
	docker-build-proto \
	docker-build-netcat \
	docker-build-kafka \
	docker-build-spout-test \
	docker-build-test-entrypoint \
	test-proto-static \
	proto \
	test \
	test-pfs-server \
	test-pfs-storage \
	test-pps \
	test-cmds \
	test-transaction \
	test-client \
	test-libs \
	test-vault \
	test-s3gateway-conformance \
	test-s3gateway-integration \
	test-s3gateway-unit \
	test-fuse \
	test-local \
	test-auth \
	test-admin \
	test-tls \
	test-worker \
	test-worker-helper \
	clean \
	compatibility \
	lint \
	spellcheck
