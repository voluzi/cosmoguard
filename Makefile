VERSION ?= $(shell git describe --tags --exclude 'chart/*' --abbrev=0)
COMMIT ?= $(shell git rev-parse HEAD)
BUILD_TARGETS := build install


BUILDDIR ?= $(CURDIR)/build

all: install

$(BUILD_TARGETS):
	CGO_ENABLED=0 go $@ \
		-mod=readonly \
		-ldflags="-s -w -X github.com/voluzi/cosmoguard/pkg/cosmoguard.Version=$(VERSION) -X github.com/voluzi/cosmoguard/pkg/cosmoguard.CommitHash=$(COMMIT)" \
		./cmd/cosmoguard

mod:
	go mod tidy

test:
	go test ./...

# test-race runs the full suite with the race detector. Slower (2-10x) but
# catches data races; required on every PR before merge.
test-race:
	go test -race -timeout 120s ./...

# test-cover produces a coverage profile suitable for reports/uploads.
test-cover:
	go test -race -coverprofile=coverage.out -covermode=atomic ./...

# fuzz runs each fuzz target for a fixed wall-clock budget. Use a longer
# fuzztime in nightly CI; FUZZTIME=10s on every PR.
FUZZTIME ?= 10s
fuzz:
	go test -run=^$$ -fuzz=FuzzEnvInterpolate          -fuzztime=$(FUZZTIME) ./pkg/cosmoguard
	go test -run=^$$ -fuzz=FuzzParseJsonRpcMessage     -fuzztime=$(FUZZTIME) ./pkg/cosmoguard
	go test -run=^$$ -fuzz=FuzzHttpRuleCompile         -fuzztime=$(FUZZTIME) ./pkg/cosmoguard
	go test -run=^$$ -fuzz=FuzzCompileOriginAllowlist  -fuzztime=$(FUZZTIME) ./pkg/cosmoguard

clean:
	rm -rf $(BUILDDIR)/ coverage.out

# helm.package builds an OCI-ready chart tarball under $(BUILDDIR). The chart
# version and appVersion are both the release version, so the chart defaults
# to the image built from the same tag.
$(BUILDDIR)/:
	mkdir -p $(BUILDDIR)/

helm.package: $(BUILDDIR)/
	helm package helm/cosmoguard \
		--version $(VERSION:v%=%) \
		--app-version $(VERSION:v%=%) \
		-d $(BUILDDIR)

.PHONY: all $(BUILD_TARGETS) test test-race test-cover fuzz clean helm.package