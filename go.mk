GITROOT ?= $(shell git rev-parse --show-toplevel)
# Ensure the Go version in .go_version is installed and used.
GOROOT?=$(shell $(GITROOT)/script/run_with_go_ver go env GOROOT)
GO:=$(GOROOT)/bin/go
export PATH:=$(GOROOT)/bin:$(PATH)

GOOSBUILD:=$(GITROOT)/build/$(shell $(GO) env GOOS)
APPROVALS=$(GOOSBUILD)/approvals
GENPACKAGE=$(GOOSBUILD)/genpackage
GOIMPORTS=$(GOOSBUILD)/goimports
GOLICENSER=$(GOOSBUILD)/go-licenser
GOLINT=$(GOOSBUILD)/golint
MAGE=$(GOOSBUILD)/mage
REVIEWDOG=$(GOOSBUILD)/reviewdog
STATICCHECK=$(GOOSBUILD)/staticcheck
ELASTICPACKAGE=$(GOOSBUILD)/elastic-package
TERRAFORMDOCS=$(GOOSBUILD)/terraform-docs
GOBENCH=$(GOOSBUILD)/gobench
PROTOC=$(GOOSBUILD)/protoc/bin/protoc
PROTOC_GEN_GO_VTPROTO=$(GOOSBUILD)/protoc-gen-go-vtproto
PROTOC_GEN_GO=$(GOOSBUILD)/protoc-gen-go
PROTOC_GEN_GO_GRPC=$(GOOSBUILD)/protoc-gen-go-grpc
PROTOC_GEN_VALIDATE=$(GOOSBUILD)/protoc-gen-validate
PROTOC_GEN_VALIDATE_MOD=github.com/envoyproxy/protoc-gen-validate
PROTOC_GEN_VALIDATE_PATH=$(shell $(GO) env GOPATH)/src/$(PROTOC_GEN_VALIDATE_MOD)
APM_SERVER_VERSION=$(shell grep defaultBeatVersion $(GITROOT)/cmd/version.go | cut -d'=' -f2 | tr -d '" ')

##############################################################################
# Rules for creating and installing build tools.
##############################################################################

BIN_MAGE=$(GOOSBUILD)/bin/mage

# BIN_MAGE is the standard "mage" binary.
$(BIN_MAGE): $(GITROOT)/go.mod
	$(GO) build -o $@ github.com/magefile/mage

# MAGE is the compiled magefile.
$(MAGE): $(GITROOT)/magefile.go $(BIN_MAGE)
	$(BIN_MAGE) -compile=$@

$(GOLINT): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< golang.org/x/lint/golint

$(GOIMPORTS): $(GITROOT)/go.mod
	$(GO) build -o $@ golang.org/x/tools/cmd/goimports

$(STATICCHECK): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< honnef.co/go/tools/cmd/staticcheck

$(GOLICENSER): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< github.com/elastic/go-licenser

$(REVIEWDOG): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< github.com/reviewdog/reviewdog/cmd/reviewdog

$(ELASTICPACKAGE): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< -ldflags '-X github.com/elastic/elastic-package/internal/version.CommitHash=anything' github.com/elastic/elastic-package

$(TERRAFORMDOCS): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< github.com/terraform-docs/terraform-docs

$(GOBENCH): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< github.com/elastic/gobench

$(PROTOC):
	@./tools/protoc-install.sh

$(PROTOC_GEN_GO_VTPROTO): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< github.com/planetscale/vtprotobuf/cmd/protoc-gen-go-vtproto

$(PROTOC_GEN_GO): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< google.golang.org/protobuf/cmd/protoc-gen-go

$(PROTOC_GEN_GO_GRPC): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< google.golang.org/grpc/cmd/protoc-gen-go-grpc

$(PROTOC_GEN_VALIDATE): $(GITROOT)/tools/go.mod
	$(GO) build -o $@ -modfile=$< $(PROTOC_GEN_VALIDATE_MOD)

$(PROTOC_GEN_VALIDATE_PATH): $(GITROOT)/tools/go.mod
	@GO111MODULE=off $(GO) get -d $(PROTOC_GEN_VALIDATE_MOD)

.PHONY: $(APPROVALS)
$(APPROVALS):
	@$(GO) build -o $@ github.com/elastic/apm-server/approvaltest/cmd/check-approvals
