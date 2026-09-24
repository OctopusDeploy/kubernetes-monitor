tidy:
	@echo Tidy go.mod dependencies
	@go mod tidy

download: tidy
	@echo Download go.mod dependencies
	@go mod download

lint: golangci-lint
	@echo Linting codebase
	$(GOLANGCI_LINT) run

format: golangci-lint
	@echo Linting and formatting codebase
	$(GOLANGCI_LINT) run --fix 

install-tools: download
	@echo Installing tools from tools.go
	@cat tools.go | grep _ | awk -F'"' '{print $$2}' | xargs -tI % go install %

clean-generated: 
	@rm internal/protos/*.pb.go &

generate-proto: install-tools clean-generated
	@protoc \
	--experimental_allow_proto3_optional \
	--go_out=internal/protos/ \
	--go_opt=paths=source_relative \
	--go-grpc_out=internal/protos/ \
	--go-grpc_opt=paths=source_relative \
	--proto_path=internal/protos/definitions \
	internal/protos/definitions/*.proto

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
GOLANGCI_LINT_VERSION ?= v2.13.2

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef
