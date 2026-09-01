VGO=go
GOFILES := $(shell find internal pkg -name '*.go' -print)
GOBIN := $(shell $(VGO) env GOPATH)/bin
LINT := $(GOBIN)/golangci-lint
MOCKERY := $(GOBIN)/mockery

# Expect that FireFly compiles with CGO disabled
CGO_ENABLED=0
GOGC=30

.DELETE_ON_ERROR:

all: build test go-mod-tidy
test: deps lint
		$(VGO) test ./internal/... ./pkg/... -cover -coverprofile=coverage.txt -covermode=atomic -timeout=30s ${TEST_FLAGS}
coverage.html:
		$(VGO) tool cover -html=coverage.txt
coverage: test coverage.html
lint: 
		$(VGO) install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.64.8
		GOGC=20 $(LINT) run -v --timeout 5m

${MOCKERY}:
		$(VGO) install github.com/vektra/mockery/v2@v2.43.1

define makemock
mocks: mocks-$(strip $(1))-$(strip $(2))
mocks-$(strip $(1))-$(strip $(2)): ${MOCKERY}
	${MOCKERY} --case underscore --dir $(1) --name $(2) --outpkg $(3) --output mocks/$(strip $(3))
endef

$(eval $(call makemock, pkg/ffcapi,             API,                         ffcapimocks))
$(eval $(call makemock, pkg/txhandler,          TransactionHandler,          txhandlermocks))
$(eval $(call makemock, pkg/txhandler,          ManagedTxEventHandler,       txhandlermocks))
$(eval $(call makemock, internal/metrics,       TransactionHandlerMetrics,   metricsmocks))
$(eval $(call makemock, internal/metrics,       EventMetricsEmitter,         metricsmocks))
$(eval $(call makemock, internal/confirmations, Manager,                     confirmationsmocks))
$(eval $(call makemock, internal/persistence,   Persistence,                 persistencemocks))
$(eval $(call makemock, internal/persistence,   TransactionPersistence,      persistencemocks))
$(eval $(call makemock, internal/persistence,   RichQuery,                   persistencemocks))
$(eval $(call makemock, internal/ws,            WebSocketChannels,           wsmocks))
$(eval $(call makemock, internal/ws,            WebSocketServer,             wsmocks))
$(eval $(call makemock, internal/events,        ManagedStream,               eventsmocks))
$(eval $(call makemock, internal/apiclient,     FFTMClient,                  apiclientmocks))

go-mod-tidy: .ALWAYS
		$(VGO) mod tidy
build: test
.ALWAYS: ;
clean:
		$(VGO) clean
deps:
		$(VGO) get ./internal/... ./pkg/...
		$(VGO) get -t ./internal/... ./pkg/...
reference:
		$(VGO) test ./pkg/fftm -timeout=10s -tags docs
