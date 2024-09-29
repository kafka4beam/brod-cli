KAFKA_VERSION ?= 3.6
export KAFKA_VERSION
all: compile release

.PHONY: compile
compile:
	@rebar3 compile

.PHONY: lint
lint:
	@rebar3 lint

.PHONY: test
test:
	@rebar3 eunit -v --cover_export_name ut-$(KAFKA_VERSION)

.PHONY: clean
clean:
	@rebar3 clean
	@rm -rf _build
	@rm -rf ebin deps doc
	@rm -f pipe.testdata

## build escript and a release, and copy escript to release bin dir
.PHONY: release
release:
	@rebar3 do escriptize,release
	@cp _build/default/bin/brodcli _build/default/rel/brod/bin/

.PHONY: cover
cover:
	@rebar3 cover -v

.PHONY: dialyzer
dialyzer:
	@rebar3 dialyzer

.PHONY: fmt
fmt:
	@rebar3 fmt -w
