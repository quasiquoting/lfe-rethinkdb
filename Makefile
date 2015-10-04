PROJECT   = lefink
REBAR    := $(shell which rebar)
VERSION   = 0.1

.PHONY: all
all: get-deps compile repl

get-deps:
	$(REBAR) get-deps

compile:
	$(REBAR) co

test: eunit

eunit:
	$(REBAR) eu skip_deps=true

repl:
	rlwrap -a 'dummy' lfe -pa ebin -pa deps/*/ebin -I deps/gpb/include
