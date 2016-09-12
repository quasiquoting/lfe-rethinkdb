REBAR3 ?= @$(shell which rebar3)

.PHONY: all
all: compile

clean:
	$(REBAR3) clean

compile:
	$(REBAR3) compile

test: eunit

eunit:
	$(REBAR3) eunit

shell:
	$(REBAR3) shell
