%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.
%%%

-module(coffer_econfig).
-behaviour(coffer_config_backend).

-export([init/1,
         all/1,
         set_value/3, set_value/4,
         get_value/2, get_value/3, get_value/4,
         delete_value/2, delete_value/3,
         terminate/1]).


init(_) ->
    DefaultConfDir =  filename:join([code:root_dir(), "./etc"]),
    ConfFile = coffer_app:get_app_env(config_file,
                                      filename:join(DefaultConfDir,
                                                    "coffer.ini")),
    %% open the config file
    econfig:open_config(coffer_config, ConfFile),

    %% subscribe to config changes
    econfig:subscribe(coffer_config),

    {ok, coffer_config}.

all(Ref) ->
    econfig:cfg2list(Ref).

set_value(Ref, Section, Value) ->
    econfig:set_value(Ref, Section, Value).

set_value(Ref, Section, Key, Value) ->
    econfig:set_value(Ref, Section, Key, Value).

get_value(Ref, Section) ->
    econfig:get_value(Ref, Section).

get_value(Ref, Section, Key) ->
    econfig:get_value(Ref, Section, Key).

get_value(Ref, Section, Key, Default) ->
    econfig:get_value(Ref, Section, Key, Default).

delete_value(Ref, Section) ->
    econfig:delete_value(Ref, Section).

delete_value(Ref, Section, Key) ->
    econfig:delete_value(Ref, Section, Key).


terminate(Ref) ->
    econfig:unregister_config(Ref).
