%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    {ok, Pid} = supervisor:start_link({local, ?MODULE}, ?MODULE, []).

    ok = start_listeners(),
    {ok, Pid}


init([]) ->
    {ok, {{one_for_all, 10, 10}, []}}.
