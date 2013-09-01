%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Args), {I, {I, start_link, Args}, permanent, 5000, worker, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    case whereis(couch_server_sup) of
        undefined ->
            %% make sure to start coffer_config first
            {ok, ConfigPid} = coffer_config:start_link(),

            %% init the base specs
            Config = {coffer_config,
                     {coffer_config, start_link1, [ConfigPid]},
                     permanent, brutal_kill, worker, [coffer_config]},
            CofferServer = ?CHILD(coffer_server, [[]]),
            Children = [Config, CofferServer],
            BaseSpec = {{one_for_one, 1, 60}, Children},

            %% start the supervisor
            {ok, SupPid} = supervisor:start_link({local, ?MODULE}, ?MODULE,
                                              BaseSpec),

            %% unlink the config pid from this process
            unlink(ConfigPid),

            %% finally return the supervisor pid
            {ok, SupPid};
        _ ->
            {error, already_started}
    end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(BaseSpec) ->
    {ok, BaseSpec}.
