%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage_ets).
-behaviour(coffer_backend).

%% ------------------------------------------------------------------
%% coffer_storage Function Exports
%% ------------------------------------------------------------------

-export([init/1, terminate/1]).
-export([handle_put/3, handle_get/3, handle_delete/2]).
-export([handle_all/1, handle_foldl/4, handle_foreach/2]).

%% ------------------------------------------------------------------
%% coffer_storage Function Definitions
%% ------------------------------------------------------------------

init(Config) ->
    case Config of
        [{Name, Options}] ->
            Tid = ets:new(Name, Options),
            {ok, Tid};
        _ ->
            lager:error("Wrong config: ~p", [Config]),
            {error, wrong_config}
    end.

terminate(Tid) ->
    ets:delete(Tid),
    ok.

handle_put(Tid, Id, Bin) when is_binary(Bin) ->
    ets:insert(Tid, {Id, Bin}),
    {ok, Tid};
handle_put(_Tid, _Id, _Chunk) ->
    % TODO to be done ;-)
    {error, not_supported}.

handle_get(Tid, Id, _Options) ->
    case ets:lookup(Tid, Id) of
        [{_Key, Value}] ->
            {ok, Value};
        _ ->
            {error, not_found}
    end.

handle_delete(Tid, Id) ->
    case ets:lookup(Tid, Id) of
        [{_Key, _Value}] ->
            ets:delete(Tid, Id),
            {ok, Tid};
        _ ->
            {error, not_found}
    end.

handle_all(Tid) ->
    Value = ets:foldl(
        fun({Key, _}, Acc) ->
            [Key|Acc]
        end,
        [],
        Tid
    ),
    {ok, Value}.

handle_foldl(Tid, Func, InitState, _Options) ->
    Value = ets:foldl(
        Func,
        InitState,
        Tid
    ),
    {ok, Value}.

handle_foreach(Tid, Func) ->
    ets:foldl(
        fun({Key, _}, _) ->
            Func(Key),
            []
        end,
        [],
        Tid
    ),
    ok.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
