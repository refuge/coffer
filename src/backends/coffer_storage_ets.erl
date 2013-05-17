%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage_ets).
-behaviour(coffer_storage).

%% ------------------------------------------------------------------
%% coffer_storage Function Exports
%% ------------------------------------------------------------------

-export([do_start/1, do_stop/1]).
-export([do_open/1, do_close/1]).
-export([do_put/3, do_get/3, do_delete/2]).
-export([do_all/1, do_foldl/4, do_foreach/2]).

%% ------------------------------------------------------------------
%% coffer_storage Function Definitions
%% ------------------------------------------------------------------

do_start(Config) ->
    case Config of
        [{Name, Options}] ->
            Tid = ets:new(Name, Options),
            {ok, Tid};
        _ ->
            lager:error("Wrong config: ~p", [Config]),
            {error, wrong_config}
    end.    

do_stop(Tid) ->
    ets:delete(Tid),
    ok.

do_open(Tid) ->
    {ok, Tid}.

do_close(Tid) ->
    {ok, Tid}.

do_put(Tid, Id, Bin) when is_binary(Bin) ->
    ets:insert(Tid, {Id, Bin}),
    {ok, Tid};
do_put(_Tid, _Id, _Chunk) ->
    % TODO to be done ;-)
    {error, not_supported}.

do_get(Tid, Id, _Options) ->
    case ets:lookup(Tid, Id) of
        [{_Key, Value}] ->
            {ok, Value};
        _ ->
            {error, not_found}
    end.

do_delete(Tid, Id) ->
    case ets:lookup(Tid, Id) of
        [{_Key, _Value}] ->
            ets:delete(Tid, Id),
            {ok, Tid};
        _ ->
            {error, not_found}
    end.

do_all(Tid) ->
    Value = ets:foldl(
        fun({Key, _}, Acc) ->
            [Key|Acc]
        end,
        [],
        Tid
    ),
    {ok, Value}.

do_foldl(Tid, Func, InitState, _Options) ->
    Value = ets:foldl(
        Func,
        InitState,
        Tid
    ),
    {ok, Value}.

do_foreach(Tid, Func) ->
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
