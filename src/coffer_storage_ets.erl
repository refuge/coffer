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
-export([new_receiver/3,
         handle_get/3, handle_delete/2]).
-export([handle_all/1, handle_foldl/4, handle_foreach/2]).


-export([receive_loop/3]).

%% ------------------------------------------------------------------
%% coffer_storage Function Definitions
%% ------------------------------------------------------------------

init(Config) ->
    case Config of
        [{Name, Options0}] ->
            Options = [public, ordered_set | Options0],
            Tid = ets:new(Name, Options),
            {ok, Tid};
        _ ->
            lager:error("Wrong config: ~p", [Config]),
            {error, wrong_config}
    end.

terminate(Tid) ->
    ets:delete(Tid),
    ok.

new_receiver(BlobRef, From, Tid) ->
    case ets:lookup(Tid, BlobRef) of
        [] ->
            ReceiverPid = spawn_link(?MODULE, receive_loop, [BlobRef,
                                                             From,
                                                             Tid]),
            {ok, {ReceiverPid, nil}, Tid};
        [{BlobRef, Blob}|_] ->
            S = size(Blob),
            {error, {already_exists, BlobRef, S}, Tid}
    end.

receive_loop(BlobRef, From, Tid) ->
    TmpBlobRef = << BlobRef/binary, ".tmp" >>,
    Self = self(),
    MonRef = erlang:monitor(process, From),
    do_receive_loop(BlobRef, TmpBlobRef, Self, From, Tid),
    erlang:demonitor(MonRef, [flush]).


do_receive_loop(BlobRef, TmpBlobRef, Self, From, Tid) ->
    receive
        {data, From, Bin, Config} ->
            lager:info("Partial upload to ~p~n", [TmpBlobRef]),
            case ets:lookup(Tid, TmpBlobRef) of
                [] ->
                    ets:insert(Tid, {TmpBlobRef, Bin});
                [{TmpBlobRef, OldBin}] ->
                    NewBin = << OldBin/binary, Bin/binary>>,
                    ets:insert(Tid, {TmpBlobRef, NewBin})
            end,
            From ! {ack, Self, Config},
            do_receive_loop(BlobRef, TmpBlobRef, Self, From, Tid);
        {eob, From, _Config} ->
            case ets:lookup(Tid, TmpBlobRef) of
                [] ->
                    From ! {error, not_found};
                [{TmpBlobRef, Bin}] ->
                    lager:info("End upload: rename ~p to ~p~n",
                               [TmpBlobRef, BlobRef]),
                    ets:insert(Tid, {BlobRef, Bin}),
                    ets:delete(Tid, TmpBlobRef),
                    From ! {ok, Self, size(Bin)}
            end;
        {'DOWN', _, process, From, _} ->
           exit(normal)
    end.

handle_get(Tid, BlobRef, _Options) ->
    case ets:lookup(Tid, BlobRef) of
        [{_Key, Value}] ->
            {ok, Value};
        _ ->
            {error, not_found}
    end.

handle_delete(Tid, BlobRef) ->
    case ets:lookup(Tid, BlobRef) of
        [{_Key, _Value}] ->
            ets:delete(Tid, BlobRef),
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
