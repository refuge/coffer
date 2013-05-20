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
         new_stream/3,
         delete/2,
         enumerate/2]).

-export([receive_loop/3]).
-export([stream_loop/4]).
-export([enumerate_loop/2]).
%% ------------------------------------------------------------------
%% coffer_storage Function Definitions
%% ------------------------------------------------------------------

init(Config) ->
    case Config of
        [{Name, Options0}] ->
            Options = [public, ordered_set | Options0],
            Tid = ets:new(Name, Options),
            STid = ets:new(coffer_storage_ets_size, [public,
                                                     ordered_set]),

            {ok, {Tid, STid}};
        _ ->
            lager:error("Wrong config: ~p", [Config]),
            {error, wrong_config}
    end.

terminate({Tid, STid}) ->
    ets:delete(Tid),
    ets:delete(STid),
    ok.

new_receiver(BlobRef, From, {Tid, _STid}=State) ->
    case ets:lookup(Tid, BlobRef) of
        [] ->
            ReceiverPid = spawn_link(?MODULE, receive_loop, [BlobRef,
                                                             From,
                                                             State]),
            {ok, {ReceiverPid, nil}, State};
        [{BlobRef, Blob}|_] ->
            S = size(Blob),
            {error, {already_exists, BlobRef, S}, State}
    end.

receive_loop(BlobRef, From, State) ->
    TmpBlobRef = << BlobRef/binary, ".tmp" >>,
    Self = self(),
    MonRef = erlang:monitor(process, From),
    do_receive_loop(BlobRef, TmpBlobRef, Self, From, State),
    erlang:demonitor(MonRef, [flush]).


do_receive_loop(BlobRef, TmpBlobRef, Self, From, {Tid, STid}=State) ->
    receive
        {data, From, Bin, Config} ->
            lager:info("Partial upload to ~p~n", [TmpBlobRef]),
            case ets:lookup(Tid, TmpBlobRef) of
                [] ->
                    Size = byte_size(Bin),
                    ets:insert(Tid, {TmpBlobRef, {Bin, Size}});
                [{TmpBlobRef, {OldBin, _OldSize}}] ->
                    NewBin = << OldBin/binary, Bin/binary>>,
                    Size = byte_size(NewBin),
                    ets:insert(Tid, {TmpBlobRef, {NewBin, Size}})
            end,
            From ! {ack, Self, Config},
            do_receive_loop(BlobRef, TmpBlobRef, Self, From, State);
        {eob, From, _Config} ->
            case ets:lookup(Tid, TmpBlobRef) of
                [] ->
                    From ! {error, not_found};
                [{TmpBlobRef, {Bin, Size}}] ->
                    lager:info("End upload: rename ~p to ~p~n",
                               [TmpBlobRef, BlobRef]),
                    ets:insert(Tid, {BlobRef, {Bin, Size}}),
                    ets:insert(STid, {BlobRef, Size}),
                    ets:delete(Tid, TmpBlobRef),
                    ets:delete(STid, TmpBlobRef),
                    From ! {ok, Self, Size}
            end;
        {'DOWN', _, process, From, _} ->
           exit(normal)
    end.

new_stream({BlobRef, Window}, To, {Tid, _STid}=State) ->
    case ets:member(Tid, BlobRef) of
        true ->
            StreamPid = spawn_link(?MODULE, stream_loop, [BlobRef, Window, To,
                                                          Tid]),
            {ok, StreamPid, State};
        _ ->
            {error, not_found, State}
    end.

stream_loop(BlobRef, Window, To, Tid) ->
    MonRef = erlang:monitor(process, To),
    [{BlobRef, {Bin, _Size}}] = ets:lookup(Tid, BlobRef),
    do_stream_loop(Bin, Window, To),
    erlang:demonitor(MonRef, [flush]).

do_stream_loop(<<>>, _Window, To) ->
    To ! {coffer_eob, self()};
do_stream_loop(Bin, Window, To)
        when byte_size(Bin) >= Window, Window > 0 ->
    << Chunk:Window/binary, Rest/binary >> = Bin,

    To ! {data, Chunk, self()},
    receive
        {ack, To} ->
            do_stream_loop(Rest, Window, To);
        {'DOWN', _, process, To, _} ->
            exit(normal)
    end;
do_stream_loop(Bin, Window, To) ->
    To ! {data, Bin, self()},
    receive
        {ack, To} ->
            do_stream_loop(<<>>, Window, To);
        {'DOWN', _, process, To, _} ->
            exit(normal)
    end.

delete(BlobRef, {Tid, _STid}=State) ->
    case ets:member(Tid, BlobRef) of
        true ->
            ets:delete(Tid, BlobRef),
            {ok, State};
        _ ->
            {error, not_found, State}
    end.


enumerate(To, {_Tid, STid}=State) ->
    EnumeratePid = spawn_link(?MODULE, enumerate_loop, [To, STid]),
    {ok, EnumeratePid, State}.

enumerate_loop(To, STid) ->
    MonRef = erlang:monitor(process, To),
    First = ets:first(STid),
    do_enumerate_loop(First, To, STid),
    erlang:demonitor(MonRef, [flush]).

do_enumerate_loop('$end_of_table', To, _STid) ->
    To ! {done, self()};
do_enumerate_loop(BlobRef, To, STid) ->
    [{BlobRef, Size}] = ets:lookup(STid, BlobRef),
    To ! {blob, {BlobRef, Size}, self()},
    receive
        {ack, To} ->
            do_enumerate_loop(ets:next(STid, BlobRef), To, STid);
        {'DOWN', _, process, To, _} ->
            exit(normal)
    end.
