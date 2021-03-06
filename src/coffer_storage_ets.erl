%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage_ets).
-behaviour(coffer_backend).

%% ------------------------------------------------------------------
%% coffer_storage Function Exports
%% ------------------------------------------------------------------

-export([init/2, terminate/1]).
-export([new_receiver/3,
         new_stream/3,
         delete/2,
         enumerate/2,
         stat/2]).
-export([blob_exists/2]).

%% ------------------------------------------------------------------
%% Internal API
%% ------------------------------------------------------------------

-export([receive_loop/3]).
-export([stream_loop/4]).
-export([enumerate_loop/2]).

%% ------------------------------------------------------------------
%% coffer_storage Function Definitions
%% ------------------------------------------------------------------

init(StorageName, Config) ->
    case Config of
        [{Name, Options0}] ->
            Options = [public, ordered_set | Options0],
            Tid = ets:new(Name, Options),
            STid = ets:new(coffer_storage_ets_size, [public,
                                                     ordered_set]),

            {ok, {StorageName, Tid, STid}};
        _ ->
            lager:error("Wrong config: ~p", [Config]),
            {error, bad_config}
    end.

terminate({_Name, Tid, STid}) ->
    ets:delete(Tid),
    ets:delete(STid),
    ok.

new_receiver(BlobRef, From, {_Name, Tid, _STid}=State) ->
    case coffer_blob:validate_ref(BlobRef) of
        ok ->
            case ets:lookup(Tid, BlobRef) of
                [] ->
                    ReceiverPid = spawn(?MODULE, receive_loop, [BlobRef,
                                                                     From,
                                                                     State]),
                    {ok, {ReceiverPid, nil}, State};
                [{BlobRef, Blob}|_] ->
                    S = size(Blob),
                    {error, {already_exists, BlobRef, S}, State}
            end;
        error ->
            {error, invalid_blobref}
    end.

receive_loop(BlobRef, From, {Name, _, _}=State) ->
    TmpBlobRef = << BlobRef/binary, ".tmp" >>,
    Self = self(),
    MonRef = erlang:monitor(process, From),
    coffer_storage:register_receiver(BlobRef),
    do_receive_loop(BlobRef, TmpBlobRef, Self, From, State),
    coffer_storage:notify(Name, {uploaded, BlobRef}),
    coffer_storage:unregister_receiver(BlobRef),
    erlang:demonitor(MonRef, [flush]).


do_receive_loop(BlobRef, TmpBlobRef, Self, From, {_Name, Tid, STid}=State) ->
    receive
        {data, From, Bin, Config} ->
            lager:info("Partial upload to ~p~n", [TmpBlobRef]),
            case ets:lookup(Tid, TmpBlobRef) of
                [] ->
                    Size = byte_size(Bin),
                    ets:insert(Tid, {TmpBlobRef, {Bin, Size}}),
                    ets:insert(STid, {TmpBlobRef, Size});
                [{TmpBlobRef, {OldBin, _OldSize}}] ->
                    NewBin = << OldBin/binary, Bin/binary>>,
                    Size = byte_size(NewBin),
                    ets:insert(Tid, {TmpBlobRef, {NewBin, Size}}),
                    ets:insert(STid, {TmpBlobRef, Size})
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

new_stream({BlobRef, Window}, To, {_Name, Tid, _STid}=State) ->
    case ets:member(Tid, BlobRef) of
        true ->
            StreamPid = spawn(?MODULE, stream_loop, [BlobRef, Window, To,
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

delete(BlobRef, {Name, Tid, _STid}=State) ->
    case ets:member(Tid, BlobRef) of
        true ->
            ets:delete(Tid, BlobRef),
            coffer_storage:notify(Name, {deleted, BlobRef}),
            {ok, State};
        _ ->
            {error, not_found, State}
    end.


enumerate(To, {_Name, _Tid, STid}=State) ->
    EnumeratePid = spawn(?MODULE, enumerate_loop, [To, STid]),
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
    S = Size - 4,
    case BlobRef of
        << _:S/binary, ".tmp" >> ->
            do_enumerate_loop(ets:next(STid, BlobRef), To, STid);
        _ ->
            To ! {blob, {BlobRef, Size}, self()},
            receive
                {next, To} ->
                    do_enumerate_loop(ets:next(STid, BlobRef), To, STid);
                {'DOWN', _, process, To, _} ->
                    exit(normal)
            end
    end.

stat(BlobRefs0, {_Name, _Tid, STid}=State) ->
    %% before stating anything check the blob refs
    BlobRefs = lists:foldl(fun(BlobRef, Acc) ->
                    case coffer_blob:validate_ref(BlobRef) of
                        ok ->
                            Acc ++ [BlobRef];
                        error ->
                            Acc
                    end
            end, [], BlobRefs0),

    {Found, Missing} = lists:foldl(fun(BlobRef, {F, M}) ->
                    case ets:lookup(STid, BlobRef) of
                        [{BlobRef, _Size}=KV] ->
                            {[KV | F], M};
                        [] ->
                            {F, [BlobRef | M]}
                    end
            end, {[], []}, BlobRefs),
    {Partials, Missing1} = case Missing of
        [] ->
            {[], Missing};
        _ ->
            ToFind = [{BlobRef, << BlobRef/binary, ".tmp" >>}
                      || BlobRef <- Missing],
            lists:foldl(fun({BlobRef, TmpBlobRef}, {P, M}) ->
                        case ets:lookup(STid, TmpBlobRef) of
                            [{TmpBlobRef, Size}] ->
                                {[{BlobRef, Size} | P], M};
                            [] ->
                                {P, [BlobRef | M]}
                        end
                end, {[], []}, ToFind)
    end,
    {ok, {lists:reverse(Found), lists:reverse(Missing1),
          lists:reverse(Partials)}, State}.

blob_exists(BlobRef, {_Name, Tid, _STid}) ->
    case ets:member(Tid, BlobRef) of
        true ->
            ok;
        _ ->
            {error, not_found}
    end.

