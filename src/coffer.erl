%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer).

-export([start/0, stop/0]).
-export([list_storages/0, add_storage/3, remove_storage/1, get_storage/1]).
-export([new_upload/2,
         upload/2, upload/3,
         fetch_stream/2, fetch/1, fetch/2,
         simple_fetch/2,
         delete/2,
         all/1, foldl/3, foldl/4, foreach/2]).

% --- Application ---

%% @doc Start the coffer application. Useful when testing using the shell.
start() ->
    coffer_deps:ensure(),
    application:load(coffer),
    coffer_app:ensure_deps_started(),
    application:start(coffer).

%% @doc Start the coffer application. Useful when testing using the shell.
stop() ->
    application:stop(coffer).

% --- storage API ---

-spec list_storages() -> list().
list_storages() ->
    coffer_server:list().

-spec add_storage(Name :: binary(), Backend :: atom(), Config :: list()) -> ok.
add_storage(Name, Backend, Config) ->
    coffer_server:add(Name, Backend, Config).

-spec remove_storage(Name :: binary()) -> ok.
remove_storage(Name) ->
    coffer_server:remove(Name).

-spec get_storage(Name :: binary()) -> pid().
get_storage(Name) ->
    coffer_server:get(Name).

% --- Storage API ---

new_upload(StoragePid, BlobRef) ->
    coffer_storage:new_upload(StoragePid, BlobRef).

upload({_Pid, _Conf}=Receiver, Bin) ->
    upload(Receiver, Bin, infinity).

upload({ReceiverPid, Config}, eob, Timeout) ->
    ReceiverPid ! {eob, self(), Config},
    receive
        {ok, ReceiverPid, UploadedSize} ->
            {ok, UploadedSize};
        {error, Reason} ->
            {error, Reason};
        {'EXIT', ReceiverPid, Reason} ->
            {error, Reason}
    after Timeout ->
        kill_receiver(ReceiverPid)
    end;

upload({ReceiverPid, Config}, Bin, Timeout) ->
    ReceiverPid ! {data, self(), Bin, Config},
    receive
        {ack, ReceiverPid, NewConfig} ->
            {ok, {ReceiverPid, NewConfig}};
        {error, Reason} ->
            {error, Reason};
        {'EXIT', ReceiverPid, Reason} ->
            {error, Reason}
    after Timeout ->
        kill_receiver(ReceiverPid)
    end.


fetch_stream(StoragePid, BlobRef) when is_binary(BlobRef)->
    fetch_stream(StoragePid, {BlobRef, 0});

fetch_stream(StoragePid, {BlobRef, Window}) ->
    coffer_storage:fetch_stream(StoragePid, {BlobRef, Window}).

fetch(StreamPid) ->
    fetch(StreamPid, infinity).

-spec fetch(StreamPid :: pid(), Timeout :: integer())
    -> {ok, binary()}
    | {ok, coffer_eob}
    | {error, term()}.
fetch(StreamPid, Timeout) ->
    receive
        {data, Data, StreamPid} ->
            StreamPid ! {ack, self()},
            {ok, Data};
        {coffer_eob, StreamPid} ->
            {ok, coffer_eob};
        {error, StreamPid, Reason} ->
            {error, Reason}
    after Timeout ->
        kill_receiver(StreamPid)
    end.

simple_fetch(StoragePid, BlobRef) ->
    case fetch_stream(StoragePid, BlobRef) of
        {ok, StreamPid} ->
            case fetch(StreamPid) of
                {ok, Data} ->
                    {ok, coffer_eob} = fetch(StreamPid),
                    {ok, Data};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

delete(StoragePid, BlobRef) ->
    coffer_storage:delete(StoragePid, BlobRef).

all(StoragePid) ->
    coffer_storage:all(StoragePid).

foldl(StoragePid, Func, InitState) ->
    foldl(StoragePid, Func, InitState, []).

foldl(StoragePid, Func, InitState, Options) ->
    coffer_storage:foldl(StoragePid, Func, InitState, Options).

foreach(StoragePid, Func) ->
    coffer_storage:foreach(StoragePid, Func).

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

-spec kill_receiver(pid()) -> any() | {error, any()}.
kill_receiver(Pid) ->
    Monitor = erlang:monitor(process, Pid),
    unlink(Pid), % or we'll kill ourself :O
    exit(Pid, timeout),
    receive
        {partial, Pid, Size} ->
            erlang:demonitor(Monitor, [flush]),
            {partial, Size};
        {'DOWN', _, process, Pid, Reason}  ->
            {error, Reason}
    end.
