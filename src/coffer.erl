%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer).

-export([start/0, stop/0]).
-export([list_storages/0, add_storage/3, remove_storage/1, get_storage/1]).
-export([new_upload/2,
         upload/2, upload/3,
         new_stream/2, fetch/1, fetch/2,
         simple_fetch/2,
         delete/2,
         stat/2,
         start_enumerate/1, enumerate/1, enumerate/2, stop_enumerate/1,
         foldl/3, all/1, foreach/2]).
-export([blob_exists/2]).

-define(DEFAULT_WINDOW, 4096).

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


new_stream(StoragePid, BlobRef) when is_binary(BlobRef)->
    new_stream(StoragePid, {BlobRef, ?DEFAULT_WINDOW});

new_stream(StoragePid, {BlobRef, Window}) ->
    coffer_storage:new_stream(StoragePid, {BlobRef, Window}).

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
    case new_stream(StoragePid, BlobRef) of
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



stat(StoragePid, BlobRefs) ->
    coffer_storage:stat(StoragePid, BlobRefs).

start_enumerate(StoragePid) ->
    coffer_storage:enumerate(StoragePid).

enumerate(EnumeratePid) ->
    enumerate(EnumeratePid, infinity).

enumerate(EnumeratePid, Timeout) ->
    receive
        {blob, {BlobRef, Size}, EnumeratePid} ->
            EnumeratePid ! {next, self()},
            {ok, {BlobRef, Size}};
        {error, Reason, EnumeratePid} ->
            {error, Reason};
        {done, EnumeratePid} ->
            done
    after Timeout ->
        kill_receiver(EnumeratePid)
    end.

stop_enumerate(EnumeratePid) ->
    stop_sync(EnumeratePid).

foldl(StoragePid, Func, InitState) ->
    case start_enumerate(StoragePid) of
        {ok, EnumeratePid} ->
            do_foldl(EnumeratePid, Func, InitState);
        Error ->
            Error
    end.

all(StoragePid) ->
    foldl(StoragePid, fun(Info, Acc) ->
                Acc ++ [Info]
        end, []).

foreach(StoragePid, Func) ->
    Wrapper = fun(Info, _State) ->
            Func(Info)
    end,
    case foldl(StoragePid, Wrapper, nil) of
        {error, Error, _} ->
            throw({error, Error});
        _ ->
            ok
    end.

blob_exists(StoragePid, BlobRef) ->
    coffer_storage:blob_exists(StoragePid, BlobRef).

%% ------------------------------------------------------------------
%% Internal functions
%% ------------------------------------------------------------------

do_foldl(EnumeratePid, Func, State) ->
    case enumerate(EnumeratePid) of
        {ok, {_BlobRef, _Size}=Info} ->
            NewState = Func(Info, State),
            do_foldl(EnumeratePid, Func, NewState);
        {error, Error} ->
            lager:error("foldl error: ~p~n", [Error]),
            {error, Error, State};
        done ->
            State
    end.


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

stop_sync(Pid) when not is_pid(Pid)->
    ok;
stop_sync(Pid) ->
    MRef = erlang:monitor(process, Pid),
    try
        catch unlink(Pid),
        catch exit(Pid, stop),
        receive
        {'DOWN', MRef, _, _, _} ->
            ok
        end
    after
        erlang:demonitor(MRef, [flush])
    end.
