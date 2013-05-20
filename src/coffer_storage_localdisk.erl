%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.
%%%
-module(coffer_storage_localdisk).
-behaviour(coffer_backend).

-include_lib("kernel/include/file.hrl").

%% ------------------------------------------------------------------
%% coffer_storage Function Exports
%% ------------------------------------------------------------------

-export([init/2, terminate/1]).
-export([new_receiver/3,
         new_stream/3,
         delete/2,
         enumerate/2,
         stat/2]).


-export([receive_loop/4]).
-export([stream_loop/3]).
-export([enumerate_loop/2]).

-record(ldst, {name,
               path}).


init(StorageName, Config) ->
    case proplists:get_value(path, Config) of
        undefined ->
            lager:info("~p: invalid path~n", [StorageName]),
            {error, {bad_config, invalid_path}};
        Path ->
            case filelib:ensure_dir(Path) of
                ok ->
                    {ok, #ldst{name=StorageName, path=Path}};
                {error, Reason} ->
                    {error, {bad_config, Reason}}
            end
    end.

terminate(_State) ->
    ok.

new_receiver(BlobRef, From, #ldst{path=Path}=State) ->
    {BlobDir, BlobFName} = blob_path(BlobRef, Path),
    BlobPath = filename:join([BlobDir, BlobFName]),

    case file:is_regular(BlobPath) of
        true ->
            {ok, FileInfo} = file:read_file_info(BlobPath),
            #file_info{size=S} = FileInfo,
            {error, {already_exists, BlobRef, S}, State};
        _ ->
            ReceiverPid = spawn_link(?MODULE, receive_loop, [BlobRef,
                                                             BlobPath,
                                                             From,
                                                             State]),
            {ok, {ReceiverPid, nil}, State}
    end.


receive_loop(BlobRef, BlobPath, From, State) ->
    MonRef = erlang:monitor(process, From),
    coffer_storage:register_receiver(BlobRef),

    TmpBlobPath = temp_blobref(BlobRef),
    case file:open(TmpBlobPath, [write, append]) of
        {ok, FD} ->
                try
                    do_receive_loop(FD, TmpBlobPath, BlobRef, BlobPath,
                                    From, State)
                after
                    catch (file:close(FD))
                end;
        {error, Reason} ->
            lager:error("Error opening ~p: ~p~n", [TmpBlobPath,
                                                   Reason]),
            From ! {error, Reason}
    end,
    coffer_storage:unregister_receiver(BlobRef),
    erlang:demonitor(MonRef, [flush]).


do_receive_loop(FD, TmpBlobPath, BlobRef, BlobPath, From,
                #ldst{name=Name}) ->
    receive
        {data, From, Bin, Config} ->
            lager:info("Partial upload to ~p~n", [TmpBlobPath]),
            file:write(FD, Bin),
            From ! {ack, self(), Config};
        {eob, From, _Config} ->
            file:close(FD),
            case file:rename(TmpBlobPath, BlobPath) of
                ok ->
                    coffer_storage:notify(Name, {uploaded, BlobRef}),
                    {ok, FileInfo} = file:read_file_info(BlobPath),
                    #file_info{size=S} = FileInfo,
                    coffer_storage:notify({uploaded, BlobRef}),
                    From ! {ok, self(), S};
                {error, Error} ->
                    From ! {error, Error}
            end;
        {'DOWN', _, process, From, _} ->
           exit(normal)
    end.

new_stream({BlobRef, Window}, To, #ldst{path=Path}=State) ->
    {BlobDir, BlobFName} = blob_path(BlobRef, Path),
    BlobPath = filename:join([BlobDir, BlobFName]),

    case file:is_regular(BlobPath) of
        ok ->
            case file:open(BlobPath, [read]) of
                {ok, Fd} ->
                    StreamPid = spawn_link(?MODULE, stream_loop,
                                           [Fd, Window, To]),
                    {ok, StreamPid, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        _ ->
            {error, not_found, State}
    end.


stream_loop(Fd, Window, To) ->
    MonRef = erlang:monitor(process, To),
    try
        do_stream_loop(Fd, Window, To)
    after
        file:close(Fd)
    end,
    erlang:demonitor(MonRef, [flush]).

do_stream_loop(Fd, Window, To) ->
    case file:read(Fd, Window) of
        {ok, Data} ->
            To ! {data, Data, self()},
            receive
                {ack, To} ->
                    do_stream_loop(Fd, Window, To);
                {'DOWN', _, process, To, _} ->
                    exit(normal)
            end;
        eof ->
            To ! {coffer_eob, self()};
        {error, Reason} ->
            To ! {error, Reason, self()}
    end.

delete(BlobRef, #ldst{path=Path}=State) ->
    {BlobDir, BlobFName} = blob_path(BlobRef, Path),
    BlobPath = filename:join([BlobDir, BlobFName]),

    case file:is_regular(BlobPath) of
        ok ->
            case file:delete(BlobPath) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    lager:error("Error deleting ~p: ~p~n", [BlobRef,
                                                            Reason]),
                    {error, Reason, State}
            end;
        _ ->
            {error, not_found, State}
    end.


enumerate(To, #ldsdt{path=Path}=State) ->
    EnumeratePid = spawn_link(?MODULE, enumerate_loop, [To, Path]),
    {ok, EnumeratePid, State}.
    ok.

enumerate_loop(To, Path) ->
    MonRef = erlang:monitor(process, To),
    Files = filelib:wildcard("*", Path),
    do_enumerate_loop(Files, To, Path),
    erlang:demonitor(MonRef, [flush]).

do_enumerate_loop([], To, Path) ->
    To ! {done, self()};
do_enumerate_loop([File|Rest], To, Path) ->
    NewPath = filename:join(Path, File),
    case filelib:is_dir(NewPath) of
        true ->
            Files = filelib:wildcard("*", NewPath),
            process_dir(Files, To, NewPath, 1)
            do_enumerate_loop(Rest, To, Path);
         _ ->
            do_enumerate_loop(Rest, To, Path)
    end.

process_dir([], _To, _Path, _Depth) ->
    ok;
process_dir([File|Rest], To, Path, Depth) ->
    NewPath = filename:join(Path, File),
    case filelib:is_dir(NewPath) of
        true when Depth <= 4 ->
            Files = filelib:wildcard("*", NewPath),
            process_dir(Files, To, NewPath, Depth + 1),
            process_dir(Rest, To, Path);
        true ->
            process_dir(Rest, To, Path);
         false when Depth > 4 ->
            [HashType|HashPart] = string:tokens(NewPath, "/"),
            BlobRef = iolist_to_binary([HashType, "-",
                lists:flatten(HashParts)]),
            To ! {blob, {BlobRef, Size}, self()},
            receive
                {ack, To} ->
                    process_dir(Rest, To, Path);
                {'DOWN', _, process, To, _} ->
                    exit(normal)
            end
    end.

stat(BlobRefs, #ldsdt{path=Path}=State) ->
    ok.



%% @private
%%
blob_path(BlobRef, Path) ->
    case binary:split(BlobRef, <<"-">>) of
        [HashType, Hash] ->
            << A:1/binary, B:1/binary, C:1/binary, FName/binary >> = Hash,
            BlobDir = filename:join([Path, HashType, A, B, C]),
            case filelib:ensure_dir(BlobDir) of
                ok ->
                    {BlobDir, FName};
                Error ->
                    Error
            end;
        _ ->
            {error, invalid_blobref}
    end.

temp_blobref(BlobRef) ->
    filename:join([cofffer_util:gettempdir(), "coffer-",
                   binary_to_list(BlobRef), ".tmp"]).
