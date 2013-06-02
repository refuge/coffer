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
-export([blob_exists/2]).


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
            case filelib:ensure_dir(filename:join(Path, "dummy")) of
                ok ->
                    {ok, #ldst{name=StorageName, path=Path}};
                {error, Reason} ->
                    {error, {bad_config, Reason}}
            end
    end.

terminate(_State) ->
    ok.

new_receiver(BlobRef, From, #ldst{path=Path}=State) ->
    case coffer_blob:validate_ref(BlobRef) of
        ok ->
            BlobPath = coffer_blob:path(Path, BlobRef),
            case filelib:is_regular(BlobPath) of
                true ->
                    Size = file_size(BlobPath),
                    {error, {already_exists, BlobRef, Size}, State};
                _ ->
                    ReceiverPid = spawn_link(?MODULE, receive_loop, [BlobRef,
                                                                     BlobPath,
                                                                     From,
                                                                     State]),
                    {ok, {ReceiverPid, nil}, State}
            end;
        error ->
            {error, invalid_blobref}
    end.

receive_loop(BlobRef, BlobPath, From, State) ->
    MonRef = erlang:monitor(process, From),
    coffer_storage:register_receiver(BlobRef),

    TmpBlobPath = temp_blob(BlobRef),
    case file:open(TmpBlobPath, [write, append]) of
        {ok, FD} ->
                do_receive_loop(FD, TmpBlobPath, BlobRef, BlobPath,
                                From, State);
        {error, Reason} ->
            lager:error("Error opening ~p: ~p~n", [TmpBlobPath,
                                                   Reason]),
            From ! {error, Reason}
    end,
    coffer_storage:unregister_receiver(BlobRef),
    erlang:demonitor(MonRef, [flush]).


do_receive_loop(FD, TmpBlobPath, BlobRef, BlobPath, From,
                #ldst{name=Name}=State) ->
    receive
        {data, From, Bin, Config} ->
            lager:info("Partial upload to ~p~n", [TmpBlobPath]),
            file:write(FD, Bin),
            From ! {ack, self(), Config},
            do_receive_loop(FD, TmpBlobPath, BlobRef, BlobPath, From,
                            State);
        {eob, From, _Config} ->
            file:close(FD),
            ok = filelib:ensure_dir(BlobPath),
            case file:rename(TmpBlobPath, BlobPath) of
                ok ->
                    coffer_storage:notify(Name, {uploaded, BlobRef}),
                    Size = file_size(BlobPath),
                    From ! {ok, self(), Size};
                {error, Error} ->
                    From ! {error, Error}
            end;
        {'DOWN', _, process, From, _} ->
           exit(normal)
    end.

new_stream({BlobRef, Window}, To, #ldst{path=Path}=State) ->
    BlobPath = coffer_blob:path(Path, BlobRef),

    lager:info("start new stream for ~p from ~p~n", [BlobRef,
                                                     BlobPath]),
    case filelib:is_file(BlobPath) of
        true ->
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

delete(BlobRef, #ldst{name=Name, path=Path}=State) ->
    BlobPath = coffer_blob:path(Path, BlobRef),

    case filelib:is_file(BlobPath) of
        true ->
            case file:delete(BlobPath) of
                ok ->
                    coffer_storage:notify(Name, {deleted, BlobRef}),
                    {ok, State};
                {error, Reason} ->
                    lager:error("Error deleting ~p: ~p~n", [BlobRef,
                                                            Reason]),
                    {error, Reason, State}
            end;
        _ ->
            {error, not_found, State}
    end.


enumerate(To, #ldst{path=Path}=State) ->
    EnumeratePid = spawn_link(?MODULE, enumerate_loop, [To, Path]),
    {ok, EnumeratePid, State}.

enumerate_loop(To, Path) ->
    MonRef = erlang:monitor(process, To),
    walk(Path, Path, To),
    To ! {done, self()},
    erlang:demonitor(MonRef, [flush]).

walk(Root, Path, To) ->
    case filelib:is_dir(Path) of
        true ->
            Children = filelib:wildcard(Path ++ "/*"),
            lists:foreach(fun(P) ->
                        walk(Root, P, To)
                end,  Children);
        _ ->
            case filelib:is_file(Path) of
                true ->
                    Size = file_size(Path),
                    BlobRef = coffer_blob:from_path(Root, Path),
                    To ! {blob, {BlobRef, Size}, self()},
                    receive
                        {next, To} ->
                            ok;
                        {'DOWN', _, process, To, _} ->
                            exit(normal)
                    end;
                _ ->
                    ok
            end
    end.

stat(BlobRefs0, #ldst{path=Path}=State) ->
    %% before stating anything check the blob refs
    BlobRefs = lists:foldl(fun(BlobRef, Acc) ->
                    case coffer_blob:validate_ref(BlobRef) of
                        ok ->
                            Acc ++ [BlobRef];
                        error ->
                            Acc
                    end
            end, [], BlobRefs0),

    %% find missing
    {Found, Missing} = lists:foldl(fun(BlobRef, {F, M}) ->
                    BlobPath = coffer_blob:path(Path, BlobRef),
                    case filelib:is_file(BlobPath) of
                        true ->
                            {[{BlobRef, file_size(BlobPath)} | F], M};
                        _ ->
                            {F, [BlobRef | M]}
                    end
            end, {[], []}, BlobRefs),

    {Partials, Missing1} = case Missing of
        [] ->
            {[], Missing};
        _ ->
            ToFind = [{BlobRef, temp_blob(BlobRef)} || BlobRef <- Missing],
            lists:foldl(fun({BlobRef, TmpBlobPath}, {P, M}) ->
                        case filelib:is_file(TmpBlobPath) of
                            true ->
                                {[{BlobRef, file_size(TmpBlobPath)} | P], M};
                            _ ->
                                {P, [BlobRef | M]}
                        end
                end, {[], []}, ToFind)
    end,
    {ok, {lists:reverse(Found), lists:reverse(Missing1),
          lists:reverse(Partials)}, State}.

%% @private
%%

temp_blob(BlobRef) ->
    TempName = iolist_to_binary([<<"coffer-">>, BlobRef, <<".tmp">>]),
    filename:join([coffer_util:gettempdir(), TempName]).

file_size(Path) ->
    {ok, FileInfo} = file:read_file_info(Path),
    #file_info{size=Size} = FileInfo,
    Size.

blob_exists(BlobRef, #ldst{path=Path}) ->
    BlobPath = coffer_blob:path(Path, BlobRef),

    case filelib:is_file(BlobPath) of
        true ->
            ok;
        _ ->
            {error, not_found}
    end.

