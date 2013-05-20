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
                    From ! {ok, self(), S};
                {error, Error} ->
                    From ! {error, Error}
            end;
        {'DOWN', _, process, From, _} ->
           exit(normal)
        end.

enumerate(To, #ldsdt{path=Path}=State) ->
    ok.

stat(BlobRefs, #ldsdt{path=Path}=State) ->
    ok.



%% @private
%%
blob_path(BlobRef, Path) ->
    case binary:split(BlobRef, <<"-">>) of
        [HashType, Hash] ->
            << A:1/binary, B:1/binary, C:1/binary, FName/binary >> = BlobRef,
            BlobDir = filename:join([HashType, A, B, C]),
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
