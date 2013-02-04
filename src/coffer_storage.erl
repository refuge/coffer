%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage).

%% Types

-type blob_id() :: binary().
-type options() :: list().
-type storage_ref() :: record().
-type chunk() :: binary() | {stream, Bin :: binary()} | {stream, done}.

%% callbacks

-callback start(Config :: list()) ->
    {ok, InitState :: any()}
    | {error, Reason :: any()}.

-callback stop(State :: record()) ->
    ok
    | {error, Reason :: any()}.

-callback init_storage(State :: record()) ->
    ok
    | {error, Reason :: any()}.

-callback open(State :: any(), Options :: options()) ->
    {ok, Ref :: storage_ref()}
    | {error, Reason :: any()}.

-callback close(Ref :: record()) ->
    ok
    | {error, Reason :: any()}.

-callback put(Ref :: storage_ref(), Id :: blob_id(), Chunk :: chunk()) ->
    {ok, Ref1 :: record()}
    | {error, Reason :: any()}.

-callback get(Ref :: storage_ref(), Id :: blob_id(), Options :: options()) ->
    {ok, Data :: binary(), Ref1 :: storage_ref()}      % in memory
    | {chunk, Data :: binary(), Ref1 :: storage_ref()} % with Options = [stream]
    | {chunk, done, Ref1 :: storage_ref()}  % with Options = [stream]
    | {error, Reason :: any()}.

-callback delete(Ref :: storage_ref(), Id :: blob_id()) ->
    {ok, Ref1 :: storage_ref()}
    | {error, Reason :: any()}.

-callback all(Ref :: storage_ref()) ->
    {ok, List :: list()}
    | {error, Reason :: any()}.

-callback foldl(Ref :: storage_ref(), Func :: fun((Id :: blob_id(), Acc :: any()) -> Final :: any()), Initstate :: any(), Options :: options()) ->
    {ok, Final :: any()}
    | {error, Reason :: any()}.

-callback foreach(Ref :: storage_ref(), Func :: fun((Id :: blob_id()) -> Final :: any())) ->
    ok
    | {error, Reason :: any()}.

