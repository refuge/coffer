%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage).

%%
%% Describes storage API
%% ---------------------
%%
%% Each storage implementation must at least implements those functions.
%%

%% Types

-type blob_id() :: binary().
-type options() :: list().
-type chunk() :: binary() | {stream, Bin :: binary()} | {stream, done}.

%% callbacks

-callback start(Config :: list()) ->
    {ok, Pid :: pid()}
    | {error, Reason :: any()}.

-callback stop(Pid :: pid()) ->
    ok
    | {error, Reason :: any()}.

-callback open(Pid :: pid()) ->
    ok
    | {error, Reason :: any()}.

-callback close(Pid :: pid(), Ref :: any()) ->
    ok
    | {error, Reason :: any()}.

-callback put(Pid :: pid(), Id :: blob_id(), Chunk :: chunk()) ->
    ok
    | {error, Reason :: any()}.

-callback get(Pid :: pid(), Id :: blob_id(), Options :: options()) ->
    {ok, Data :: binary()}      % in memory
    | {chunk, Data :: binary()} % with Options = [stream]
    | {chunk, done}  % with Options = [stream]
    | {error, Reason :: any()}.

-callback delete(Pid :: pid(), Id :: blob_id()) ->
    ok
    | {error, Reason :: any()}.

-callback all(Pid :: pid()) ->
    {ok, List :: list()}
    | {error, Reason :: any()}.

-callback foldl(Pid :: pid(), Func :: fun((Id :: blob_id(), Acc :: any()) -> Final :: any()), Initstate :: any(), Options :: options()) ->
    {ok, Final :: any()}
    | {error, Reason :: any()}.

-callback foreach(Pid :: pid(), Func :: fun((Id :: blob_id()) -> Final :: any())) ->
    ok
    | {error, Reason :: any()}.

