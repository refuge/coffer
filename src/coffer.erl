%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer).

-include_lib("coffer/includes/coffer.hrl").

-export([start/0, stop/0]).
-export([list_storages/0, add_storage/3, remove_storage/1, get_storage/1]).
-export([put/3, get/2, get/3, delete/2, all/1, foldl/3, foldl/4, foreach/2]).

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

put(StoragePid, Id, Chunk) ->
    coffer_storage:put(StoragePid, Id, Chunk).

get(StoragePid, Id) ->
    get(StoragePid, Id, []).

get(StoragePid, Id, Options) ->
    coffer_storage:get(StoragePid, Id, Options).

delete(StoragePid, Id) ->
    coffer_storage:delete(StoragePid, Id).

all(StoragePid) ->
    coffer_storage:all(StoragePid).

foldl(StoragePid, Func, InitState) ->
    foldl(StoragePid, Func, InitState, []).

foldl(StoragePid, Func, InitState, Options) ->
    coffer_storage:foldl(StoragePid, Func, InitState, Options).

foreach(StoragePid, Func) ->
    coffer_storage:foreach(StoragePid, Func).

