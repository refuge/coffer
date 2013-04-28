%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer).

-include_lib("coffer/includes/coffer.hrl").

-export([start/0, stop/0]).
-export([list_storages/0, add_storage/3, remove_storage/1, get_storage/1]).
-export([open/1, close/1]).
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

list_storages() ->
    coffer_server:list().

add_storage(Name, Backend, Config) ->
    coffer_server:add(Name, Backend, Config).

remove_storage(Name) ->
    coffer_server:remove(Name).

get_storage(Name) ->
    coffer_server:get(Name).

% --- Storage API ---

open(Pid) ->
    gen_server:call(Pid, {open}).

close(Pid) ->
    gen_server:call(Pid, {close}).

put(Pid, Id, Chunk) ->
    gen_server:call(Pid, {put, Id, Chunk}).

get(Pid, Id) ->
    gen_server:call(Pid, {get, Id, []}).

get(Pid, Id, Options) ->
    gen_server:call(Pid, {get, Id, Options}).

delete(Pid, Id) ->
    gen_server:call(Pid, {delete, Id}).

all(Pid) ->
    gen_server:call(Pid, {all}).

foldl(Pid, Func, InitState) ->
    gen_server:call(Pid, {foldl, Func, InitState, []}).

foldl(Pid, Func, InitState, Options) ->
    gen_server:call(Pid, {foldl, Func, InitState, Options}).

foreach(Pid, Func) ->
    gen_server:call(Pid, {foreach, Func}).

