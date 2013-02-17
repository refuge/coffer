%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer).

-include_lib("coffer/includes/coffer.hrl").

-export([start/0, stop/0]).
-export([list_resources/0, add_resource/3, remove_resource/1]).
-export([open/1, open/2, close/1]).
-export([put/3, get/2, get/3, delete/2, all/1, foldl/3, foldl/4, foreach/2]).


%% @doc Start the coffer application. Useful when testing using the shell.
start() ->
    coffer_deps:ensure(),
    application:load(coffer),
    coffer_app:ensure_deps_started(),
    application:start(coffer).

%% @doc Start the coffer application. Useful when testing using the shell.
stop() ->
    application:stop(coffer).


list_resources() ->
    coffer_resource:list().

add_resource(Name, Backend, Config) ->
    coffer_resource:add(Name, Backend, Config).

remove_resource(Name) ->
    coffer_resource:remove(Name).

open(ResourceName) ->
    open(ResourceName, []).

open(ResourceName, Options) ->
    coffer_resource:open(ResourceName, Options).

close(Ref) ->
    coffer_resource:close(Ref).

put(#ref{backend=Backend, sref=SRef}=_Ref, Id, Chunk) ->
    Backend:put(SRef, Id, Chunk).

get(Ref, Id) ->
    get(Ref, Id, []).

get(#ref{backend=Backend, sref=SRef}=_Ref, Id, Options) ->
    Backend:get(SRef, Id, Options).

delete(#ref{backend=Backend, sref=SRef}=_Ref, Id) ->
    Backend:delete(SRef, Id).

all(#ref{backend=Backend, sref=SRef}=_Ref) ->
    Backend:all(SRef).

foldl(Ref, Func, InitState) ->
    foldl(Ref, Func, InitState, []).

foldl(#ref{backend=Backend, sref=SRef}=_Ref, Func, InitState, Options) ->
    Backend:foldl(SRef, Func, InitState, Options).

foreach(#ref{backend=Backend, sref=SRef}=_Ref, Func) ->
    Backend:foreach(SRef, Func).

