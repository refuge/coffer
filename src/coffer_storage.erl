%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage).

-behaviour(gen_server).

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

%% callbacks -----------------------------------------------------------------

% INIT/STOP

-callback do_start(Config :: list()) ->
    {ok, State :: any()}
    | {error, Reason :: any()}.

-callback do_stop(State :: any()) ->
    ok
    | {error, Reason :: any()}.


% SESSION HANDLING
% TODO still some refinement to do here I think

-callback do_open(State :: any()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any()}.

-callback do_close(State :: any()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any()}.


% CHANGE/ERASE A BLOB

-callback do_put(State :: any(), Id :: blob_id(), Chunk :: chunk()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any()}.

-callback do_delete(State :: any(), Id :: blob_id()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any()}.


% GET BLOB

-callback do_get(State :: any(), Id :: blob_id(), Options :: options()) ->
    {ok, Data :: binary()}      % in memory
    | {chunk, Data :: binary()} % with Options = [stream]
    | {chunk, done}  % with Options = [stream]
    | {error, Reason :: any()}.


% QUERY callbacks
% those functions don't change anything by themselves
% but merely iterate over the blobs

% TODO we can remove this one from the storage
% all is a special case of foldl and can be
% handled by the coffer_storage behaviour
% for all storage (to discuss...)
-callback do_all(State :: any()) ->
    {ok, List :: list()}
    | {error, Reason :: any()}.

-callback do_foldl(State :: any(), Func :: fun((Id :: blob_id(), Acc :: any()) -> Final :: any()), Initstate :: any(), Options :: options()) ->
    {ok, Final :: any()}
    | {error, Reason :: any()}.

% TODO foreach is also a special case of foldl without taking care of the final result
% (to discuss)
-callback do_foreach(State :: any(), Func :: fun((Id :: blob_id()) -> Final :: any())) ->
    ok
    | {error, Reason :: any()}.

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/2, stop/1]).
-export([open/1, close/1]).
-export([put/3, get/3, delete/2, all/1, foldl/4, foreach/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start(Backend, Config) ->
    gen_server:start_link(?MODULE, [Backend, Config], []).

stop(Pid) ->
    gen_server:call(Pid, {stop}).

open(Pid) ->
    gen_server:call(Pid, {open}).

close(Pid) ->
    gen_server:call(Pid, {close}).

put(Pid, Id, Stuff) ->
    gen_server:call(Pid, {put, Id, Stuff}).

get(Pid, Id, Options) ->
    gen_server:call(Pid, {get, Id, Options}).

delete(Pid, Id) ->
    gen_server:call(Pid, {delete, Id}).

all(Pid) ->
    gen_server:call(Pid, {all}).

foldl(Pid, Func, InitState, Options) ->
    gen_server:call(Pid, {foldl, Func, InitState, Options}).

foreach(Pid, Func) ->
    gen_server:call(Pid, {foreach, Func}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-record(ss, {
    backend,
    config,
    state
}).

init([GivenBackend, GivenConfig]) ->
    case GivenBackend:do_start(GivenConfig) of
        {ok, State} ->
            SS = #ss{backend=GivenBackend, config=GivenConfig, state=State},
            {ok, SS};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({stop}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_stop(State) of
        ok ->
            {stop, normal, ok, SS};
        {error, Reason} ->
            % TODO not too sure what to do here...
            {reply, {error, Reason}, SS}
    end;
handle_call({open}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_open(State) of
        {ok, NewState} ->
            UpdatedSS = SS#ss{state=NewState},
            {reply, ok, UpdatedSS};
        {error, Reason} ->
            {reply, {error, Reason}, SS}
    end;
handle_call({close}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_close(State) of
        {ok, NewState} ->
            UpdatedSS = SS#ss{state=NewState},
            {reply, ok, UpdatedSS};
        {error, Reason} ->
            {reply, {error, Reason}, SS}
    end;
handle_call({put, Id, Chunk}, _From, #ss{backend=Backend, state=State}=SS) ->
    % TODO need to put here a check to avoid 2 callers on the same Id
    case Backend:do_put(State, Id, Chunk) of
        {ok, NewState} ->
            UpdatedSS = SS#ss{state=NewState},
            {reply, ok, UpdatedSS};
        {error, Reason} ->
            {reply, {error, Reason}, SS}
    end;
handle_call({get, Id, Options}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_get(State, Id, Options) of
        {error, Reason} ->
            {reply, {error, Reason}, SS};
        Reply ->
            {reply, Reply, SS}
    end;
handle_call({delete, Id}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_delete(State, Id) of
        {ok, NewState} ->
            UpdatedSS = SS#ss{state=NewState},
            {reply, ok, UpdatedSS};
        {error, Reason} ->
            {reply, {error, Reason}, SS}
    end;
handle_call({all}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_all(State) of
        {error, Reason} ->
            {reply, {error, Reason}, SS};
        Reply ->
            {reply, Reply, SS}
    end;
handle_call({foldl, Func, InitState, Options}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_foldl(State, Func, InitState, Options) of
        {error, Reason} ->
            {reply, {error, Reason}, SS};
        Reply ->
            {reply, Reply, SS}
    end;
handle_call({foreach, Func}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:do_all(State, Func) of
        {error, Reason} ->
            {reply, {error, Reason}, SS};
        Reply ->
            {reply, Reply, SS}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #ss{backend=Backend, state=State}=_SS) ->
    Backend:do_stop(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% Internal Function Definitions
%% ---------------------------------------------------------------------------


