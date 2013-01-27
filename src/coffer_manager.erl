%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_manager).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1, stop/0]).
-export([init_storage/0, init_storage/1]).
-export([get_blob_init/1, get_blob/1, get_blob_end/1,
         get_blob_content/1]).
-export([store_blob_init/1, store_blob/2, store_blob_end/1,
         store_blob_content/2]).
-export([remove_blob/1]).
-export([fold_blobs/2]).
-export([exists/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

stop() ->
    gen_server:call(?MODULE, {stop}).

init_storage() ->
    init_storage([]).

init_storage(Options) ->
    gen_server:call(?MODULE, {run, {init_storage, [Options]}}).

get_blob_init(Id) when is_binary(Id) ->
    gen_server:call(?MODULE, {run, {get_blob_init, [Id]}}).

get_blob(Token) ->
    gen_server:call(?MODULE, {run, {get_blob, [Token]}}).

get_blob_end(Token) ->
    gen_server:call(?MODULE, {run, {get_blob_end, [Token]}}).

get_blob_content(Id) when is_binary(Id) ->
    gen_server:call(?MODULE, {run, {get_blob_content, [Id]}}).

store_blob_init(Id) when is_binary(Id) ->
    gen_server:call(?MODULE, {run, {store_blob_init, [Id]}}).

store_blob(Token, Data) ->
    gen_server:call(?MODULE, {run, {store_blob, [Token, Data]}}).

store_blob_end(Token) ->
    gen_server:call(?MODULE, {run, {store_blob_end, [Token]}}).

store_blob_content(Id, Data) when is_binary(Id) ->
    gen_server:call(?MODULE, {run, {store_blob_content, [Id, Data]}}).

remove_blob(Id) when is_binary(Id) ->
    gen_server:call(?MODULE, {run, {remove_blob, [Id]}}).

fold_blobs(Func, InitState) ->
    gen_server:call(?MODULE, {run, {fold_blobs, [Func, InitState]}}).

exists(Id) when is_binary(Id) ->
    gen_server:call(?MODULE, {run, {exists, [Id]}}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-record(state, {backendPid, backend, backendArgs}).

init([Backend, BackendArgs]) ->
    io:format("BackendArgs = ~p~n", [BackendArgs]),
    BackendPid = Backend:start_link(BackendArgs),

    State = #state{backendPid=BackendPid, backend=Backend, backendArgs=BackendArgs},
    {ok, State}.

handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call({run, {Function, Args}}, _From, #state{backend=Backend}=State) ->
    Reply = apply(Backend, Function, Args),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{backend=Backend}) ->
    Backend:stop(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

