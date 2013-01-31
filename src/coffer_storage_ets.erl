%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage_ets).
-behaviour(coffer_storage).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/1, stop/1]).
-export([open/2, close/1]).
-export([put/3, get/3, delete/2, enumerate/1, foldl/4, foreach/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Config], []).

stop(Pid) ->
    gen_server:call(Pid, {stop}).

open(Pid, _Options) ->
    {ok, Pid}.

close(_Pid) ->
    ok.

put(Pid, Id, Bin) when is_binary(Bin) ->
    gen_server:call(Pid, {put, Id, Bin});
put(_Ref, _Id, _Chunk) ->
    {error, not_supported}.

get(Pid, Id, Options) ->
    gen_server:call(Pid, {get, Id, Options}).

delete(Pid, Id) ->
    gen_server:call(Pid, {delete, Id}).

enumerate(Pid) ->
    gen_server:call(Pid, {enumerate}).

foldl(Pid, Func, InitState, Options) ->
    gen_server:call(Pid, {foldl, Func, InitState, Options}).

foreach(Pid, Func) ->
    gen_server:call(Pid, {foreach, Func}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(Config) ->
    case Config of
        [{Name, Options}] ->
            Tid = ets:new(Name, Options),
            {ok, Tid};
        _ ->
            lager:error("Wrong config: ~p", [Config]),
            {error, wrong_config}
    end.

handle_call({stop}, _From, Tid) ->
    {stop, normal, ok, Tid};
handle_call({put, Id, Bin}, _From, Tid) ->
    ets:insert(Tid, {Id, Bin}),
    {reply, {ok, self()}, Tid};
handle_call({get, Id, _Options}, _From, Tid) ->
    [{_Key, Value}] = ets:lookup(Tid, Id),
    {reply, {ok, Value, self()}, Tid};
handle_call({delete, Id}, _From, Tid) ->
    ets:delete(Tid, Id),
    {reply, {ok, self()}, Tid};
handle_call({enumerate}, _From, Tid) ->
    Value = ets:foldl(
        fun({Key, _}, Acc) ->
            [Key|Acc]
        end,
        [],
        Tid
    ),
    {reply, {ok, Value}, Tid};
handle_call({foldl, Func, Initstate, _Options}, _From, Tid) ->
    Value = ets:foldl(
        Func,
        Initstate,
        Tid
    ),
    {reply, {ok, Value}, Tid};
handle_call({foreach, Func}, _From, Tid) ->
    io:format("CAL foreach with func: ~p~n", [Func]),
    ets:foldl(
        fun({Key, _}, _) ->
            Func(Key),
            []
        end,
        [],
        Tid
    ),
    {reply, ok, Tid};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, Tid) ->
    ets:delete(Tid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
