%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage).

-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/2, stop/1]).
-export([new_upload/2,
         fetch_stream/2,
         delete/2,
         enumerate/1,
         stat/2]).

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
    gen_server:call(Pid, stop).


new_upload(Pid, BlobRef) ->
    gen_server:call(Pid, {new_upload, BlobRef}).

fetch_stream(Pid, {BlobRef, Window}) ->
    gen_server:call(Pid, {fetch_stream, {BlobRef, Window}}).

delete(Pid, BlobRef) ->
    gen_server:call(Pid, {delete, BlobRef}).

enumerate(Pid) ->
    gen_server:call(Pid, enumerate).

stat(Pid, BlobRefs) ->
    gen_server:call(Pid, {stat, BlobRefs}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-record(ss, {
    backend,
    config,
    state
}).

init([GivenBackend, GivenConfig]) ->
    case GivenBackend:init(GivenConfig) of
        {ok, State} ->
            SS = #ss{backend=GivenBackend, config=GivenConfig,
                     state=State},
            {ok, SS};
        {error, Reason} ->
            {stop, Reason}
    end.

handle_call({stop}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:terminate(State) of
        ok ->
            {stop, normal, ok, SS};
        {error, Reason} ->
            % TODO not too sure what to do here...
            {reply, {error, Reason}, SS}
    end;
handle_call({new_upload, BlobRef}, {From, _}, #ss{backend=Backend,
                                              state=State}=SS) ->
    case Backend:new_receiver(BlobRef, From, State) of
        {ok, {_ReceiverPid, _Config}=Receiver, NewState} ->
            {reply, {ok, Receiver}, SS#ss{state=NewState}};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, SS#ss{state=NewState}}
    end;
handle_call({fetch_stream, {BlobRef, Window}}, {From, _},
            #ss{backend=Backend, state=State}=SS) ->

    case Backend:new_stream({BlobRef, Window}, From, State) of
        {ok, StreamPid, NewState} ->
            {reply, {ok, StreamPid},  SS#ss{state=NewState}};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, SS#ss{state=NewState}}
    end;
handle_call({delete, BlobRef}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:delete(BlobRef, State) of
        {ok, NewState} ->
            {reply, ok, SS#ss{state=NewState}};
        {error, _Reason, NewState} ->
            %% it is safe to ignore any error when we delete a blob
            {reply, ok, SS#ss{state=NewState}}
    end;
handle_call(enumerate, {From, _}, #ss{backend=Backend, state=State}=SS) ->
    case Backend:enumerate(From, State) of
        {ok, EnumeratePid, NewState} ->
            {reply, {ok, EnumeratePid}, SS#ss{state=NewState}};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, SS#ss{state=NewState}}
    end;
handle_call({stat, BlobRefs}, _From, #ss{backend=Backend, state=State}=SS) ->
    case Backend:stat(BlobRefs, State) of
        {ok, {_Found, _Missing}=R, NewState} ->
            {reply, {ok, R}, SS#ss{state=NewState}};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, SS#ss{state=NewState}}
    end;

handle_call(_Request, _From, State) ->
    {reply, unknown_call, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #ss{backend=Backend, state=State}=_SS) ->
    Backend:terminate(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ---------------------------------------------------------------------------
%% Internal Function Definitions
%% ---------------------------------------------------------------------------


