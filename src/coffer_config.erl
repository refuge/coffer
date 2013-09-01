%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.
%%%
%% @doc module to maintain an update the config when needed.

-module(coffer_config).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([start_link/0]).

-export([all/0,
         set/2, set/3,
         get/1, get/2, get/3,
         del/1, del/2]).

-export([bind_opts/1,
         start_listener/3,
         http_env/0,
         init_http/0]).

%% OTP API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(st, {http_conf, http_started}).

-define(DEFAULT_PORT, 7000).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

all() ->
    econfig:cfg2list(coffer_config).

set(Section, Value) ->
    econfig:set_value(coffer_config, Section, Value).

set(Section, Key, Value) ->
    econfig:set_value(coffer_config, Section, Key, Value).

get(Section) ->
    econfig:get_value(coffer_config, Section).

get(Section, Key) ->
    econfig:get_value(coffer_config, Section, Key).

get(Section, Key, Default) ->
    econfig:get_value(coffer_config, Section, Key, Default).

del(Section) ->
    econfig:delete_value(coffer_config, Section).

del(Section, Key) ->
    econfig:delete_value(coffer_config, Section, Key).

%% PRIVATE API

init([]) ->
    %% start the HTTP api if needed
    %% note: if the listener can't be started the server won't crash .
    %% The user will have to restart the connection it if needed
    {HttpConfig, HttpStarted} = init_http(),

    %% subscribe to config changes
    econfig:subscribe(coffer_config),
    {ok, #st{http_conf=HttpConfig, http_started=HttpStarted}}.


handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({config_updated, coffer_config, {set, {Section, Key}}}, State) ->
    NewState = update_config(Section, Key, State),
    {noreply, NewState}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.



init_http() ->
    Bind = get("core", "bind_http", ""),

    %% only start if bind_http is set
    case Bind of
        "" ->
            {nil, false};
        _ ->
            %% start HTTP dependencies if needed
            coffer_util:require([crypto, public_key, ssl, ranch,
                                 cowboy]),

            %% start the listener
            start_listener(listener_opts(Bind), http_env(), 5)
    end.


update_config(_, _, State) ->
    State.

listener_opts(Bind) ->
    Opts = bind_opts(Bind),
    {NbAcceptors, SslOpts, IsSsl} = coffer_config_util:http_config(),
    {NbAcceptors, Opts ++ SslOpts, IsSsl}.

bind_opts(Bind) ->
    case parse_address(list_to_binary(Bind)) of
        {any, Port} ->
            [{port, Port}];
        {Ip, Port} ->
            {ok, ParsedIp} = inet_parse:address(Ip),
            [{port, Port}, {ip, ParsedIp}]
    end.

parse_address(<<"[", Rest/binary>>) ->
    case binary:split(Rest, <<"]">>) of
        [Host, <<>>] ->
            {binary_to_list(Host), ?DEFAULT_PORT};
        [Host, <<":", Port/binary>>] ->
            {binary_to_list(Host),
             list_to_integer(binary_to_list(Port))};
        _ ->
            parse_address(Rest)
    end;
parse_address(Addr) ->
    case binary:split(Addr, <<":">>) of
        [Port] ->
            {any, list_to_integer(binary_to_list(Port))};
        [<<>>, Port] ->
            {any, list_to_integer(binary_to_list(Port))};
        [Host, Port] ->
            {binary_to_list(Host),
             list_to_integer(binary_to_list(Port))}
    end.

http_env() ->
    DispatchRules = coffer_http:dispatch_rules(),
    Dispatch = [{'_', DispatchRules}],
    Dispatch1 = cowboy_router:compile(Dispatch),
    [{env, [{dispatch, Dispatch1}]}].


%% start a listener
start_listener(_, _, 0) ->
    lager:info("HTTP API not started, too much tries.~n", []),
    {nil, false};
start_listener({NbAcceptors, TransOpts, Ssl}=ListenerOpts, ProtoOpts,
                Tries) ->
    Transport = case Ssl of
        true -> ranch_ssl;
        _ -> ranch_tcp
    end,

    case ranch:start_listener(coffer_http, NbAcceptors, Transport, TransOpts,
                              cowboy_protocol, ProtoOpts) of
        {ok, _} ->
            lager:info("HTTP API started", []),
            {ListenerOpts, true};
        {error, _} = Error ->
            lager:error("error starting the HTTP api: ~p~n", [Error]),
            start_listener(ListenerOpts, ProtoOpts, Tries-1);
        _ ->
            %% already started
            {ListenerOpts, true}
    end.
