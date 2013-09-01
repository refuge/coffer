%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.
%%%
%% @doc module to maintain an update the config when needed.

-module(coffer_config).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([start_link1/1]).
-export([start_link/0]).

-export([all/0,
         set/2, set/3,
         get/1, get/2, get/3,
         del/1, del/2]).

-export([bind_opts/1,
         start_listener/3,
         http_env/0,
         init_http/1]).

%% OTP API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(st, {mod,
             handle,
             http_conf,
             http_started}).

-define(DEFAULT_PORT, 7000).


start_link1(Pid) ->
    case is_process_alive(Pid) of
        true ->
            link(Pid),
            {ok, Pid};
        false ->
            start_link()
    end.

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

all() ->
    call(all).

set(Section, Value) ->
    call({set, Section, Value}).

set(Section, Key, Value) ->
    call({set, Section, Key, Value}).

get(Section) ->
    call({get, Section}).

get(Section, Key) ->
    call({get, Section, Key}).

get(Section, Key, Default) ->
    call({get, Section, Key, Default}).

del(Section) ->
    call({del, Section}).

del(Section, Key) ->
    call({del, Section, Key}).


%% PRIVATE API

init([]) ->
    %% set the config handler
    {Mod, Args} = coffer_app:get_app_env(config_backend,
                                         {coffer_econfig, []}),

    %% initialize the config
    {ok, Handle} = Mod:init(Args),

    InitState = #st{mod=Mod, handle=Handle},

    %% start the HTTP api if needed
    %% note: if the listener can't be started the server won't crash .
    %% The user will have to restart the connection it if needed
    {HttpConfig, HttpStarted} = init_http(InitState),

    {ok, InitState#st{http_conf=HttpConfig,
                      http_started=HttpStarted}}.

handle_call(all, _From, #st{mod=Mod, handle=H}=State) ->
    Res = Mod:all(H),
    {reply, Res, State};

handle_call({set, Section, Value}, _From, #st{mod=Mod, handle=H}=State) ->
    Value1 = [{coffer_util:to_list(K), coffer_util:to_list(V)}
              || {K, V} <- Value],
    Section1 = coffer_util:to_list(Section),
    Bind = Mod:get_value(H, "core", "bind_http", ""),
    Reply = case Section1 of
        "http" when Bind /= "" ->
            case restart_http(Bind, Value, State) of
                true ->
                    Mod:set_value(H, Section, Value1);
                false ->
                    {error, <<"http config error">>}
            end;
        "http" ->
            Mod:set_value(H, Section1, Value1);
        "core" ->
            NewBind = proplists:get_value("bind", Value1),
            case NewBind of
                Bind ->
                    %% no need to restart
                    Mod:set_value(H, Section1, Value1);
                _ ->
                    Conf = Mod:get_value(H, "http"),
                    case restart_http(Bind, Conf, State) of
                        true ->
                            Mod:set_value(H, Section, Value1);
                        false ->
                            {error, <<"http config error">>}
                    end
            end;
        _ ->
            Mod:set_value(H, Section, Value)
    end,
    {reply, Reply, State};

handle_call({set, Section, Key, Value}, _From, State) ->
    Reply = do_set_value(coffer_util:to_list(Section), coffer_util:to_list(Key),
                         coffer_util:to_list(Value), State),
    {reply, Reply, State};

handle_call({get, Section}, _From, #st{mod=Mod, handle=H}=State) ->
    Res = Mod:get_value(H, Section),
    {reply, Res, State};

handle_call({get, Section, Key}, _From, #st{mod=Mod, handle=H}=State) ->
    Res = Mod:get_value(H, Section, Key),
    {reply, Res, State};

handle_call({get, Section, Key, Default}, _From,
            #st{mod=Mod, handle=H}=State) ->
    Res = Mod:get_value(H, Section, Key, Default),
    {reply, Res, State};

handle_call({del, Section}, _From, #st{mod=Mod, handle=H}=State) ->
    Section1 = coffer_util:to_list(Section),
    Bind = Mod:get_value(H, "core", "bind_http", ""),
    Reply = case Section1 of
        "core" ->
            stop_http(),
            Mod:delete_value(H, Section);
        "http" when Bind /= "" ->
            case restart_http(Bind, [], State) of
                true ->
                    Mod:delete_value(H, Section);
                false ->
                    {error, <<"config error">>}
            end;
        _ ->
            Mod:delete_value(H, Section)
    end,
    {reply, Reply, State};

handle_call({del, Section, Key}, _From, State) ->
    Reply = do_delete_value(coffer_util:to_list(Section),
                            coffer_util:to_list(Key), State),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({config_updated, coffer_config, {set, {Section, Key}}}, State) ->
    NewState = update_config(Section, Key, State),
    {noreply, NewState}.

terminate(_Reason,  #st{mod=Mod, handle=H}) ->
    Mod:terminate(H),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


call(Msg) ->
    gen_server:call(?MODULE, Msg).


do_set_value("core", "bind_http", Bind, #st{mod=Mod, handle=H}=State) ->
    OldBind = Mod:get_value(H, "core", "bind_http", ""),
    case Bind of
        OldBind ->
            ok;
        "" ->
            stop_http(),
            Mod:set_value(H, "core", "bind_http", "");
        _ ->
            Conf = Mod:get_value(H, "http"),
            case restart_http(Bind, Conf, State) of
                true ->
                    Mod:set_value(H, "core", "bind_http", Bind);
                false ->
                    {error, <<"http config error">>}
            end
    end;
do_set_value("http", Key, Value, #st{mod=Mod, handle=H}=State) ->
    Bind = Mod:get_value(H, "core", "bind_http", ""),
    case Bind of
        "" ->
            Mod:set_value(H, "http", Key, Value),
            ok;
        _ ->
            Conf = Mod:get_value(H, "http"),
            Conf1 = lists:keyreplace(Key, 1, Conf, {Key, Value}),
            case restart_http(Bind, Conf1, State) of
                true ->
                    Mod:set_value(H, "http", Key, Value);
                false ->
                    {error, <<"http config error">>}
            end
    end;
do_set_value(Section, Key, Value, #st{mod=Mod, handle=H}) ->
    Mod:set_value(H, Section, Key, Value).


do_delete_value("core", "bind_http", #st{mod=Mod, handle=H}) ->
    Bind = Mod:get_value(H, "core", "bind_http", ""),
    case Bind of
        "" ->
            ok;
        _ ->
            stop_http(),
            Mod:delete_value(H, "core", "bind_http")
    end;
do_delete_value("Section", Key, #st{mod=Mod, handle=H}=State) ->
    Bind = Mod:get_value(H, "core", "bind_http", ""),
    case Bind of
        "" ->
            Mod:delete_value(H, "http", Key),
            ok;
        _ ->
            Conf = proplists:delete(Key, Mod:get_value(H, "http")),
            case restart_http(Bind, Conf, State) of
                true ->
                    Mod:delete_value(H, "http", Key);
                false ->
                    {error, <<"http config error">>}
            end

    end;
do_delete_value(Section, Key, #st{mod=Mod, handle=H}) ->
    Mod:delete_value(H, Section, Key).

init_http(#st{mod=Mod, handle=Handle}) ->
    Bind = Mod:get_value(Handle, "core", "bind_http", ""),

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

restart_http(Bind, Conf, State) ->
    %% stop the HTTP API
    %% Note: it would be better to first test if we can start a new listener
    %% gracefully and then stop the old one if it's ok. but unfortunately we
    %% can't yet since we are using cowboy.
    stop_http(),

    Opts = bind_opts(Bind),
    {NbAcceptors, SslOpts, IsSsl} = coffer_config_util:http_config(Conf),
    ListenerOpts = {NbAcceptors, Opts ++ SslOpts, IsSsl},

    case start_listener(ListenerOpts, http_env(), 1) of

        {_, false} ->
            %% restart with the old conf
            init_http(State),
            false;
        _ ->
            true
    end.

stop_http() ->
    cowboy:stop_listener(coffer_http),
    lager:info("HTTP API stopped", []).
