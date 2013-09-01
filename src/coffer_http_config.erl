-module(coffer_http_config).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).


init(_Transport, Req, []) ->
    {ok, Req, undefined}.


handle(Req, State) ->
    {Method, Req2} = cowboy_req:method(Req),
    {Bindings, Req3} = cowboy_req:bindings(Req2),
    {ok, Req4} = handle_req(Method, Bindings, Req3),
    {ok, Req4, State}.

handle_req(<<"GET">>, [], Req) ->
    %% clean config, convert to binay value
    %%
    Config = lists:foldl(fun
                ({"http", HttpConfig}, Acc) ->
                    HttpConfig1 = lists:map(fun({Name, Settings}) ->
                                    KVs = [{list_to_binary(K),
                                            list_to_binary(V)} || {K, V}
                                                                  <- Settings],
                                    {clean_name(Name), KVs}
                            end, HttpConfig),
                    [{<<"http">>, HttpConfig1} | Acc];

                ({Key, Val}, Acc) ->
                    KVs = [{list_to_binary(K),list_to_binary(V)}
                           || {K, V} <- Val],
                    [{list_to_binary(Key), KVs} | Acc]
            end, [], econfig:cfg2list(coffer_config, " ")),

    {JsonConfig, Req1} = coffer_http_util:to_json(Config, Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                 JsonConfig, Req1);

handle_req(<<"GET">>, [{section, Section}], Req) ->
    KVs = [{list_to_binary(K), list_to_binary(V)}
           || {K, V} <- econfig:get_value(coffer_config,
                                          binary_to_list(Section))],
    {Json, Req1} = coffer_http_util:to_json([{Section, KVs}], Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                 Json, Req1);

handle_req(<<"GET">>, [{key, Key}, {section, Section}], Req) ->
    case coffer_config:get( binary_to_list(Section),
                           binary_to_list(Key)) of
        undefined ->
            coffer_http_util:not_found(Req);
        Val ->
            {Json, Req1} = coffer_http_util:to_json(list_to_binary(Val), Req),
            cowboy_req:reply(200, [{<<"Content-Type">>,
                                    <<"application/json">>}], Json,
                             Req1)
    end;

handle_req(<<"PUT">>, [{section, Section}], Req) ->
    case cowboy_req:body(Req) of
        {ok, Bin, Req1} ->
            Config = jsx:decode(Bin),
            case Section of
                <<"http">> ->
                    %% we only store the new http config if we are able to
                    %% restart the server
                    %% remove the connection from the connection supervisor
                    ok = ranch:unlink_connection(coffer_http),

                    case do_restart_http(nil, Config) of
                        true ->
                            %% store the config
                            coffer_config:set(Section, Config),
                            coffer_http_util:ok(Req1);
                        false ->
                            coffer:error(<<"config_unchanged">>, Req1)
                    end;
                _ ->
                    ok = coffer_config:set(Section,
                                           Config),
                    coffer_http_util:ok(202, Req1)
            end;
        Error ->
            Error
    end;

handle_req(<<"PUT">>, [{key, <<"bind_http">>}, {section, <<"core">>}], Req) ->
    %% remove the connection from the connection supervisor
    ranch:unlink_connection(coffer_http),

    maybe_restart_http(<<"core">>, <<"bind_http">>, Req);
handle_req(<<"PUT">>, [{key, Key}, {section, <<"http">>}], Req) ->
    %% remove the connection from the connection supervisor
    ok = ranch:unlink_connection(coffer_http),

    maybe_restart_http(<<"http">>, Key, Req);
handle_req(<<"PUT">>, [{key, Key}, {section, Section}], Req) ->
    case cowboy_req:body(Req) of
        {ok, Bin, Req1} ->
            Value = jsx:decode(Bin),
            case coffer_config:set(Section, Key,
                                   Value) of
                ok ->
                    coffer_http_util:ok(202, Req1);
                Error ->
                    lager:error("config update: ~p~n", [Error]),
                    coffer_http_util:error(Error, Req1)
            end;
        Error ->
            Error
    end;

handle_req(<<"DELETE">>, [{section, Section}], Req) ->
    case Section of
        <<"http">> ->
            %% remove the connection from the connection supervisor
            ok = ranch:unlink_connection(coffer_http),

            %% we only store the new http config if we are able to
            %% restart the server
            case do_restart_http(nil, []) of
                true ->
                    coffer_config:del("http"),
                    coffer_http_util:ok(Req);
                false ->
                    coffer_http_util:error(<<"configuration unchanged">>, Req)
            end;
        _ ->
            ok = coffer_config:del(Section),
            coffer_http_util:ok(202, Req)
    end;


handle_req(<<"DELETE">>, [{key, <<"bind_http">>},
                          {section, <<"core">>}], Req) ->

    %% remove the connection from the connection supervisor
    ok = ranch:unlink_connection(coffer_http),

    stop_http(),
    coffer_http_util:ok(202, Req);

handle_req(<<"DELETE">>, [{key, Key}, {section, <<"http">>}], Req) ->
    Section = << "http \"", Key/binary, "\"" >>,

    Conf = proplists:delete(binary_to_list(Key),
                            coffer_config:get("http")),

    %% remove the connection from the connection supervisor
    ok = ranch:unlink_connection(coffer_http),

    case do_restart_http(nil, Conf) of
        true ->
            coffer_config:del(binary_to_list(Section), binary_to_list(Key)),
            coffer_http_util:ok(202, Req);
        false ->
            coffer_http_util:error(<<"configuration unchanged">>, Req)
    end;
handle_req(<<"DELETE">>, [{key, Key}, {section, Section}], Req) ->
    case coffer_config:del(binary_to_list(Section), binary_to_list(Key)) of
        ok ->
            coffer_http_util:ok(202, Req);
        Error ->
            lager:error("config update: ~p~n", [Error]),
            coffer_http_util:error(500, Req)
    end;

handle_req(_, _, Req) ->
    coffer_http_util:not_allowed([<<"GET">>, <<"DELETE">>, <<"PUT">>], Req).

terminate(_Reason, _Req, _State) ->
    ok.


maybe_restart_http(Section, Key, Req) ->
    case cowboy_req:body(Req) of
        {ok, Bin, Req1} ->
            Value = coffer_util:to_list(jsx:decode(Bin)),
            Conf = coffer_config:get("http"),
            Key1 = binary_to_list(Key),
            Conf1 = lists:keyreplace(Key1, 1, Conf, {Key1, Value}),

            Bind = case Key of
                <<"bind_http">> -> Value;
                _ -> nil
            end,

            case do_restart_http(Bind, Conf1) of
                true ->
                    ok = coffer_config:set(Section, Key, Value),
                    coffer_http_util:ok(Req1);
                false ->
                    coffer_http_util:error(500, "config unchanged", Req1)
            end;
        Error ->
            Error
    end.

do_restart_http(nil, Conf) ->
    Bind = coffer_config:get("core", "bind_http", ""),
    do_restart_http(Bind, Conf);

do_restart_http(Bind, Conf0) ->
    %% we only expect to get string in parse config.
    Conf = lists:map(fun({K, V}) ->
                    {coffer_util:to_list(K), coffer_util:to_list(V)}
            end, Conf0),

    restart_http(Bind, Conf).

restart_http("", _Conf) ->
    case coffer_config:get("core", "bind_http", "") of
        "" ->
            true;
        _ ->
            stop_http(),
            true
    end;
restart_http(Bind, Conf) ->
    %% stop the HTTP API
    %% Note: it would be better to first test if we can start a new listener
    %% gracefully and then stop the old one if it's ok. but unfortunately we
    %% can't yet since we are using cowboy.
    stop_http(),

    Opts = coffer_config:bind_opts(Bind),
    {NbAcceptors, SslOpts, IsSsl} = coffer_config_util:http_config(Conf),
    ListenerOpts = {NbAcceptors, Opts ++ SslOpts, IsSsl},

    case coffer_config:start_listener(ListenerOpts, coffer_config:http_env(),
                                      1) of

        {_, false} ->
            %% restart with the old conf
            coffer_config:init_http(),
            false;
        _ ->
            true
    end.

stop_http() ->
    cowboy:stop_listener(coffer_http),
    lager:info("HTTP API stopped", []).

%% internals
%%
clean_name([ $\s | Rest ]) ->
    clean_name(Rest);
clean_name([ $\" | Rest ]) ->
    clean_name(Rest);
clean_name(Name0) ->
    case re:split(Name0, "\"", [{return, list}]) of
        [Name0] -> list_to_binary(Name0);
        [Name1, _] -> list_to_binary(Name1)
    end.
