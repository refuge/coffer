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
    Config = lists:foldl(fun({Key, Val}, Acc) ->
                    KVs = [{list_to_binary(K),list_to_binary(V)}
                           || {K, V} <- Val],
                    [{list_to_binary(Key), KVs} | Acc]
            end, [], coffer_config:all()),

    {JsonConfig, Req1} = coffer_http_util:to_json(Config, Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                 JsonConfig, Req1);

handle_req(<<"GET">>, [{section, Section}], Req) ->
    KVs = [{list_to_binary(K), list_to_binary(V)}
           || {K, V} <- coffer_config:get(binary_to_list(Section))],
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
            Bind = coffer_config:get("core", "bind_http", ""),
            case Section of
                <<"core">> when Bind /= "" ->
                    %% remove the connection from the connection supervisor
                    ok = ranch:unlink_connection(coffer_http);

                <<"http">> ->
                    %% remove the connection from the connection supervisor
                    ok = ranch:unlink_connection(coffer_http);
                _ ->
                    ok
            end,
            case coffer_config:set(Section, Config) of
                ok ->
                    coffer_http_util:ok(202, Req1);
                {error, Reason} ->
                    coffer:error(Reason, Req1);
                _ ->
                    coffer:error(<<"unknown error">>, Req1)
            end;
        Error ->
            Error
    end;

handle_req(<<"PUT">>, [{key, Key}, {section, Section}], Req) ->
    case cowboy_req:body(Req) of
        {ok, Bin, Req1} ->
            Value = jsx:decode(Bin),
            case {Section, Key} of
                {<<"core">>, <<"bind_http">>} ->
                    ok = ranch:unlink_connection(coffer_http);
                {<<"http">>, _} ->
                    ok = ranch:unlink_connection(coffer_http);
                _ ->
                    ok
            end,

            case coffer_config:set(Section, Key, Value) of
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
    Bind = coffer_config:get("core", "bind_http", ""),
    case Section of
        <<"core">> when Bind /= "" ->
            %% remove the connection from the connection supervisor
            ok = ranch:unlink_connection(coffer_http);
        <<"http">> ->
            %% remove the connection from the connection supervisor
            ok = ranch:unlink_connection(coffer_http);
        _ ->
            ok
    end,
    case coffer_config:del(Section) of
        ok ->
            coffer_http_util:ok(202, Req);
        Error ->
            lager:error("config update: ~p~n", [Error]),
            coffer_http_util:error(Error, Req)
    end;

handle_req(<<"DELETE">>, [{key, Key}, {section, Section}], Req) ->
    case {Section, Key} of
        {<<"core">>, <<"bind_http">>} ->
            ok = ranch:unlink_connection(coffer_http);
        {<<"http">>, _} ->
            ok = ranch:unlink_connection(coffer_http);
        _ ->
            ok
    end,
    case coffer_config:del(Section, Key) of
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
