-module(coffer_http_config).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).


init(_Transport, Req, []) ->
    {ok, Req, undefined}.


handle(Req, State) ->
    {Method, Req2} = cowboy_req:method(Req),
    {Bindings, Req3} = cowboy_req:bindings(Req2),
    {ok, handle_req(Method, Bindings, Req3), State}.

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

handle_req(<<"GET">>, [{section, <<"http">>}], Req) ->
    %% clean config, convert to binay value
    %%
    Config = lists:map(fun(Name0) ->
                    Settings = econfig:get_value(coffer_config, Name0),
                    KVs = [{list_to_binary(K),
                            list_to_binary(V)} || {K, V}
                                                  <- Settings],
                    "http" ++ Name = Name0,
                    {clean_name(Name), KVs}
            end, econfig:prefix(coffer_config, "http")),
    {JsonConfig, Req1} = coffer_http_util:to_json([{<<"htttp">>, Config}], Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                 JsonConfig, Req1);
handle_req(<<"GET">>, [{section, Section}], Req) ->
    KVs = [{list_to_binary(K), list_to_binary(V)}
           || {K, V} <- econfig:get_value(coffer_config,
                                          binary_to_list(Section))],
    {Json, Req1} = coffer_http_util:to_json([{Section, KVs}], Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                 Json, Req1);
handle_req(<<"GET">>, [{key, Key}, {section, <<"http">>}], Req) ->

    HttpSections = lists:map(fun(Section) ->
                    "http" ++ Name = Section,
                    {clean_name(Name), Section}
            end, econfig:prefix(coffer_config, "http")),

    Config = case lists:keyfind(Key, 1, HttpSections) of
        false ->
            [{}];
        {_, Section} ->
            Settings = econfig:get_value(coffer_config, Section),
            [{list_to_binary(K), list_to_binary(V)} || {K, V} <- Settings]
    end,
    {JsonConfig, Req1} = coffer_http_util:to_json(Config, Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                 JsonConfig, Req1);
handle_req(<<"GET">>, [{key, Key}, {section, Section}], Req) ->
    case econfig:get_value(coffer_config,  binary_to_list(Section),
                           binary_to_list(Key)) of
        undefined ->
            coffer_http_util:not_found(Req);
        Val ->
            {Json, Req1} = coffer_http_util:to_json(list_to_binary(Val), Req),
            cowboy_req:reply(200, [{<<"Content-Type">>,
                                    <<"application/json">>}], Json,
                             Req1)
    end;
handle_req(<<"PUT">>, [{key, Key}, {section, <<"http">>}], Req) ->
    case cowboy_req:body(Req) of
        {ok, Bin, Req1} ->
            Value = jsx:decode(Bin),
            Section = << "http \"", Key/binary, "\"" >>,
            case econfig:set_value(coffer_config, Section, Value) of
                ok ->
                    coffer_http_util:ok(202, Req1);
                Error ->
                    lager:error("config update: ~p~n", [Error]),
                    coffer_http_util:error(500, Req1)
            end;
        Error ->
            Error
    end;


handle_req(<<"PUT">>, [{key, Key}, {section, Section}], Req) ->
    case cowboy_req:body(Req) of
        {ok, Bin, Req1} ->
            Value = jsx:decode(Bin),
            case econfig:set_value(coffer_config, Section, Key,
                                   Value) of
                ok ->
                    coffer_http_util:ok(202, Req1);
                Error ->
                    lager:error("config update: ~p~n", [Error]),
                    coffer_http_util:error(500, Req1)
            end;
        Error ->
            Error
    end;
handle_req(<<"DELETE">>, [{key, Key}, {section, <<"http">>}], Req) ->
    Section = << "http \"", Key/binary, "\"" >>,

    case econfig:delete_value(coffer_config, Section) of
        ok ->
            coffer_http_util:ok(202, Req);
        Error ->
            lager:error("config update: ~p~n", [Error]),
            coffer_http_util:error(500, Req)
    end;

handle_req(<<"DELETE">>, [{key, Key}, {section, Section}], Req) ->
    case econfig:delete_value(coffer_config, Section, Key) of
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
