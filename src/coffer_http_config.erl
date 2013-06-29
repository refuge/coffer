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
                    [{list_to_binary(Key), list_to_binary(Val)} | Acc]
            end, [], econfig:cfg2list(coffer_config, " ")),

    lager:info("got ~p~n", [Config]),
    {JsonConfig, Req1} = coffer_http_util:to_json(Config, Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                 JsonConfig, Req1);

handle_req(_, _, Req) ->
    coffer_http_util:not_allowed([<<"GET">>, <<"PUT">>], Req).

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
