%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_root).

-export([init/3]).
-export([handle/2]).
-export([terminate/2]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    {Method, Req2} = cowboy_req:method(Req),
    {ok, Req3} = maybe_process(Method, Req2),
    {ok, Req3, State}.

%%
maybe_process(<<"HEAD">>, Req) ->
    cowboy_req:reply(200, [{<<"Content-Type">>,
                            <<"application/json">>}], <<>>, Req);
maybe_process(<<"GET">>, Req) ->
    Json = jsx:encode(server_info()),
    {Json1, Req1} = coffer_http_util:maybe_prettify_json(Json, Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                     Json1, Req1);
maybe_process(_, Req) ->
    coffer_http_util:not_allowed([<<"GET">>, <<"HEAD">>], Req).

terminate(_Req, _State) ->
    ok.


server_info() ->
    [{<<"server">>, <<"coffer">>},
      {<<"version">>, coffer_version()}].

coffer_version() ->
    case application:get_key(coffer, vsn) of
        {ok, Version} ->
            list_to_binary(Version);
        _ ->
            << "0.0.0" >>
    end.
