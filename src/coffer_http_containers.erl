%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_containers).

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
maybe_process(<<"GET">>, Req) ->
    Containers = coffer:list_storages(),

    Json = jsx:encode([{<<"containers">>,Containers}]),
    PrettyJson = jsx:prettify(Json),

    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}], PrettyJson, Req);
maybe_process(_, Req) ->
    coffer_http_util:not_allowed([<<"GET">>], Req).

terminate(_Req, _State) ->
    ok.
