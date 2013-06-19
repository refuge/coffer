%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_storages).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    {Method, Req2} = cowboy_req:method(Req),
    {ok, Req3} = maybe_process(Method, Req2),
    {ok, Req3, State}.

%%
maybe_process(<<"GET">>, Req) ->
    Storages = coffer:list_storages(),
    JsonObj = [{<<"storages">>, Storages}],
    {Json, Req1} = coffer_http_util:to_json(JsonObj, Req),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                     Json, Req1);
maybe_process(_, Req) ->
    coffer_http_util:not_allowed([<<"GET">>], Req).

terminate(_Reason, _Req, _State) ->
    ok.
