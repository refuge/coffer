%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_stat).

-export([init/3]).
-export([handle/2]).
-export([terminate/2]).


init(_Transport, Req, []) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    {Method, Req2} = cowboy_req:method(Req),
    {StorageName, Req3} = cowboy_req:binding(container, Req2),
    Storage = coffer:get_storage(StorageName),
    {ok, Req4} = handle_stat(Storage, Method, Req3),
    {ok, Req4, State}.

handle_stat(Storage, <<"GET">>, Req) ->
    {KVs, Req1} = cowboy_req:qs_vals(Req),
    BlobRefs = [BlobRef || {_, BlobRef} <- KVs],
    stat_response(BlobRefs, Storage, Req1);
handle_stat(Storage, <<"POST">>, Req) ->
    {KVs, Req1} = cowboy_req:body_qs(1024000, Req),
    BlobRefs = [BlobRef || {_, BlobRef} <- KVs],
    stat_response(BlobRefs, Storage, Req1);
handle_stat(_, _, Req) ->
    coffer_http_util:not_allowed([<<"GET">>, <<"POST">>], Req).

terminate(_Req, _State) ->
    ok.

stat_response(BlobRefs, Storage, Req) ->
    JsonObj = stat_object(Storage, BlobRefs),
    Json = jsx:encode(JsonObj),
    PrettyJson = jsx:prettify(Json),
    cowboy_req:reply(200, [{<<"Content-Type">>, <<"application/json">>}],
                     PrettyJson, Req).

stat_object(Storage, BlobRefs) ->
    {ok, {Found, _Missing, Partials}} = coffer:stat(Storage, BlobRefs),
    MaxUploadSize = lists:max([Size || {_, Size} <- Found]),
    Obj = [{<<"stat">>, Found}, {<<"maxUploadSize">>, MaxUploadSize}],
    case Partials of
        [] ->
            Obj;
        _ ->
            Obj + [{<<"alreadyHavePartially">>, Partials}]
    end.


