%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_util).

-export([not_allowed/2]).
-export([not_found/1, error/2]).
-export([to_json/2]).
-export([maybe_prettify_json/2]).

not_allowed(AllowedMethods, Req) ->
    AddCommaFunc = fun(Element, Acc) ->
        case Acc of
            [<<>>] ->
                [Element|Acc];
            _ ->
                [Element,<<",">>|Acc]
        end
	end,
    ReversedAllowedMethodsWithComaList = lists:foldl(
        AddCommaFunc,
        [<<"">>],
        AllowedMethods
    ),
    AllowedMethodsWithComaList = lists:reverse(
            ReversedAllowedMethodsWithComaList
    ),
    AllowedMethodsWithComa = iolist_to_binary(AllowedMethodsWithComaList),
    Json = [{<<"Allow">>, AllowedMethodsWithComa}],
    {Json1, Req1} = to_json(Json, Req),
	cowboy_req:reply(405, Json1, <<"Not allowed">>, Req1).

not_found(Req) ->
    ReturnedData = [{<<"error">>, <<"not found">>}],
    {Json, Req1} = to_json(ReturnedData, Req),
    cowboy_req:reply(404, [{<<"Content-Type">>, <<"application/json">>}],
                     Json, Req1).

error(Reason, Req) ->
    ReasonInBinary = iolist_to_binary(io_lib:format("~p", [Reason])),
    ReturnedData = [{<<"error">>, ReasonInBinary}],
    {Json, Req1} = to_json(ReturnedData, Req),
    cowboy_req:reply(500, [{<<"Content-Type">>, <<"application/json">>}],
                     Json, Req1).

to_json(Json, Req) ->
    maybe_prettify_json(jsx:encode(Json), Req).

maybe_prettify_json(Json, Req) ->
    case cowboy_req:qs_val(<<"pretty">>, Req) of
        {undefined, Req1} ->
            {Json, Req1};
        {<<"true">>, Req1} ->
           {jsx:prettify(Json), Req1}
    end.
