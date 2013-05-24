%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_util).

-export([not_allowed/2]).
-export([not_found/1, error/2]).
-export([to_json/1]).

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
    AllowedMethodsWithComaList = lists:reverse(ReversedAllowedMethodsWithComaList),
    AllowedMethodsWithComa = iolist_to_binary(AllowedMethodsWithComaList),

    %% TODO probably better to use a json instead of text
	cowboy_req:reply(405, [{<<"Allow">>, AllowedMethodsWithComa}], <<"Not allowed">>, Req).

not_found(Req) ->
    ReturnedData = [{<<"error">>, <<"not found">>}],
    PrettyJson = to_json(ReturnedData),
    cowboy_req:reply(404, [{<<"Content-Type">>, <<"application/json">>}], PrettyJson, Req).

error(Reason, Req) ->
    ReasonInBinary = iolist_to_binary(io_lib:format("~p", [Reason])),
    ReturnedData = [{<<"error">>, ReasonInBinary}],
    PrettyJson = to_json(ReturnedData),
    cowboy_req:reply(500, [{<<"Content-Type">>, <<"application/json">>}], PrettyJson, Req).

to_json(Data) ->
    FlatJson = jsx:encode(Data),
    jsx:prettify(FlatJson).
