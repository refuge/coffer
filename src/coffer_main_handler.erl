%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_main_handler).

-export([init/3, handle/2, terminate/2]).

-define(MAX_BODY_SIZE, 4294967296).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    {Method, Req2} = cowboy_req:method(Req),
    {HasBody, Req3} = cowboy_req:has_body(Req2),
    {ok, Req4} = maybe_process_it(Method, HasBody, Req3),
    {ok, Req4, State}.

maybe_process_it(<<"GET">>, false, Req0) ->
    {[ContentId], Req} = cowboy_req:path_info(Req0),
    case coffer_manager:exists(ContentId) of
        true  ->
            {ok, Req2} = cowboy_req:chunked_reply(200, Req),
            {ok, Token} = coffer_manager:get_blob_init(ContentId),
            {ok, Req3} = iterate_over_reading_chunks(Req2, Token),
            {ok, Req3};
        false ->
            cowboy_req:reply(404, [], <<"Doesn't exist">>, Req)
    end;

maybe_process_it(<<"POST">>, true, Req) ->
    {_, Req2} = iterate_over_parts_with(Req),
    cowboy_req:reply(201, [], <<"OK">>, Req2);

maybe_process_it(<<"DELETE">>, false, Req0) ->
    {[ContentId], Req} = cowboy_req:path_info(Req0),
    case coffer_manager:exists(ContentId) of
        true  ->
            cowboy_req:reply(200, [], <<"OK">>, Req);
        false ->
            cowboy_req:reply(404, [], <<"Doesn't exist">>, Req)
    end;

maybe_process_it(<<"OPTIONS">>, false, Req) ->
    RespHeaders = [{<<"Allow">>, <<"OPTIONS, GET, POST, DELETE">>}],
    cowboy_req:reply(200, RespHeaders, <<"">>, Req);

maybe_process_it(_, _, Req) ->
    cowboy_req:reply(405, Req).

terminate(_Req, _State) ->
    ok.

%%

iterate_over_reading_chunks(Req, Token) ->
    case coffer_manager:get_blob(Token) of
        {ok, Data} ->
            ok = cowboy_req:chunk(Data, Req),
            iterate_over_reading_chunks(Req, Token);
        eof ->
            coffer_manager:get_blob_end(Token),
            {ok, Req}
    end.

%iterate_over_writing_chunks(Req, Token, FinalSize) ->
%    case cowboy_req:stream_body(Req) of
%        {ok, Data, Req2} ->
%            %io:format("FinalSize: ~p~n", [FinalSize]),
%            %%io:format("DATA: {~p}~n", [Data]),
%            coffer_manager:store_blob(Token, Data),
%            iterate_over_writing_chunks(Req2, Token, FinalSize+size(Data));
%        {done, Req2} ->
%            coffer_manager:store_blob_end(Token),
%            io:format("Final size: ~p~n", [FinalSize]),
%            {ok, Req2};
%        {error, Reason} ->
%            lager:error("An error occured during writing a blob: ~p", [Reason]),
%%            {error, Reason}
%    end.

%%

iterate_over_parts_with(Req) ->
    iterate_over_parts(cowboy_req:multipart_data(Req), <<>>).

iterate_over_parts({headers, Headers, Req}, _) ->
    Comp = proplists:get_value(<<"content-disposition">>, Headers),

    % TODO: mimetype should be stored as a metadata
    %Mtype = proplists:get_value(<<"content-type">>, Headers),

    SomeFields = binary:split(Comp, <<"; ">>, [global]),
    Fields = lists:foldl(
        fun(X, Acc1) ->
            case binary:split(X, <<"=">>) of
                [_] -> Acc1;
                [Key, Value] -> [{Key, binary:replace(Value, <<"\"">>, <<>>, [global])} | Acc1]
            end
        end,
        [],
        SomeFields
    ),

    ContentId = proplists:get_value(<<"name">>, Fields),

    {ok, Token} = coffer_manager:store_blob_init(ContentId),

    iterate_over_parts(cowboy_req:multipart_data(Req), Token);
iterate_over_parts({body, Data, Req}, Token) ->
    coffer_manager:store_blob(Token, Data),
    iterate_over_parts(cowboy_req:multipart_data(Req), Token);
iterate_over_parts({end_of_part, Req}, Token) ->
    coffer_manager:store_blob_end(Token),
    iterate_over_parts(cowboy_req:multipart_data(Req), <<>>);
iterate_over_parts({eof, Req}, _) ->
    {ok, Req}.
