%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http_blob).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

-compile([{parse_transform, lager_transform}]).

init(_Transport, Req, []) ->
    {ok, Req, undefined}.

handle(Req, State) ->
    {Method, Req2} = cowboy_req:method(Req),
    {StorageName, Req3} = cowboy_req:binding(container, Req2),
    {BlobRef, Req4} = cowboy_req:binding(blob, Req3),
    {ok, Req5} = maybe_process(StorageName, BlobRef, Method, Req4),
    {ok, Req5, State}.

maybe_process(StorageName, BlobRef, <<"HEAD">>, Req) ->
    case coffer:get_storage(StorageName) of
        {error, Reason} ->
            coffer_http_util:error(Reason, Req);
        StoragePid ->
            case coffer:blob_exists(StoragePid, BlobRef) of
                ok ->
                    cowboy_req:reply(200, [], [], Req);
                {error, not_found} ->
                    cowboy_req:reply(404, [], [], Req)
            end
    end;
maybe_process(StorageName, BlobRef, <<"DELETE">>, Req) ->
    case coffer:get_storage(StorageName) of
        {error, not_found} ->
            coffer_http_util:not_found(Req);
        {error, Reason} ->
            coffer_http_util:error(Reason, Req);
        _ ->
            StoragePid = coffer:get_storage(StorageName),
            case coffer:delete(StoragePid, BlobRef) of
                ok ->
                    StatusMessage = [
                        { <<"deleted">>,
                            [
                                {<<"blobref">>, BlobRef}
                            ]
                        }
                    ],
                    {Json, Req1} =  coffer_http_util:to_json(
                            StatusMessage, Req),
                    cowboy_req:reply(202, [], Json, Req1);
                {error, not_found} ->
                    coffer_http_util:not_found(Req)
            end
    end;
maybe_process(StorageName, BlobRef, <<"PUT">>, Req) ->
    case coffer:get_storage(StorageName) of
        {error, not_found} ->
            coffer_http_util:not_found(Req);
        {error, Reason} ->
            coffer_http_util:error(Reason, Req);
        _ ->
            StoragePid = coffer:get_storage(StorageName),
            case coffer:new_upload(StoragePid, BlobRef) of
                {ok, Receiver} ->
                    case stream_in_blob(Receiver, Req) of
                        {ok, UploadSize, Req2} ->
                            StatusMessage = [
                                { <<"received">>, [
                                    [
                                        {<<"blobref">>, BlobRef},
                                        {<<"size">>, UploadSize}]
                                    ]
                                }
                            ],
                            {Json, Req3} = coffer_http_util:to_json(StatusMessage,
                                                                    Req2),
                            cowboy_req:reply(201, [], Json, Req3);
                        {Error, Req2} ->
                            lager:error("problem uploading blob id ~p: ~p",
                                        [BlobRef, Error]),
                            coffer_http_util:error(400, Error, Req2)
                    end;
                {error, {already_exists, _, _}} ->
                    lager:error("problem uploading blob id ~p with error: ~p",
                                [BlobRef, already_exists]),
                    coffer_http_util:error(409, {error, already_exists}, Req);
                UnknownError ->
                    lager:error("problem uploading blob id ~p with error: ~p",
                                [BlobRef, UnknownError]),
                     coffer_http_util:error(500, {error, already_exists}, Req)
            end
    end;
maybe_process(StorageName, BlobRef, <<"GET">>, Req) ->
    case coffer:get_storage(StorageName) of
        {error, not_found} ->
            coffer_http_util:not_found(Req);
        {error, Reason} ->
            coffer_http_util:error(Reason, Req);
        Storage ->
            case coffer:new_stream(Storage, BlobRef) of
                {ok, Stream} ->
                    BodyFun = fun(Socket, Transport) ->
                        do_stream_out_blob(Stream, Socket, Transport),
                        ok
                    end,
                    cowboy_req:reply(200, [], BodyFun, Req);
                {error, Error} ->
                    lager:error("Error fetching the blob ~pn: ~p", [BlobRef,
                                                                    Error]),
                    coffer_http_util:not_found(Req)
            end
    end;
maybe_process(_, _, _, Req) ->
    coffer_http_util:not_allowed([<<"HEAD">>, <<"GET">>, <<"PUT">>, <<"DELETE">>], Req).

terminate(_Reason, _Req, _State) ->
    ok.

%% ---

stream_in_blob(Receiver, Req) ->
    case cowboy_req:stream_body(Req) of
        {ok, Bin, Req2} ->
            case coffer:upload(Receiver, Bin) of
                {ok, Receiver1} ->
                    stream_in_blob(Receiver1, Req2);
                Error ->
                    {Error, Req2}
            end;
        {done, Req2} ->
            case coffer:upload(Receiver, eob) of
                {ok, UploadSize} ->
                    {ok, UploadSize, Req2};
                Error ->
                    {Error, Req2}
            end
    end.

do_stream_out_blob(Stream, Socket, Transport) ->
    case coffer:fetch(Stream) of
        {ok, coffer_eob} ->
            ok;
        {error, Error} ->
            Json = jsx:encode([{<<"error">>, Error}]),
            Transport:send(Socket, Json),
            ok;
        {ok, Bin} ->
            Transport:send(Socket, Bin),
            do_stream_out_blob(Stream, Socket, Transport)
    end.
