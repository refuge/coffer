%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.
%%%

-module(coffer_blob).

-export([validate_blobref/1,
         parse_blobref/1,
         blob_path/2]).

blob_path(Root, BlobRef) ->
    {HashType, Hash} = parse_blobref(BlobRef),
    << A:1/binary, B:1/binary, C:1/binary, FName/binary >> = Hash,
    filename:join([Root, HashType, A, B, C, FName]).

parse_blobref(Ref) ->
    Re = get_blob_regexp(),
    case re:run(Ref, Re, [{capture, all, binary}]) of
        nomatch ->
            error;
        Matches ->
            {match, [_, HashType, Hash]} = Matches,
            {HashType, Hash}
    end.

validate_blobref(Ref) ->
    Re = get_blob_regexp(),
    case re:run(Ref, Re, [{capture, none}]) of
        nomatch ->
            error;
        _ ->
            ok
    end.

get_blob_regexp() ->
    %% we cache the regexp so it can be reused in the same process
    case get(blob_regexp) of
        undefined ->
            {ok, RegExp} = re:compile("^([a-z][a-zA-Z0-9^-]*)-([a-zA-Z0-9]*)$"),
            put(blob_regexp, RegExp),
            RegExp;
        RegExp ->
            RegExp
    end.
