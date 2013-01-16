
-module(coffer_util).

-export([content_hash/1, content_hash_on_stream/2]).

content_hash(Data) ->
    <<Mac:160/integer>> = crypto:sha(Data),
    list_to_binary(lists:flatten(io_lib:format("~40.16.0b", [Mac]))).

%
% Func is a
%   fun(State) -> {Data, NewState}
%              -> {Data, eof}      when it's over
content_hash_on_stream(Func, InitState) ->
    Context = crypto:sha_init(),
    iterate_hash_over_stream(Func, Context, InitState).

iterate_hash_over_stream(_, Context, eof) ->
    <<Mac:160/integer>> = crypto:sha_final(Context),
    list_to_binary(lists:flatten(io_lib:format("~40.16.0b", [Mac])));
iterate_hash_over_stream(Func, Context, State) ->
    {Data, NewState} = Func(State),
    NewContext = crypto:sha_update(Context, Data),
    iterate_hash_over_stream(Func, NewContext, NewState).
