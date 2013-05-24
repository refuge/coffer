%%% -*- erlang -*-
%%%
%%% This file is part of coffer-server released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_http).

-export([dispatch_rules/1]).

dispatch_rules(Prefix0) ->
    Rules = [{[], coffer_http_root, []},
             {[container], coffer_http_container, []},
             {[container, <<"stat">>], coffer_http_stat, []},
             {[container, blob], coffer_http_blob, []}],

    case maybe_prefix(Prefix0) of
        [] ->
            Rules;
        Prefix ->
            lists:reverse(lists:foldl(fun({Pattern, Mod, Args}, Acc) ->
                            Pattern1 = Prefix ++ Pattern,
                            [{Pattern1, Mod, Args} | Acc]
                    end, [], Rules))
    end.


%% internal
maybe_prefix(Prefix) when is_binary(Prefix) ->
    [Prefix];
maybe_prefix(_) ->
    [].
