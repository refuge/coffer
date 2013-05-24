-module(coffer_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
-export([ensure_deps_started/0, ensure_started/1]).
-export([get_app_env/1, get_app_env/2]).


%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    coffer_deps:ensure(),
    ensure_deps_started(),
    ok =  maybe_start_http(is_embedded()),
    coffer_sup:start_link().

stop(_State) ->
    ok.

maybe_start_http(true) ->
    ok;
maybe_start_http(false) ->
    DispatchRules = coffer_http:dispatch_rules(get_app_env(api_prefix)),
    Dispatch = [{'_', DispatchRules}],
    DefaultListener = {http, http, 100, [{port, 8080}]},
    Listeners = get_app_env(http_listeners, [DefaultListener]),

    lists:foreach(fun
            ({http, Ref, NbAcceptors, Opts}) ->
                {ok, _} = cowboy:start_http(Ref, NbAcceptors, Opts,
                                            [{dispatch, Dispatch}]);
            ({https, Ref, NbAcceptors, Opts}) ->
                {ok, _} = cowboy:start_https(Ref, NbAcceptors, Opts,
                                            [{dispatch, Dispatch}])
        end, Listeners),
    ok.

is_embedded() ->
    get_app_env(embedded, false).

ensure_deps_started() ->
    {ok, Deps0} = application:get_key(coffer, applications),
    Deps = case is_embedded() of
        false ->
            Deps0 ++ [crypto, public_key, ssl, ranch, cowboy];
        true ->
            Deps0
    end,
    true = lists:all(fun ensure_started/1, Deps).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            true;
        {error, {already_started, App}} ->
            true;
        Else ->
            error_logger:error_msg("Couldn't start ~p: ~p", [App, Else]),
            Else
    end.

get_app_env(Env) ->
    get_app_env(Env, undefined).

get_app_env(Env, Default) ->
    case application:get_env(coffer, Env) of
        {ok, Val} -> Val;
        undefined -> Default
    end.
