-module(coffer_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).
-export([ensure_deps_started/0, ensure_started/1]).
-export([get_app_env/1, get_app_env/2]).

-include_lib("../deps/hackney/include/hackney.hrl").

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
    Dispatch1 = cowboy_router:compile(Dispatch),
    Env = [{env, [{dispatch, Dispatch1}]}],
    DefaultListener = {"http://127.0.0.1:7000", http, 100, []},
    Listeners = get_app_env(listeners, [DefaultListener]),


    lists:foreach(fun({UrlStr, Ref, NbAcceptors, Opts}) ->
                %% parse URL
                #hackney_url{host=Ip,
                             port=Port,
                             scheme=Scheme} = hackney_url:parse_url(UrlStr),

                {ok, ParsedIp} = inet_parse:address(Ip),
                Opts1 = [{port, Port},
                         {ip, ParsedIp}] ++ Opts,

                %% start HTTP
                case Scheme of
                    http ->
                        {ok, _} = cowboy:start_http(Ref, NbAcceptors,
                                                    Opts1, Env);
                    https ->
                        {ok, _} = cowboy:start_https(Ref, NbAcceptors,
                                                     Opts, Env)
                end
        end, Listeners),
    ok.

is_embedded() ->
    get_app_env(listeners, []) =:= [].

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
