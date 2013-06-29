%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_server).
-behaviour(gen_server).
-define(SERVER, ?MODULE).


%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1]).
-export([list/0]).
-export([add/3, remove/1]).
-export([get/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).


-define(DEFAULT_NB_ACCEPTORS, 100).
-define(DEFAULT_TIMEOUT, 30).
-define(DEFAULT_LISTENER,{default, {100, [{port, 7000}], false}}).


%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Args, []).

list() ->
    gen_server:call(?MODULE, {list}).

add(StorageName, Backend, Config) ->
    gen_server:call(?MODULE, {add, StorageName, Backend, Config}).

remove(StorageName) ->
    gen_server:call(?MODULE, {remove, StorageName}).

get(StorageName) ->
    gen_server:call(?MODULE, {get, StorageName}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

% gen_server state records
-record(state, {
    storages = [],
    options = [],
    http_config = [],
    graceful_timeout
}).
-record(storage, {
    name,
    backend,
    config = [],
    pid
}).

init([]) ->
    %
    % storages : [
    %     { StorageName, Mod, Config }
    % ]
    %
    StoragesConfig = coffer_app:get_app_env(storages, []),
    FinalState = lists:foldl(
        fun({StorageName, Backend, Config}, State) ->
            {_, NewState} = do_add_storage(StorageName, Backend, Config,
                                           State),
            NewState
        end,
        #state{},
        StoragesConfig
    ),

    %% start HTTP
    HttpConfig = init_http(),

    %% subscribe to config changes
    econfig:subscribe(coffer_config),

    %% return initial stat
    {ok, FinalState#state{http_config=HttpConfig}}.

handle_call({list}, _From, #state{storages=Storages}=State) ->
    Reply = lists:foldl(
        fun({StorageName, _Storage}, Acc) ->
            [StorageName|Acc]
        end,
        [],
        Storages
    ),
    {reply, Reply, State};
handle_call({add, StorageName, Backend, Config}, _From, State) ->
    {Reply, NewState} = do_add_storage(StorageName, Backend, Config, State),
    {reply, Reply, NewState};
handle_call({remove, StorageName}, _From, State) ->
    {Reply, NewState} = do_remove_storage(StorageName, State),
    {reply, Reply, NewState};
handle_call({get, StorageName}, _From, #state{storages=Storages}=State) ->
    Reply = case proplists:get_value(StorageName, Storages) of
        undefined ->
            {error, not_found};
        #storage{pid=Pid} ->
            Pid
    end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({config_updated, coffer_config, {set, {Section, Key}}}, State) ->
    HttpEnabled = http_enabled(),
    NewState = case re:split(Section, "http", [{return, list}]) of
        [Section] ->
            update_config(Section, Key, State);
        _ when HttpEnabled =:= true ->
            NewConf = coffer_config_util:parse_http_config(Section),
            update_http_config(Section, Key, NewConf, State)
    end,
    {noreply, NewState};

handle_info({config_updated, coffer_config, {delete, {Section, Key}}},
            State) ->
    HttpEnabled = http_enabled(),
    NewState = case re:split(Section, "http", [{return, list}]) of
        [Section] ->
            update_config(Section, Key, State);
        _ when HttpEnabled =:= true ->
            NewState0 = delete_http_config(Section, Key, State),
            %% make sure we start a default config if needed.
            case {HttpEnabled, NewState0} of
                {true, #state{http_config=[]}} ->
                    NewState0#state{http_config=init_http()};
                _ ->
                    NewState0
            end
    end,
    {noreply, NewState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{storages=Storages,
                          http_config=HttpConfs}=_State) ->

    %% stop the HTTP API
    lists:foreach(fun({Ref, {_, _, _}}) ->
                ranch:stop_listener(Ref)
        end, HttpConfs),

    %% stop storages
    lists:foldl(
        fun(#storage{pid=Pid}=_Storage, _Acc) ->
            coffer_storage:stop(Pid)
        end,
        [],
        Storages
    ),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

do_add_storage(StorageName, Backend, Config, State)
        when is_list(StorageName) ->
    do_add_storage(iolist_to_binary(StorageName), Backend, Config, State);
do_add_storage(StorageName, Backend, Config,
               #state{storages=Storages}=State) ->
    case proplists:get_value(StorageName, Storages) of
        undefined ->
            relager:info("Starting storage: ~p with backend: ~p",
                       [StorageName, Backend]),
            case coffer_storage:start(StorageName, Backend, Config) of
                {ok, Pid} ->
                    lager:info("Storage ~p successfully started!",
                               [StorageName]),
                    Storage = #storage{name=StorageName,
                                       backend=Backend,
                                       config=Config,
                                       pid=Pid},
                    UpdatedStorages = [ {StorageName, Storage} | Storages ],
                    NewState = State#state{storages=UpdatedStorages},
                    {ok, NewState};
                ErrorAtLoad ->
                    lager:error("Error when loading storage ~p: ~p~n", [
                            StorageName, ErrorAtLoad]),
                    {{error, cant_start}, State}
            end;
        _AlreadyThere ->
            lager:error("Storage ~p already exists!", [StorageName]),
            {{error, already_exists}, State}
    end.

do_remove_storage(StorageName, #state{storages=Storages}=State) ->
    case proplists:get_value(StorageName, Storages) of
        undefined ->
            {{error, not_found}, State};
        #storage{pid=Pid}=_Storage ->
            lager:info("Stopping storage ~p", [StorageName]),
            case coffer_storage:stop(Pid) of
                ok ->
                    lager:info("Storage ~p is now stopped", [StorageName]),
                    UpdatedStorages = proplists:delete(StorageName, Storages),
                    {ok, State#state{storages=UpdatedStorages}};
                {error, Reason} ->
                    {{error, Reason}, State}
            end
    end.


%% manage config
%%

http_enabled() ->
    case econfig:get_value(coffer_config, "coffer", "enable_http",
                           "true") of
        "true" -> true;
        _ -> false
    end.

init_http() ->
    %% get all settings
    Settings0 = coffer_config_util:http_settings(),

    %a if no http settings and HTTP is enabled set the default.
    Settings = case {http_enabled(), Settings0} of
        {true, []} ->
            coffer_util:require([crypto, public_key, ssl, ranch,
                                 cowboy]),
            [?DEFAULT_LISTENER];
        {true, _} ->
            coffer_util:require([crypto, public_key, ssl, ranch,
                                 cowboy]),
            Settings0;
        _ -> []
    end,

    %% get default env
    Env = get_http_env(),

    %% start all HTTP interfaces
    lists:foldl(fun(Conf, Acc) ->
                case start_listener(Conf, Env, 5) of
                    {ok, Listener} -> [Listener | Acc];
                    error -> Acc
                end
        end, [], Settings).


%% start a listener
start_listener({Ref, _}, _, 0) ->
    lager:info("Listener ~p not started~n", [Ref]),
    error;
start_listener({Ref, {NbAcceptors, TransOpts, Ssl}}=Conf, ProtoOpts,
                Tries) ->
    Transport = case Ssl of
        true -> ranch_ssl;
        _ -> ranch_tcp
    end,

    case ranch:start_listener(Ref, NbAcceptors, Transport, TransOpts,
                              cowboy_protocol, ProtoOpts) of
        {ok, _} ->
            lager:info("start to listen: ~p~n", [Ref]),
            {ok, Conf};
        Error ->
            lager:error("error starting ~p: ~p~n", [Ref, Error]),
            start_listener(Conf, ProtoOpts, Tries-1)
    end.


restart_listener({Ref, _}=Conf, #state{http_config=HttpConfs}=State) ->
    %% stop the listerener first if it already exist.
    case lists:keyfind(Ref, 1, HttpConfs) of
        false ->
            ok;
        _ ->
            lager:info("HTTP: stop ~p~n", [Ref]),
            ranch:stop_listener(Ref)
    end,

    lager:info("restart listener ~p with ~p~n", [Ref, Conf]),
    %% start the listener with new options
    Env = get_http_env(),

    NewConfs = case start_listener(Conf, Env, 5) of
        {ok, Conf} ->
            lager:info("ici", []),
            lists:keyreplace(Ref, 1, HttpConfs, Conf);
        error ->
            lists:keydelete(Ref, 1, HttpConfs)
    end,
    lager:info("la", []),
    State#state{http_config=NewConfs}.


get_http_env() ->
    DispatchRules = coffer_http:dispatch_rules(),
    Dispatch = [{'_', DispatchRules}],
    Dispatch1 = cowboy_router:compile(Dispatch),
    [{env, [{dispatch, Dispatch1}]}].

update_http_config(Section, _Key, unbound,
                   #state{http_config=HttpConfs}=State) ->
    Ref = coffer_config_util:http_ref(Section),

    case lists:keyfind(Ref, 1, HttpConfs) of
        false ->
            State;
        _ ->
            lager:info("http config: stop ~p~n", [Ref]),
            ranch:stop_listener(Ref),
            NewConfs = lists:keydelete(Ref, 1, HttpConfs),
            State#state{http_config=NewConfs}
    end;
update_http_config(_, "nb_acceptors", {Ref, {N, _, _}}, State) ->
    lager:info("update HTTP config: ~p~n", [Ref]),
    ranch:set_max_connections(Ref, N),
    State;
update_http_config(_Section, _Key, Conf, State) ->
    restart_listener(Conf, State).

delete_http_config(Section, "listen",
                   #state{http_config=HttpConfs}=State) ->
    Ref = coffer_config_util:http_ref(Section),

    %% stop the listener
    lager:info("HTTP: stop ~p~n", [Ref]),
    ranch:stop_listener(Ref),

    %% reset the confs
    NewConfs = lists:keydelete(Ref, 1, HttpConfs),
    State#state{http_config=NewConfs};
delete_http_config(Section, "nb_acceptors", State) ->
    case coffer_config_util:parse_http_config(Section) of
        unbound -> ok;
        {Ref, {N, _, _}} ->
            lager:info("update HTTP config: ~p~n", [Ref]),
            ranch:set_max_connections(Ref, N)
    end,
    State;
delete_http_config(Section, _, State) ->

    case coffer_config_util:parse_http_config(Section) of
        unbound ->
            State;
        Conf ->
            restart_listener(Conf, State)
    end.

update_config("coffer", "enable_http",
              #state{http_config=HttpConfs}=State) ->
    case {http_enabled(), HttpConfs} of
        {true, []} ->
            lager:info("start the HTTP API.~n", []),
            NewHttpConfs = init_http(),
            State#state{http_config=NewHttpConfs};
        {true, _} ->
            State;
        {false, []} ->
            State;
        {false, _} ->
            lager:info("stop the HTTP API.~n", []),
            stop_http(State)
    end;
update_config(_, _, State) ->
    State.

stop_http(#state{http_config=HttpConfs}=State) ->
    lists:foreach(fun({Ref, {_, _, _}}) ->
                ranch:stop_listener(Ref)
        end, HttpConfs),
    State#state{http_config=[]}.
