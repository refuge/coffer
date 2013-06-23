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

-define(DEFAULT_PORT, 7000).

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
    http_config
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

    HttpConf = maybe_start_http(),
    {ok, FinalState#state{http_config=HttpConf}}.

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

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{storages=Storages}=_State) ->
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
            lager:info("Starting storage: ~p with backend: ~p",
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

maybe_start_http() ->
    HttpConf = http_config(),
    EnableHttp = econfig:get_value(coffer_config, "core", "enable_http",
                                   "true"),
    DispatchRules = coffer_http:dispatch_rules(),
    Dispatch = [{'_', DispatchRules}],
    Dispatch1 = cowboy_router:compile(Dispatch),
    Env = [{env, [{dispatch, Dispatch1}]}],

    HttpConf1 = case {HttpConf, EnableHttp} of
        {[], "true"} ->
            coffer_util:require([crypto, public_key, ssl, ranch,
                                 cowboy]),
            [{default, {100, [{port, 7000}], false}}];
        {_, "true"} ->
            coffer_util:require([crypto, public_key, ssl, ranch,
                                 cowboy]),
            HttpConf;
        _ ->
            []
    end,

    lists:foreach(fun
            ({Ref, {NbAcceptors, Opts, false}}) ->
                {ok, _} = cowboy:start_http(Ref, NbAcceptors, Opts, Env);
            ({Ref, {NbAcceptors, Opts, true}}) ->
                {ok, _} = cowboy:start_https(Ref, NbAcceptors, Opts, Env)
        end, HttpConf1),
    HttpConf1.

http_config() ->
    case econfig:prefix(coffer_config, "http") of
        [] -> [];
        Sections ->
            lists:foldl(fun(Section, Acc) ->
                        ["", RefStr] = re:split(Section, "http ",
                                                [{return, list}]),
                        Ref = list_to_atom(RefStr),
                        Conf = econfig:get_value(coffer_config,
                                                 Section),

                        case proplists:get_value("listen", Conf) of
                            undefined ->
                                Acc;
                            Addr ->
                                {Ip, Port} = parse_address(Addr),
                                NbAcceptors = list_to_integer(
                                        proplists:get_value("nb_acceptors",
                                                            Conf, "100")
                                        ),
                                {ok, ParsedIp} = inet_parse:address(Ip),
                                Opts = [{port, Port}, {ip, ParsedIp}],
                                case is_ssl(Conf) of
                                    false ->
                                        [{Ref, {NbAcceptors, Opts,
                                                false}} | Acc];
                                    true ->
                                        Opts1 = Opts ++ ssl_options(Section),
                                        [{Ref, {NbAcceptors, Opts1,
                                                true}} | Acc]
                                end
                        end
                end, [], Sections)
    end.

ssl_options(Section) ->
    CertFile = econfig:get_value(coffer_config, Section, "cert_file", nil),
    KeyFile = econfig:get_value(coffer_config, Section, "key_file", nil),
    case CertFile /= nil of
        true ->
            SslOpts0 = [{certfile, CertFile}],

            %% open certfile to get entries.
            {ok, PemBin} = file:read_file(CertFile),
            CertEntries = public_key:pem_decode(PemBin),

            SslOpts = case econfig:get_value(coffer_config, Section,
                                             "key_file", nil) of
                nil ->
                    if length(CertEntries) >= 2 ->
                            SslOpts0;
                        true ->
                            lager:error("SSL Private Key is missing", []),
                            throw({error, missing_keyfile})
                    end;
                KeyFile ->
                    SslOpts0 ++ [{keyfile, KeyFile}]
            end,

            %% set password if one is needed for the cert
            SslOpts1 = case econfig:get_value(coffer_config, Section,
                                              "password", nil) of
                nil -> SslOpts;
                Password ->
                    SslOpts ++ [{password, Password}]
            end,

            %% check if cacerts are already set in the pem file
            SslOpts2 = case econfig:get_value(coffer_config, Section,
                                              "cacert_file", nil) of
                nil ->
                    case CertEntries of
                        [_P, _Cert| CaCerts] when CaCerts /= [] ->
                            SslOpts1 ++ [{cacerts, CaCerts}];
                        _ ->
                            SslOpts1
                    end;
                CaCertFile ->
                    SslOpts1 ++ [{cacertfile, CaCertFile}]
            end,

            % do we verify certificates ?
            FinalSslOpts = case econfig:get_value(coffer_config, Section,
                                                  "verify_ssl_certificates",
                                                  "false") of
                "false" ->
                    SslOpts2 ++ [{verify, verify_none}];
                "true" ->
                    %% get depth
                    Depth = list_to_integer(
                            econfig:get_value(coffer_config, Section,
                                              "ssl_certificate_max_depth",
                                              "1")
                    ),
                    %% check if we need a CA.
                    WithCA = SslOpts1 /= SslOpts1,
                    case WithCA of
                        false when Depth >= 1 ->
                           lager:error("Verify SSL certificate "
                                    ++"enabled but file containing "
                                    ++"PEM encoded CA certificates is "
                                    ++"missing", []),
                            throw({error, missing_cacerts});
                        _ ->
                            ok
                    end,
                    [{depth, Depth},{verify, verify_peer}]
            end,
            FinalSslOpts;
        false ->
            lager:error("SSL enabled but PEM certificates are missing.", []),
            throw({error, missing_certs})
    end.


is_ssl(Conf) ->
    proplists:get_value("ssl", Conf, "false") =:= "true".

parse_address(Addr) when is_list(Addr) ->
    parse_address(list_to_binary(Addr));
parse_address(<<"[", Rest/binary>>) ->
    case binary:split(Rest, <<"]">>) of
        [Host, <<>>] ->
            {binary_to_list(Host), ?DEFAULT_PORT};
        [Host, <<":", Port/binary>>] ->
            {binary_to_list(Host),
             list_to_integer(binary_to_list(Port))};
        _ ->
            parse_address(Rest)
    end;
parse_address(Addr) ->
    case binary:split(Addr, <<":">>) of
        [Port] ->
            {"0.0.0.0", list_to_integer(binary_to_list(Port))};
        [Host, Port] ->
            {binary_to_list(Host),
             list_to_integer(binary_to_list(Port))}
    end.

