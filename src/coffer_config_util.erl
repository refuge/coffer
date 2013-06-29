-module(coffer_config_util).


-export([parse_http_config/1,
         http_settings/0,
         http_ref/1]).

-define(DEFAULT_PORT, 7000).

parse_http_config(Section) ->
    Conf = econfig:get_value(coffer_config, Section),
    case proplists:get_value("listen", Conf) of
        undefined ->
            unbound;
        Addr ->
            %% get listener ref
            ListenerRef = http_ref(Section),

            %% get max of acceptors
            NbAcceptors = list_to_integer(
                    proplists:get_value("nb_acceptors", Conf, "100")
            ),

            %% parse the IP an port
            Opts = case parse_address(list_to_binary(Addr)) of
                {any, Port} ->
                    [{port, Port}];
                {Ip, Port} ->
                    {ok, ParsedIp} = inet_parse:address(Ip),
                    [{port, Port}, {ip, ParsedIp}]
            end,

            %% append the SSL configuration if needed
            case is_ssl(Conf) of
                false ->
                    {ListenerRef, {NbAcceptors, Opts, false}};
                true ->
                    Opts1 = Opts ++ ssl_options(Section),
                    {ListenerRef, {NbAcceptors, Opts1, true}}
            end
        end.

http_settings() ->
    case econfig:prefix(coffer_config, "http") of
        [] -> [];
        Sections ->
            lists:foldl(fun(Section, Acc) ->
                        case parse_http_config(Section) of
                            unbound -> Acc;
                            Conf -> [Conf | Acc]
                        end
                end, [], Sections)
    end.

http_ref(Section) ->
    "http" ++ Rest = Section,
    http_ref1(Rest).

http_ref1([ $\s | Rest ]) ->
    http_ref1(Rest);
http_ref1([ $\" | Rest ]) ->
    http_ref1(Rest);
http_ref1(Name0) ->
    Name = case re:split(Name0, "\"", [{return, list}]) of
        [Name0] -> Name0;
        [Name1, _] -> Name1
    end,
    list_to_atom(Name).

%% internals
%%
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
            {any, list_to_integer(binary_to_list(Port))};
        [Host, Port] ->
            {binary_to_list(Host),
             list_to_integer(binary_to_list(Port))}
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
