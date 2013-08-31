%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_config_util).

-export([http_config/0]).


http_config() ->
    Conf = econfig:get_value(coffer_config, "http"),

    %% get max of acceptors
    NbAcceptors = list_to_integer(
            proplists:get_value("nb_acceptors", Conf, "100")
    ),

    %% parse the SSL configuration if needed
    case is_ssl(Conf) of
        false ->
            {NbAcceptors, [], false};
        true ->
            {NbAcceptors, ssl_options(Conf), true}
    end.


%% internals
%%

ssl_options(Conf) ->
    CertFile = proplists:get_value("cert_file", Conf, nil),
    KeyFile = proplists:get_value("key_file", Conf, nil),
    case CertFile /= nil of
        true ->
            SslOpts0 = [{certfile, CertFile}],

            %% open certfile to get entries.
            {ok, PemBin} = file:read_file(CertFile),
            CertEntries = public_key:pem_decode(PemBin),

            SslOpts = case KeyFile of
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
            SslOpts1 = case proplists:get_value("password", Conf, nil) of
                nil -> SslOpts;
                Password ->
                    SslOpts ++ [{password, Password}]
            end,

            %% check if cacerts are already set in the pem file
            SslOpts2 = case proplists:get_value("cacert_file", Conf, nil) of
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
            FinalSslOpts = case proplists:get_value("verify_ssl_certificates",
                                                    Conf, "false") of
                "false" ->
                    SslOpts2 ++ [{verify, verify_none}];
                "true" ->
                    %% get depth
                    Depth = list_to_integer(
                        proplists:get_value("ssl_certificate_max_depth", Conf,
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
