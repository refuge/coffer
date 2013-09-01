%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.
%%%

-module(coffer_config_backend).

-type default() :: atom() | string().

%% initialize a config handler
-callback init(Args :: any()) ->
    {ok, Handle ::any()}
    | {error, Reason :: any()}.


%% get all values from the config
-callback all(Handle :: any()) ->
    [{Key :: string(), Value :: string()}] | [].


%% set a config value
-callback set_value(Handle :: any(), Section :: string(), Value :: string()) ->
    ok
    | {error, Reason :: any()}.


-callback set_value(Handle :: any(), Section :: string(), Key :: string(),
                    Value :: string()) ->
    ok
    | {error, Reason :: any()}.


%% get a value frnm the config
-callback get_value(Handle :: any(), Section :: string()) ->
    [{Key :: string(), Value :: string()}]
    | undefined.


-callback get_value(Handle :: any(), Section :: string(), Key :: string()) ->
    Value :: string() | undefined.


-callback get_value(Handle :: any(), Section :: string(), Key :: string(),
                    Default :: default()) ->
    Value :: string()
    | default().


%% delete a value from the config
-callback delete_value(Handle :: any(), Section :: string()) -> ok.


-callback delete_value(Handle :: any(), Section :: string(),
                       Key :: string()) -> ok.


%% terminate the config handler
-callback terminate(Handle :: any()) -> ok.
