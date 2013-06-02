%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.
%%
%% Describes storage API
%% ---------------------
%%
%% Each storage implementation must at least implements those functions.
%%
-module(coffer_backend).

%% Types

-type blob_ref() :: binary().

%% callbacks -----------------------------------------------------------------

%% INIT/STOP
%%
-callback init(StorageName :: binary(), Config :: list()) ->
    {ok, State :: any()}
    | {error, Reason :: any()}.

-callback terminate(State :: any()) ->
    ok
    | {error, Reason :: any()}.


%% CHANGE/ERASE A BLOB
%%
-callback new_receiver(BlobRef :: blob_ref(), From :: pid(),
                       State :: any()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any(), NewState :: any()}.

-callback delete(State :: any(), BlobRef :: blob_ref()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any()}.


%% GET BLOB
%%
-callback new_stream({BlobRef :: blob_ref(), Window :: integer()}, To :: pid(),
                State :: any()) ->
    {ok, StreamPid :: pid()}
    | {error, any()}.

-callback blob_exists(BlobRef :: blob_ref(), State :: any()) ->
    ok
    | {error, Reason :: any()}.


%% ENUMERATE
%%
-callback enumerate(To :: pid(), State :: any()) ->
    {ok, EnumeratePid :: pid()}
    | {error, Reason :: any()}.


%% STAT
%%
-callback stat(BlobRefs :: [blob_ref()], State :: any()) ->
    {ok, {Found ::  [blob_ref()], Missing :: [blob_ref()],
          Partials :: [blob_ref()]}, State :: any() }
    | {error, Reason :: any(), State :: any()}.

