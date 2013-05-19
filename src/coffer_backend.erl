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
-type options() :: list().

%% callbacks -----------------------------------------------------------------

% INIT/STOP

-callback init(Config :: list()) ->
    {ok, State :: any()}
    | {error, Reason :: any()}.

-callback terminate(State :: any()) ->
    ok
    | {error, Reason :: any()}.



% CHANGE/ERASE A BLOB

-callback new_receiver(BlobRef :: blob_ref(), From :: pid(),
                       State :: any()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any(), NewState :: any()}.

-callback handle_delete(State :: any(), BlobRef :: blob_ref()) ->
    {ok, NewState :: any()}
    | {error, Reason :: any()}.


% GET BLOB

-callback new_stream({BlobRef :: blob_ref(), Window :: integer()}, To :: pid(),
                State :: any()) ->
    {ok, StreamPid :: pid()}
    | {error, any()}.


% QUERY callbacks
% those functions don't change anything by themselves
% but merely iterate over the blobs

% TODO we can remove this one from the storage
% all is a special case of foldl and can be
% handled by the coffer_storage behaviour
% for all storage (to discuss...)
-callback handle_all(State :: any()) ->
    {ok, List :: list()}
    | {error, Reason :: any()}.

-callback handle_foldl(State :: any(), Func :: fun((BlobRef :: blob_ref(), Acc :: any()) -> Final :: any()), Initstate :: any(), Options :: options()) ->
    {ok, Final :: any()}
    | {error, Reason :: any()}.

% TODO foreach is also a special case of foldl without taking care of the final result
% (to discuss)
-callback handle_foreach(State :: any(), Func :: fun((BlobRef :: blob_ref()) -> Final :: any())) ->
    ok
    | {error, Reason :: any()}.


