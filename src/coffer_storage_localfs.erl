%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_storage_localfs).
-behaviour(coffer_storage).

-define(DEFAULT_REPO_HOME, "./data").
-define(DEFAULT_CHUNK_SIZE, 4096).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start/1, stop/1]).
-export([open/2, close/1]).
-export([put/3, get/3, delete/2, all/1, foldl/4, foreach/2]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

-record(config, {
    repo_home = ?DEFAULT_REPO_HOME,
    chunk_size = ?DEFAULT_CHUNK_SIZE
}).
-record(state, {
    config
}).
-record(sref, {
    options = [],
    config,
    properties = []
}).

start(Properties) ->
    % read the configuration
    RepoHome = proplists:get_value(repo_home, Properties, ?DEFAULT_REPO_HOME),
    ChunkSize = proplists:get_value(chunk_size, Properties, ?DEFAULT_CHUNK_SIZE),
    Config = #config{repo_home=RepoHome, chunk_size=ChunkSize},

    maybe_init_repo(RepoHome),

    State = #state{config=Config},
    {ok, State}.

stop(_) ->
    %% nothing for now
    %% perhaps making sure every handle is actually closed?
    ok.

open(#state{config=Config}=_State, Options) ->
    SRef = #sref{options=Options, config=Config},
    {ok, SRef}.

close(#sref{properties=Properties}=_SRef) ->
    % close any remaining iodevice
    case proplists:get_value(iodevice, Properties, undefined) of
        undefined ->
            ok;
        IoDevice ->
            file:close(IoDevice)
    end,
    ok.

put(#sref{config=Config}=SRef, Id, Bin) when is_binary(Bin) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    maybe_create_directory(RepoHome, content_directory(Id)),
    case file:open(Filename, [write, binary]) of
        {ok, IoDevice} ->
            case file:write(IoDevice, Bin) of
                ok ->
                    {ok, SRef};
                {error, Reason} ->
                    lagger:error("Couldn't write data for ID: ~p with reason: ~p", [Id, Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            lager:error("Can't open the file ~p for writing: ~p for blob Id: ~p", [Filename, Reason, Id]),
            {error, Reason}
    end;
put(_Ref, _Id, _Chunk) ->
    {error, not_yet_supported}.

get(#sref{config=Config}=SRef, Id, []) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    case filelib:is_file(Filename) of
        false ->
            {error, not_exist};
        true  ->
            case file:read_file(Filename) of
                {ok, Binary} ->
                    {ok, Binary, SRef};
                {error, Reason} ->
                    {error, Reason}
            end
    end;
get(_SRef, _Id, _Options) ->
    {error, not_yet_supported}.

delete(#sref{config=Config}=SRef, Id) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    case file:delete(Filename) of
        ok ->
            {ok, SRef};
        {error, Reason} ->
            {error, Reason}
    end.

all(_SRef) ->
    {error, not_yet_supported}.

foldl(_SRef, _Func, _InitState, _Options) ->
    {error, not_yet_supported}.

foreach(_SRef, _Func) ->
    {error, not_yet_supported}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

maybe_init_repo(RepoHome) ->
    case file:make_dir(RepoHome) of
        ok ->
            ok;
        {error, eexist} ->
            %% TODO maybe add here something to scan the directory (useful?)
            ok;
        Error ->
            throw({error, cant_init_simple_storage, Error})
    end.

%%

-spec content_directory(Id :: binary()) -> string().
content_directory(Id) ->
    string:sub_string(binary_to_list(Id), 1,2).

-spec content_filename(Id :: binary()) -> string().
content_filename(Id) ->
    string:sub_string(binary_to_list(Id), 3).

-spec content_location(Id :: binary()) -> string().
content_location(Id) ->
    content_directory(Id) ++ "/" ++ content_filename(Id).

-spec content_full_location(RepoHome :: string(), Id :: binary()) -> string().
content_full_location(RepoHome, Id) ->
    RepoHome ++ "/" ++ content_location(Id).

maybe_create_directory(RepoHome, Path) ->
    FullPath = RepoHome ++ "/" ++ Path,
    file:make_dir(FullPath).
