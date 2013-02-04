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
-export([init_storage/1]).
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
    iodevice = undefined
}).

start(Properties) when is_list(Properties) ->
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

init_storage(#state{config=Config}=_State) ->
    RepoHome = Config#config.repo_home,
    case filelib:fold_files(
        RepoHome,
        ".*",
        true,
        fun(Filename, Acc) ->
            case file:delete(Filename) of
                ok -> Acc;
                {error, Reason} -> [{error, Filename, Reason} | Acc]
            end
        end,
        []
    ) of
        [] -> ok;
        Other -> {error, Other}
    end.

open(#state{config=Config}=_State, Options) when is_list(Options) ->
    SRef = #sref{options=Options, config=Config},
    {ok, SRef}.

close(#sref{iodevice=undefined}=_SRef) ->
    ok;
close(#sref{iodevice=IoDevice}=_SRef) ->
    file:close(IoDevice),
    ok.

put(SRef, Id, Bin) when is_binary(Bin) ->
    case put(SRef, Id, {stream, Bin}) of
        {ok, SRef1} ->
            put(SRef1, Id, {stream, done});
        Other ->
            Other
    end;
put(#sref{config=Config, iodevice=undefined}=SRef, Id, {stream, Bin}) when is_binary(Bin) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    maybe_create_directory(RepoHome, content_directory(Id)),
    case file:open(Filename, [exclusive, append, binary]) of
        {ok, IoDevice} ->
            put(SRef#sref{iodevice=IoDevice}, Id, {stream, Bin});
        {error, Reason} ->
            {error, Reason}
    end;
put(#sref{iodevice=IoDevice}=SRef, _Id, {stream, Bin}) when is_binary(Bin) ->
    case file:write(IoDevice, Bin) of
        ok ->
            {ok, SRef};
        {error, Reason} ->
            {error, Reason}
    end;
put(#sref{iodevice=undefined}=_SRef, _Id, {stream, done}) ->
    {error, already_done};
put(#sref{iodevice=IoDevice}=SRef, _Id, {stream, done}) ->
    file:close(IoDevice),
    {ok, SRef#sref{iodevice=undefined}}.

get(#sref{config=Config}=SRef, Id, []) when is_binary(Id) ->
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
get(#sref{config=Config, iodevice=undefined}=SRef, Id, [stream]) when is_binary(Id) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    case filelib:is_file(Filename) of
        false ->
            {error, not_exist};
        true  ->
            case file:open(Filename, [read, binary]) of
                {ok, IoDevice} ->
                    get(SRef#sref{iodevice=IoDevice}, Id, [stream]);
                {error, Reason} ->
                    {error, Reason}
            end
    end;
get(#sref{config=Config, iodevice=IoDevice}=SRef, _Id, [stream]) ->
    ChunkSize = Config#config.chunk_size,
    case file:read(IoDevice, ChunkSize) of
        {ok, Binary} ->
            {chunk, Binary, SRef#sref{iodevice=IoDevice}}; 
        eof ->
            file:close(IoDevice),
            {chunk, done, SRef#sref{iodevice=undefined}};
        {error, Reason} ->
            {error, Reason}
    end.

delete(#sref{config=Config}=SRef, Id) when is_binary(Id) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    case file:delete(Filename) of
        ok ->
            {ok, SRef};
        {error, Reason} ->
            {error, Reason}
    end.

all(#sref{}=SRef) ->
    Func = fun(X, Acc) -> [X|Acc] end,
    foldl(SRef, Func, [], []).

foldl(#sref{config=Config}=_SRef, Func, InitState, []) when is_function(Func, 2) ->
    RepoHome = Config#config.repo_home,

    ProcessFilename = fun(Filename, Acc) ->
        Elements = string:tokens(Filename, "/"),
        Length = length(Elements),
        Id = list_to_binary(lists:nth(Length - 1, Elements) ++ lists:nth(Length, Elements)),
        Func(Id, Acc)
    end,

    Value = filelib:fold_files(
        RepoHome,
        ".+",
        true,
        ProcessFilename,
        InitState
    ),

    case Value of
        List when is_list(List) ->
            {ok, List};
        BadValue ->
            {error, BadValue}
    end;
% TODO missing Options support
foldl(#sref{}=_SRef2, Func, _InitState, Options) when is_list(Options), is_function(Func, 2) ->
    {error, not_yet_supported}.

foreach(#sref{}=SRef, Func) when is_function(Func, 1) ->
    FoldingFun = fun(X, _) ->
      Func(X),
      []
    end,
    foldl(SRef, FoldingFun, [], []),
    ok.

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
