-module(coffer_simple_storage).
-behaviour(coffer_storage).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-define(DEFAULT_REPO_HOME, "./data").
-define(DEFAULT_CHUNK_SIZE, 4096).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/1, stop/0]).
-export([init_storage/1]).
-export([get_blob_init/1, get_blob/1, get_blob_end/1]).
-export([store_blob_init/1, store_blob/2, store_blob_end/1]).
-export([get_blob_content/1]).
-export([store_blob_content/2]).
-export([remove_blob/1]).
-export([fold_blobs/2]).
-export([exists/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

stop() ->
    gen_server:call(?MODULE, {stop}).

init_storage(Options) ->
    gen_server:call(?MODULE, {init_storage, Options}).

get_blob_init(Id) ->
    gen_server:call(?MODULE, {get_blob_init, Id}).

get_blob(Token) ->
    gen_server:call(?MODULE, {get_blob, Token}).

get_blob_end(Token) ->
    gen_server:call(?MODULE, {get_blob_end, Token}).

get_blob_content(Id) ->
    gen_server:call(?MODULE, {get_blob_content, Id}).

store_blob_init(Id) ->
    gen_server:call(?MODULE, {store_blob_init, Id}).

store_blob(Token, Data) ->
    gen_server:call(?MODULE, {store_blob, Token, Data}).

store_blob_end(Token) ->
    gen_server:call(?MODULE, {store_blob_end, Token}).

store_blob_content(Id, Data) ->
    gen_server:call(?MODULE, {store_blob_content, Id, Data}).

remove_blob(Id) ->
    gen_server:call(?MODULE, {remove_blob, Id}).

fold_blobs(Func, InitState) ->
    gen_server:call(?MODULE, {fold_blobs, Func, InitState}).

exists(Id) ->
    gen_server:call(?MODULE, {exists, Id}).


%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

-record(config, {repo_home, chunk_size}).
-record(state, {config}).
-record(token, {id, iodevice, filename, repo_home}).

init(Properties) ->

    % read the configuration
    RepoHome = proplists:get_value(repo_home, Properties, ?DEFAULT_REPO_HOME),
    ChunkSize = proplists:get_value(chunk_size, Properties, ?DEFAULT_CHUNK_SIZE),
    Config = #config{repo_home=RepoHome, chunk_size=ChunkSize},

    % see if the repository needs to be created
    maybe_init_repo(RepoHome),

    State = #state{config=Config},
    {ok, State}.

handle_call({stop}, _From, State) ->
    {stop, normal, ok, State};
handle_call({init_storage, Options}, _From, #state{config=Config}=State) ->
    Reply = do_init_storage(Config, Options),
    {reply, Reply, State};
handle_call({get_blob_init, Id}, _From, #state{config=Config}=State) ->
    Reply = do_start_get(Id, Config),
    {reply, Reply, State};
handle_call({get_blob, Token}, _From, #state{config=Config}=State) ->
    Reply = do_get(Token, Config),
    {reply, Reply, State};
handle_call({get_blob_end, Token}, _From, State) ->
    Reply = do_get_end(Token),
    {reply, Reply, State};
handle_call({get_blob_content, Id}, _From, #state{config=Config}=State) ->
    Reply = do_get_blob_content(Id, Config),
    {reply, Reply, State};
handle_call({store_blob_init, Id}, _From, #state{config=Config}=State) ->
    Reply = do_start_store(Id, Config),
    {reply, Reply, State};
handle_call({store_blob, Token, Data}, _From, State) ->
    Reply = do_store(Token, Data),
    {reply, Reply, State};
handle_call({store_blob_end, Token}, _From, State) ->
    Reply = do_store_end(Token),
    {reply, Reply, State};
handle_call({store_blob_content, Id, Data}, _From, #state{config=Config}=State) ->
    Reply = do_store_blob_content(Id, Config, Data),
    {reply, Reply, State};
handle_call({remove_blob, Id}, _From, #state{config=Config}=State) ->
    Reply = do_remove(Id, Config),
    {reply, Reply, State};
handle_call({fold_blobs, Func, InitState}, _From, #state{config=Config}=State) ->
    Reply = do_fold_blobs(Func, InitState, Config),
    {reply, Reply, State};
handle_call({exists, Id}, _From, #state{config=Config}=State) ->
    Reply = do_exists(Id, Config),
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

maybe_init_repo(RepoHome) ->
    case file:make_dir(RepoHome) of
        ok ->
            ok;
        {error, eexist} ->
            %% TODO add here something to scan the directory
            ok;
        Error ->
            throw({error, cant_init_simple_storage, Error})
    end.

do_init_storage(Config, _Options) ->
    RepoHome = Config#config.repo_home,

    % first remove any blob
    filelib:fold_files(
        RepoHome,
        ".+",
        true,
        fun(Filename, _Acc) ->
            case file:delete(Filename) of
                ok -> ok;
                {error, Reason} -> lager:error("OOPS file ~p: ~p~n", [Filename, Reason])
            end
        end,
        []),
    
    % then remove their directories
    Dirs = filelib:wildcard("*", RepoHome),
    lists:foldl(
        fun(Dir, _Acc) ->
            case file:del_dir(RepoHome ++ "/" ++ Dir) of
                ok -> ok;
                {error, Reason} -> lager:error("OOPS dir ~p: ~p~n", [Dir, Reason])
            end
        end,
        [],
        Dirs
    ),

    ok.

do_start_get(Id, Config) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    case filelib:is_file(Filename) of
        false ->
            {error, not_exist};
        true  ->
            {ok, IoDevice} = file:open(Filename, [read, binary]),
            Token = #token{id=Id, iodevice=IoDevice, filename=Filename, repo_home=RepoHome},
            {ok, Token}
    end.

do_get(#token{iodevice=IoDevice}=_Token, Config) ->
    ChunkSize = Config#config.chunk_size,
    case file:read(IoDevice, ChunkSize) of
        {ok, Data} ->
            {ok, Data};
        eof ->
            eof;
        {error, Reason} ->
            {error, Reason}
    end.

do_get_end(#token{iodevice=IoDevice}=_Token) ->
    file:close(IoDevice),
    ok.

%%

do_start_store(Id, Config) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    maybe_create_directory(RepoHome, content_directory(Id)),
    {ok, IoDevice} = file:open(Filename, [write, binary]),
    Token = #token{id=Id, iodevice=IoDevice, filename=Filename, repo_home=RepoHome},
    {ok, Token}.

do_store(#token{id=Id, iodevice=IoDevice}=_Token, Data) ->
    case file:write(IoDevice, Data) of
        ok ->
            ok;
        {error, Reason} ->
            lagger:error("Couldn't write data for ID: ~p with reason: ~p", [Id, Reason]),
            {error, Reason}
    end.

do_store_end(#token{iodevice=IoDevice}=_Token) ->
    file:close(IoDevice),
    ok.

%%

do_get_blob_content(Id, Config) ->
    {ok, Token} = do_start_get(Id, Config),
    iterate_over_data(Token, Config, do_get(Token, Config), []).

do_store_blob_content(Id, Config, Data) ->
    {ok, Token} = do_start_store(Id, Config),
    do_store(Token, Data),
    do_store_end(Token).

%%

do_remove(Id, Config) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    case file:delete(Filename) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

do_fold_blobs(Func, InitState, Config) ->
    RepoHome = Config#config.repo_home,

    ProcessFilename = fun(Filename, Acc) ->
        Elements = string:tokens(Filename, "/"),
        Length = length(Elements),
        Id = list_to_binary(lists:nth(Length - 1, Elements) ++ lists:nth(Length, Elements)),
        Func(Id, Acc)
    end,

    filelib:fold_files(
        RepoHome,
        ".+",
        true,
        ProcessFilename,
        InitState
    ).

do_exists(Id, Config) ->
    RepoHome = Config#config.repo_home,
    Filename = content_full_location(RepoHome, Id),
    filelib:is_file(Filename).

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

iterate_over_data(Token, _Config, eof, Acc) ->
    do_get_end(Token),
    {ok, list_to_binary(lists:reverse(Acc))};
iterate_over_data(Token, Config, {ok, Data}, Acc) ->
    NewAcc = [Data | Acc],
    iterate_over_data(Token, Config, do_get(Token, Config), NewAcc);
iterate_over_data(Token, _Config, {error, Reason}, _) ->
    do_get_end(Token),
    {error, Reason}.
