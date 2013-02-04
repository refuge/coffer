-module(coffer_storage_localfs_test).
-include_lib("eunit/include/eunit.hrl").

-define(setup(Storage, Options, F), {setup, fun() -> start(Storage, Options) end, fun stop/1, F}).
-define(title(Storage, Title), "[Storage " ++ atom_to_list(Storage) ++ "]: " ++ Title).

-define(TEST_REPO, "/tmp/coffer_storage_test").
-define(TEST_REPO_BIN, <<"/tmp/coffer_storage_test">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

do_test_() ->
    [
    test_given_storage(coffer_storage_localfs, [{repo_home, ?TEST_REPO}, {chunk_size, 4096}])
    ].

test_given_storage(Storage, Options) ->
    [
     {?title(Storage, "Store and retrieve a binary."),
     ?setup(Storage, Options, fun store_and_retrieve_a_binary/1)},
     {?title(Storage, "Store a binary."),
     ?setup(Storage, Options, fun store_a_binary/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%

start(Storage, Options) ->
    {ok, State} = Storage:start(Options),
    ok = Storage:init_storage(State),
    {Storage, State}.

stop({Storage, State}) ->
    Storage:stop(State).

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%

store_and_retrieve_a_binary({Storage, State}) ->
    {ok, SRef} = Storage:open(State, []),

    Content = <<"Hello World!">>,
    ContentHash = coffer_util:content_hash(Content),
    Res = Storage:put(SRef, ContentHash, Content),
    Res2 = Storage:get(SRef, ContentHash, []),

    ok = Storage:close(SRef),
    [?_assertEqual({ok, SRef}, Res), ?_assertEqual({ok, Content, SRef}, Res2)].

store_a_binary({Storage, State}) ->
    {ok, SRef} = Storage:open(State, []),

    Content = <<"Hello World!">>,
    ContentHash = coffer_util:content_hash(Content),
    Res = Storage:put(SRef, ContentHash, Content),

    Filename = content_full_path(ContentHash),
    IsFile = filelib:is_file(Filename),

    ok = Storage:close(SRef),
    [?_assertEqual({ok, SRef}, Res), ?_assertEqual(true, IsFile)].

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

content_dir(Id) when is_binary(Id) ->
    binary:part(Id, {0, 2}).

content_filename(Id) when is_binary(Id) ->
    binary:part(Id, {2, byte_size(Id)-2}).

content_full_path(Id) ->
    binary:list_to_bin([?TEST_REPO_BIN, <<"/">>, content_dir(Id), <<"/">>, content_filename(Id)]).

% the end
