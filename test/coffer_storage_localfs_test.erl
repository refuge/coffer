-module(coffer_storage_localfs_test).
-include_lib("eunit/include/eunit.hrl").

-define(STORAGE, coffer_storage_localfs).
-define(setup(Options, F), {setup, fun() -> start(Options) end, fun stop/1, F}).
-define(title(Title), "[Storage " ++ atom_to_list(?STORAGE) ++ "]: " ++ Title).

-define(TEST_REPO, "/tmp/coffer_storage_test").
-define(TEST_REPO_BIN, <<"/tmp/coffer_storage_test">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

do_test_() ->
    [
    test_with_config([{repo_home, ?TEST_REPO}, {chunk_size, 4096}])
    ].

test_with_config(Options) ->
    [
     {?title("Store a binary."),
     ?setup(Options, fun store_a_binary/1)},
     {?title("Store a stream."),
     ?setup(Options, fun store_a_stream/1)},
     {?title("Store and retrieve a binary."),
     ?setup(Options, fun store_and_retrieve_a_binary/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%

start(Options) ->
    {ok, State} = ?STORAGE:start(Options),
    ok = ?STORAGE:init_storage(State),
    {State}.

stop({State}) ->
    ?STORAGE:stop(State).

%%%%%%%%%%%%%%%%%%%%
%%% ACTUAL TESTS %%%
%%%%%%%%%%%%%%%%%%%%

store_a_binary({State}) ->
    Content = <<"Hello World!">>,
    ContentHash = coffer_util:content_hash(Content),
    Filename = content_full_path(ContentHash),

    {ok, SRef} = ?STORAGE:open(State, []),
    NoFile = filelib:is_file(Filename),
    Res = ?STORAGE:put(SRef, ContentHash, Content),
    IsFile = filelib:is_file(Filename),
    {ok, Binary} = file:read_file(Filename),
    ok = ?STORAGE:close(SRef),

    [?_assertEqual({ok, SRef}, Res),
     ?_assertEqual(false, NoFile),
     ?_assertEqual(true, IsFile),
     ?_assertEqual(Content, Binary)].

store_a_stream({State}) ->
    Fragment = <<"Once upon a time">>,
    Full = binary:list_to_bin([Fragment, Fragment, Fragment, Fragment]),
    Id = <<"1234567890">>,
    Filename = content_full_path(Id),

    {ok, SRef0} = ?STORAGE:open(State, []),
    NoFile = filelib:is_file(Filename),
    {ok, SRef1} = ?STORAGE:put(SRef0, Id, {stream, Fragment}),
    {ok, SRef2} = ?STORAGE:put(SRef1, Id, {stream, Fragment}),
    {ok, SRef3} = ?STORAGE:put(SRef2, Id, {stream, Fragment}),
    {ok, SRef4} = ?STORAGE:put(SRef3, Id, {stream, Fragment}),
    {ok, SRef5} = ?STORAGE:put(SRef4, Id, {stream, done}),
    IsFile = filelib:is_file(Filename),
    {ok, Binary} = file:read_file(Filename),
    ok = ?STORAGE:close(SRef5),

    [?_assertEqual(false, NoFile),
     ?_assertEqual(true, IsFile),
     ?_assertEqual(Full, Binary)].

store_and_retrieve_a_binary({State}) ->
    Content = <<"Hello World!">>,
    ContentHash = coffer_util:content_hash(Content),

    {ok, SRef} = ?STORAGE:open(State, []),
    Res = ?STORAGE:put(SRef, ContentHash, Content),
    Res2 = ?STORAGE:get(SRef, ContentHash, []),
    ok = ?STORAGE:close(SRef),

    [?_assertEqual({ok, SRef}, Res),
     ?_assertEqual({ok, Content, SRef}, Res2)].

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
