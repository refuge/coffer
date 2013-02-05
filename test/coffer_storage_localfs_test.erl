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
    % testing with a very small chunk size to test streaming with get
    test_with_config([{repo_home, ?TEST_REPO}, {chunk_size, 5}])
    ].

test_with_config(Options) ->
    [
     {?title("Basic API tests."),
     ?setup(Options, fun basic_api_test/1)}
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

basic_api_test({State}) ->
    {ok, SRef} = ?STORAGE:open(State, []),

    Id1 = <<"1234567890">>,
    Id2 = <<"0987654321">>,
    Id3 = <<"af34228709">>,

    Content1 = <<"Hello World!">>,
    Content2 = <<"Once upon a time">>,
    Fragment = <<"12345">>,
    Content3 = binary:list_to_bin([Fragment, Fragment, Fragment, Fragment]),

    % Storing some binary contents
    ?STORAGE:put(SRef, Id1, Content1),
    ?STORAGE:put(SRef, Id2, Content2),
   
    % Storing a streamed content
    {ok, SRef1} = ?STORAGE:put(SRef, Id3, {stream, Fragment}),
    {ok, SRef2} = ?STORAGE:put(SRef1, Id3, {stream, Fragment}),
    {ok, SRef3} = ?STORAGE:put(SRef2, Id3, {stream, Fragment}),
    {ok, SRef4} = ?STORAGE:put(SRef3, Id3, {stream, Fragment}),
    {ok, _SRef5} = ?STORAGE:put(SRef4, Id3, {stream, done}),

    % grabing binary contents now
    {ok, Binary1, _} = ?STORAGE:get(SRef, Id1, []),
    {ok, Binary2, _} = ?STORAGE:get(SRef, Id2, []),

    % grabing the streamed content at once first
    {ok, Binary3, _} = ?STORAGE:get(SRef, Id3, []),
    % then let's stream it
    {chunk, Chunk1, SRef10} = ?STORAGE:get(SRef, Id3, [stream]),
    {chunk, Chunk2, SRef11} = ?STORAGE:get(SRef10, Id3, [stream]),
    {chunk, Chunk3, SRef12} = ?STORAGE:get(SRef11, Id3, [stream]),
    {chunk, Chunk4, SRef13} = ?STORAGE:get(SRef12, Id3, [stream]),
    {chunk, done, _SRef14} = ?STORAGE:get(SRef13, Id3, [stream]),

    ok = ?STORAGE:close(SRef),

    [
      ?_assertEqual(Content1, Binary1),
      ?_assertEqual(Content2, Binary2),
      ?_assertEqual(Content3, Binary3),
      ?_assertEqual(Fragment, Chunk1),
      ?_assertEqual(Fragment, Chunk2),
      ?_assertEqual(Fragment, Chunk3),
      ?_assertEqual(Fragment, Chunk4)
    ].

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

% the end
