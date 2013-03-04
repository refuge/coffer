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

    Id1 = <<"0987654321">>,
    Id2 = <<"1234567890">>,
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
    {ok, SRef5} = ?STORAGE:put(SRef4, Id3, {stream, done}),
    PutAlreadyDone = ?STORAGE:put(SRef5, Id3, {stream, done}),

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
    % streaming a not valid content
    StreamNotFound = ?STORAGE:get(SRef, <<"Alice in Wonderland">>, [stream]),

    % trying to add them again
    AlreadyThere1 = ?STORAGE:put(SRef, Id1, Content1),
    AlreadyThere2 = ?STORAGE:put(SRef, Id2, Content2),
    AlreadyThere3 = ?STORAGE:put(SRef, Id3, Content3),

    % grabing unknown contents
    UnknownContent = ?STORAGE:get(SRef, <<"what the hell">>, []),

    {ok, List1} = ?STORAGE:all(SRef),

    % basic folding
    FoldingFun = fun(Id, Acc) ->
        [{id, Id}|Acc]
    end,
    {ok, FoldingResult} = ?STORAGE:foldl(SRef, FoldingFun, [], []),

    % testing foreach by inserting entries in an ets
    Tid = ets:new(coffer_storage_test_ets, [set]),
    ForeachFun = fun(Id) ->
        ets:insert(Tid, {Id, Id})
    end,
    ok = ?STORAGE:foreach(SRef, ForeachFun),
    Id1PresentInForeachEts = ets:member(Tid, Id1),
    Id2PresentInForeachEts = ets:member(Tid, Id2),
    Id3PresentInForeachEts = ets:member(Tid, Id3),

    % delete and list
    {ok, _} = ?STORAGE:delete(SRef, Id2),
    {ok, List2} = ?STORAGE:all(SRef),
    ContentGone = ?STORAGE:get(SRef, Id2, []),
    CantDeleteTwice = ?STORAGE:delete(SRef, Id2),

    ok = ?STORAGE:close(SRef),

    [
      ?_assertEqual(Content1, Binary1),
      ?_assertEqual(Content2, Binary2),
      ?_assertEqual(Content3, Binary3),
      ?_assertEqual({error, already_done}, PutAlreadyDone),
      ?_assertEqual(Fragment, Chunk1),
      ?_assertEqual(Fragment, Chunk2),
      ?_assertEqual(Fragment, Chunk3),
      ?_assertEqual(Fragment, Chunk4),
      ?_assertEqual({error, not_found}, StreamNotFound),
      ?_assertEqual({error, already_exist}, AlreadyThere1),
      ?_assertEqual({error, already_exist}, AlreadyThere2),
      ?_assertEqual({error, already_exist}, AlreadyThere3),
      ?_assertEqual({error, not_found}, UnknownContent),
      ?_assertEqual(true, lists:member(Id1, List1)),
      ?_assertEqual(true, lists:member(Id2, List1)),
      ?_assertEqual(true, lists:member(Id3, List1)),
      ?_assertEqual(true, lists:member({id, Id1}, FoldingResult)),
      ?_assertEqual(true, lists:member({id, Id2}, FoldingResult)),
      ?_assertEqual(true, lists:member({id, Id3}, FoldingResult)),
      ?_assertEqual(true, Id1PresentInForeachEts),
      ?_assertEqual(true, Id2PresentInForeachEts),
      ?_assertEqual(true, Id3PresentInForeachEts),
      ?_assertEqual(true, lists:member(Id1, List2)),
      ?_assertEqual(true, lists:member(Id3, List2)),
      ?_assertEqual({error, not_found}, ContentGone),
      ?_assertEqual({error, not_found}, CantDeleteTwice)
    ].

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

% the end
