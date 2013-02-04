-module(coffer_storage_localfs_test).
-include_lib("eunit/include/eunit.hrl").

-define(setup(Storage, Options, F), {setup, fun() -> start(Storage, Options) end, fun stop/1, F}).
-define(title(Storage, Title), "[Storage " ++ atom_to_list(Storage) ++ "]: " ++ Title).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% TESTS DESCRIPTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%

do_test_() ->
    [
    test_given_storage(coffer_storage_localfs, [{repo_home, "/tmp/coffer_test_data"}, {chunk_size, 4096}])
    ].

test_given_storage(Storage, Options) ->
    [
     {?title(Storage, "Store and retrieve a binary."),
     ?setup(Storage, Options, fun store_and_retrieve_a_binary/1)}
    ].

%%%%%%%%%%%%%%%%%%%%%%%
%%% SETUP FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%

start(Storage, Options) ->
    {ok, State} = Storage:start(Options),
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

%%%%%%%%%%%%%%%%%%%%%%%%
%%% HELPER FUNCTIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

