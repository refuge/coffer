%%% -*- erlang -*-
%%%
%%% This file is part of coffer released under the Apache license 2.
%%% See the NOTICE for more information.

-module(coffer_util).

-export([content_hash/1, content_hash_on_stream/2]).

%% "registry-like" functions
-export([register/1,
         register_member/1,
         unregister/1,
         unregister_member/1,
         lookup/1,
         lookup_members/1,
         await/1,
         await/2]).

-export([gettempdir/0]).
-export([require/1]).
-export([to_list/1]).

-ifdef(sha_workaround).
-define(SHA(Data), crypto:sha(Data)).
-define(SHA_INIT(), crypto:sha_init()).
-define(SHA_UPDATE(Ctx, Data), crypto:sha_update(Ctx, Data)).
-define(SHA_FINAL(Ctx), crypto:sha_final(Ctx)).
-else.
-define(SHA(Data), crypto:hash(sha, Data)).
-define(SHA_INIT(), crypto:hash_init(sha)).
-define(SHA_UPDATE(Ctx, Data), crypto:hash_update(Ctx, Data)).
-define(SHA_FINAL(Ctx), crypto:hash_final(Ctx)).
-endif.

%% @doc Register the local process under a local name
%% @end
-spec register(tuple()) -> true.
register(Name) ->
    gproc:add_local_name(Name).

%% @doc Register the local process as a member of a group
%% @end
-spec register_member(tuple()) -> true.
register_member(Group) ->
    gproc:reg({p, l, Group}, member).


%% @doc Unregister the local process from a name
%% @end
-spec unregister(tuple()) -> true.
unregister(Name) ->
    gproc:unreg({n, l, Name}).

%% @doc Unregister the local process as a member of a group
%% @end
-spec unregister_member(tuple()) -> true.
unregister_member(Group) ->
    gproc:unreg({p, l, Group}).


%% @doc Resolve a local name to a pid
%% @end
-spec lookup(tuple()) -> pid().
lookup(Name) ->
    gproc:lookup_pid({n, l, Name}).

%% @doc Lookup the process id's of all members of a group
%% @end
-spec lookup_members(tuple()) -> [pid()].
lookup_members(Group) ->
    gproc:lookup_pids({p, l, Group}).


%% @doc Wait until a process registers under a local name
%% @end
-spec await(tuple()) -> pid().
await(Name) ->
    await(Name, 5000).

-spec await(tuple(), non_neg_integer()) -> pid().
await(Name, Timeout) ->
    {Pid, undefined} = gproc:await({n, l, Name}, Timeout),
    Pid.

content_hash(Data) ->
    <<Mac:160/integer>> = ?SHA(Data),
    list_to_binary(lists:flatten(io_lib:format("~40.16.0b", [Mac]))).

%% @spec gettempdir() -> string()
%% @doc Get a usable temporary directory using the first of these that is a directory:
%%      $TMPDIR, $TMP, $TEMP, "/tmp", "/var/tmp", "/usr/tmp", ".".
gettempdir() ->
    gettempdir(gettempdir_checks(), fun normalize_dir/1).

gettempdir_checks() ->
    [{fun os:getenv/1, ["TMPDIR", "TMP", "TEMP"]},
     {fun gettempdir_identity/1, ["/tmp", "/var/tmp", "/usr/tmp"]},
     {fun gettempdir_cwd/1, [cwd]}].

gettempdir_identity(L) ->
    L.

gettempdir_cwd(cwd) ->
    {ok, L} = file:get_cwd(),
    L.

gettempdir([{_F, []} | RestF], Normalize) ->
    gettempdir(RestF, Normalize);
gettempdir([{F, [L | RestL]} | RestF], Normalize) ->
    case Normalize(F(L)) of
        false ->
            gettempdir([{F, RestL} | RestF], Normalize);
        Dir ->
            Dir
    end.

normalize_dir(False) when False =:= false orelse False =:= "" ->
    %% Erlang doesn't have an unsetenv, wtf.
    false;
normalize_dir(L) ->
    Dir = filename:absname(L),
    case filelib:is_dir(Dir) of
        false ->
            false;
        true ->
            Dir
    end.
%
% Func is a
%   fun(State) -> {Data, NewState}
%              -> {Data, eof}      when it's over
content_hash_on_stream(Func, InitState) ->
    Context = ?SHA_INIT(),
    iterate_hash_over_stream(Func, Context, InitState).

iterate_hash_over_stream(_, Context, eof) ->
    <<Mac:160/integer>> = ?SHA_FINAL(Context),
    list_to_binary(lists:flatten(io_lib:format("~40.16.0b", [Mac])));
iterate_hash_over_stream(Func, Context, State) ->
    {Data, NewState} = Func(State),
    NewContext = ?SHA_UPDATE(Context, Data),
    iterate_hash_over_stream(Func, NewContext, NewState).

%% @doc Start the given applications if they were not already started.
-spec require(list(module())) -> ok.
require([]) ->
	ok;
require([App|Rest]) ->
	case application:start(App) of
		ok -> ok;
		{error, {already_started, App}} -> ok
	end,
	require(Rest).


%% @doc converto a value to a list
to_list(V) when is_list(V) ->
    V;
to_list(V) when is_binary(V) ->
    binary_to_list(V);
to_list(V) when is_integer(V) ->
    integer_to_list(V).
