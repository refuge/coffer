
-module(coffer_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Args), {I, {I, start_link, Args}, permanent, 5000, worker, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {Backend, BackendArgs} = case application:get_env(coffer, backend) of
        undefined ->
            {coffer_simple_storage, [[]]};
        {ok, Other} ->
            Other
    end,
    BlobManager = ?CHILD(coffer_manager, [[Backend, BackendArgs]]),

    Children = [BlobManager],
    RestartStrategy = {one_for_one, 1, 60},
    {ok, { RestartStrategy, Children } }.
