%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2025, 2600Hz
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(fax_ra_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-include("fax.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

-define(FAX_MACHINE_OPTIONS, #{ra_name => ?FAX_RA_NAME
                              ,pg_scope => ?FAX_RA_SCOPE
                              ,ra_system => ?FAX_RA_SYSTEM
                              ,ra_machine => ?FAX_RA_MACHINE
                              ,ra_tick_timeout => ?FAX_RA_TICK_INTERVAL
                              ,ra_auto_leave => ?FAX_RA_AUTO_LEAVE
                              ,ra_derive_datadir => true
                              }).

-define(CHILDREN, [?WORKER_ARGS('pg', [?FAX_RA_SCOPE])
                  ,?WORKER_ARGS(kz_ra_formation, [?FAX_MACHINE_OPTIONS])
                  ]).


%% ===================================================================
%% API functions
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    supervisor:start_link({'local', ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%------------------------------------------------------------------------------
-spec init([]) -> kz_types:sup_init_ret().
init([]) ->
    RestartStrategy = 'one_for_one',
    MaxRestarts = 5,
    MaxSecondsBetweenRestarts = 10,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {'ok', {SupFlags, ?CHILDREN}}.
