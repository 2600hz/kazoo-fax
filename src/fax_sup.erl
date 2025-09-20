%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2025, 2600Hz
%%% @doc
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fax_sup).

-behaviour(supervisor).

-include("fax.hrl").

-define(SERVER, ?MODULE).

-export([start_link/0]).
-export([cache_proc/0]).
-export([listener_proc/0]).
-export([smtp_sessions/0]).
-export([init/1]).


-define(ORIGIN_BINDINGS, [[{'db', ?KZ_FAXES_DB}, {'type', <<"faxbox">>}]]).

-define(CACHE_PROPS, [{'origin_bindings', ?ORIGIN_BINDINGS}]).

-define(SMTP_ARGS, [{'protocol', 'tcp'}
                   ,{'family', 'inet'}
                   ,{'address', {0, 0, 0, 0}}
                   ,{'port', ?SMTP_PORT}
                   ,{'protocol', 'smtp'}
                   ,{'callbackoptions', []} %% Options passed to fax_smtp:init/4
                   ]).
%% in case we want to make the settings constant per execution
%%                                  ,{'sessionoptions', [?SMTP_CALLBACK_OPTIONS]}

-define(CHILDREN, [?WORKER('fax_init')
                  ,?CACHE_ARGS(?CACHE_NAME, ?CACHE_PROPS)
                  ,?SUPER('fax_requests_sup')
                  ,?SUPER('fax_worker_sup')
                  ,?WORKER('fax_shared_listener')
                  ,gen_smtp_server:child_spec('fax_smtp', 'fax_smtp', ?SMTP_ARGS)
                   %%,?WORKER_ARGS('gen_smtp_server', ?SMTP_ARGS)
                  ,?SUPER('fax_ra_sup')
                  ]).

%%==============================================================================
%% API functions
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the supervisor.
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> kz_types:startlink_ret().
start_link() ->
    supervisor:start_link({'local', ?SERVER}, ?MODULE, []).

-spec cache_proc() -> {'ok', ?CACHE_NAME}.
cache_proc() ->
    {'ok', ?CACHE_NAME}.

-spec listener_proc() -> {'ok', pid()}.
listener_proc() ->
    [P] = [P || {Mod, P, _, _} <- supervisor:which_children(?SERVER),
                Mod =:= 'fax_listener'],
    {'ok', P}.

-spec smtp_sessions() -> non_neg_integer().
smtp_sessions() ->
    [P] = [P || {Mod, P, _, _} <- supervisor:which_children(?SERVER),
                Mod =:= 'gen_smtp_server'],
    Sessions = gen_smtp_server:sessions(P),
    length(Sessions).

%%==============================================================================
%% Supervisor callbacks
%%==============================================================================

%%------------------------------------------------------------------------------
%% @doc Whenever a supervisor is started using `supervisor:start_link/[2,3]',
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%% @end
%%------------------------------------------------------------------------------
-spec init(any()) -> kz_types:sup_init_ret().
init([]) ->
    _ = kz_process:set_startup(),
    RestartStrategy = 'one_for_one',
    MaxRestarts = 5,
    MaxSecondsBetweenRestarts = 25,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    {'ok', {SupFlags, ?CHILDREN}}.
