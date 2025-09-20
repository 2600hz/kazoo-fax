%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2025, 2600Hz
%%% @doc
%%% @author Luis Azedo
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fax_ra_action).

-export([pending_accounts/0
        ,process_account/1
        ,process_accounts/0, process_accounts/1
        ]).

%% Handy commands
-export([remove_worker/3
        ,remove_stale_worker/2

        ,force_remove_worker/2
        ,kill_worker/2
        ]).

%% Handy queries
-export([list_account_ids/0
        ,list_pids/0
        ,workers_count/0

        ,account_workers/1
        ,pid_info/1
        ,worker_info/2
        ]).

-include("fax.hrl").

-define(DEFAULT_LIMITS(AccountId)
       ,kapps_account_config:get_global(AccountId, ?CONFIG_CAT, <<"max_outbound">>, 10)
       ).
-define(TICK_INTERVAL, 5 * ?MILLISECONDS_IN_SECOND).

%%%=============================================================================
%%% Command functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec remove_worker(AccountId::kz_term:ne_binary(), JobId::kz_term:ne_binary(), Reference::reference()) ->
          {'ok', Job::map()} |
          {'error', 'timeout' | 'noproc' | 'not_found'}.
remove_worker(AccountId, JobId, Reference) ->
    process_command({'remove_job', {AccountId, JobId, Reference}}).

-spec remove_stale_worker(AccountId::kz_term:ne_binary(), JobId::kz_term:ne_binary()) ->
          {'ok', Job::map()} |
          {'error', 'timeout' | 'noproc' | 'not_found'}.
remove_stale_worker(AccountId, JobId) ->
    process_command({'remove_stale_worker', {AccountId, JobId}}).

-spec force_remove_worker(AccountId::kz_term:ne_binary(), JobId::kz_term:ne_binary()) ->
          {'ok', Job::map()} |
          {'error', 'timeout' | 'noproc' | 'not_found'}.
force_remove_worker(AccountId, JobId) ->
    process_command({'force_remove_job', {'false', AccountId, JobId}}).

-spec kill_worker(AccountId::kz_term:ne_binary(), JobId::kz_term:ne_binary()) ->
          {'ok', Job::map()} |
          {'error', 'timeout' | 'noproc' | 'not_found'}.
kill_worker(AccountId, JobId) ->
    process_command({'force_remove_job', {'true', AccountId, JobId}}).

%%%=============================================================================
%%% Query functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec list_account_ids() ->
          {'ok', map()} |
          {'error', any()}.
list_account_ids() ->
    query_state(
      fun(State) ->
              Fold = fun(AccountId, Map, Acc) ->
                             Acc#{AccountId => maps:size(maps:get('jobs', Map, #{}))}
                     end,
              {'ok', maps:fold(Fold, #{}, maps:get('accounts', State, #{}))}
      end
     ).

-spec list_pids() ->
          {'ok', [pid()]} |
          {'error', any()}.
list_pids() ->
    query_state(
      fun(State) ->
              {'ok', maps:keys(maps:get('pids', State, #{}))}
      end
     ).

-spec workers_count() ->
          {'ok', Counts::kz_term:proplist()} |
          {'error', 'timeout' | 'noproc'}.
workers_count() ->
    query_state(
      fun(State) ->
              {'ok', [{'worker_counts', maps:size(maps:get('pids', State, #{}))}
                     ,{'account_counts', maps:size(maps:get('accounts', State, #{}))}
                     ]
              }
      end
     ).

-spec account_workers(AccountId::kz_term:ne_binary()) ->
          {'ok', Jobs::map()} |
          {'error', 'timeout' | 'noproc' | 'not_found'}.
account_workers(AccountId) ->
    query_state(
      fun(#{accounts := #{AccountId := #{jobs := Jobs}}}) ->
              {'ok', Jobs};
         (_) ->
              {'error', 'not_found'}
      end
     ).

-spec pid_info(pid()) ->
          {'ok', Jobs::map()} |
          {'error', 'timeout' | 'noproc' | 'not_found'}.
pid_info(Pid) ->
    query_state(
      fun(#{'pids' := Pids}) ->
              case maps:get(Pid, Pids, 'undefined') of
                  'undefined' -> {'error', 'not_found'};
                  Map -> {'ok', Map}
              end
      end
     ).

-spec worker_info(AccountId::kz_term:ne_binary(), JobId::kz_term:ne_binary()) ->
          {'ok', Job::map()} |
          {'error', 'timeout' | 'noproc' | 'not_found'}.
worker_info(AccountId, JobId) ->
    query_state(
      fun(#{accounts := #{AccountId := #{jobs := #{JobId := Job}}}}) ->
              {'ok', Job};
         (_) ->
              {'error', 'not_found'}
      end
     ).

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec pending_accounts() -> kz_term:ne_binaries().
pending_accounts() ->
    ViewOptions = ['reduce'
                  ,'group'
                  ,{'group_level', 1}
                  ],
    case kz_datamgr:get_result_keys(?KZ_FAXES_DB, <<"faxes/schedule_accounts">>, ViewOptions) of
        {'ok', AccountIds} -> AccountIds;
        {'error', _Reason} -> []
    end.

-spec process_accounts() -> 'ok'.
process_accounts() ->
    erlang:put(kz_application, fax),
    AccountIds = pending_accounts(),
    process_accounts(AccountIds).

-spec process_accounts(kz_term:ne_binaries()) -> 'ok'.
process_accounts(AccountIds) ->
    lists:foreach(fun process_account/1, AccountIds).

-spec process_account(kz_term:ne_binary()) -> 'ok'.
process_account(AccountId) ->
    kz_log:put_callid(AccountId),
    Upto = kz_time:now_s(),
    ViewOptions = [{'limit', ?DEFAULT_LIMITS(AccountId)}
                  ,{'startkey', [AccountId]}
                  ,{'endkey', [AccountId, Upto]}
                  ],
    case kz_datamgr:get_result_ids(?KZ_FAXES_DB, <<"faxes/jobs_by_account">>, ViewOptions) of
        {'ok', []} -> 'ok';
        {'ok', JobIds} ->
            start_processing_account(AccountId, JobIds);
        {'error', _Reason} -> 'ok'
    end.

-spec start_processing_account(kz_term:ne_binary(), kz_term:ne_binaries()) -> 'ok'.
start_processing_account(AccountId, JobIds) ->
    lager:debug("found ~b jobs for account ~s, locking the jobs and start workers", [length(JobIds), AccountId]),
    LockedJobs = lock_jobs(AccountId, JobIds),
    Failed = lists:foldl(fun(JobId, Acc) ->
                                 start_account_worker(AccountId, JobId, Acc)
                         end
                        ,[]
                        ,LockedJobs
                        ),
    maybe_rollback_to_pending(AccountId, Failed).

-spec start_account_worker(kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binaries()) -> kz_term:ne_binaries().
start_account_worker(AccountId, JobId, Acc) ->
    case ra:process_command(fax_ra:ra_node(), {'start_worker', {AccountId, JobId, make_ref()}}) of
        {'ok', {'error', {'exists', _Job}}, _ServerId} ->
            Acc;
        {'ok', {'error', Error}, _} ->
            lager:debug("failed to start job ~s: ~p", [JobId, Error]),
            [JobId | Acc];
        {'ok', _Ok, _ServerId} ->
            Acc;
        {'error', _Reason} ->
            lager:debug("failed to start job ~s: ~p", [JobId, _Reason]),
            [JobId | Acc];
        {'timeout', _ServerId} ->
            lager:debug("timeout to start job ~s", [JobId]),
            [JobId | Acc]
    end.

-spec lock_jobs(kz_term:ne_binary(), kz_term:ne_binaries()) -> kz_term:ne_binaries().
lock_jobs(AccountId, JobIds) ->
    LockedJobs = lists:foldl(fun(JobId, Acc) ->
                                     update_job_state(JobId, <<"locked">>, Acc)
                             end
                            ,[]
                            ,JobIds
                            ),
    lager:debug("locked ~b pending jobs for account ~s", [length(LockedJobs), AccountId]),
    LockedJobs.

-spec maybe_rollback_to_pending(kz_term:ne_binary(), kz_term:ne_binaries()) -> 'ok'.
maybe_rollback_to_pending(_, []) -> 'ok';
maybe_rollback_to_pending(AccountId, JobIds) ->
    lager:debug("failed to ~b jobs start worker for account ~s, rolling back to pending", [length(JobIds), AccountId]),
    case rollback_to_pending(JobIds) of
        [] -> 'ok';
        Failed ->
            lager:error("failed to rollback to pending: ~s", [kz_binary:join(Failed)])
    end.

rollback_to_pending(JobIds) ->
    lists:foldl(fun rollback_to_pending_fold/2, [], JobIds).

rollback_to_pending_fold(JobId, Acc) ->
    update_job_state(JobId, <<"pending">>, Acc).

%% lifted from fax_worker
-define(DEFAULT_RETRY_PERIOD, kapps_config:get_integer(?CONFIG_CAT, <<"default_retry_period">>, 300)).

-spec update_job_state(kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binaries()) -> kz_term:ne_binaries().
update_job_state(JobId, State, Acc) ->
    Options = [{'should_create', 'false'}
              ,{'ensure_saved', 'true'}
              ,{'update', [{[<<"pvt_job_status">>], State}
                          ,{[<<"pvt_modified">>], kz_time:now_s()}
                          ,{[<<"retry_after">>], ?DEFAULT_RETRY_PERIOD}
                          ]
               }
              ],
    case kz_datamgr:update_doc(?KZ_FAXES_DB, JobId, Options) of
        {'ok', _} ->
            [JobId | Acc];
        {'error', _Reason} ->
            lager:debug("failed to lock job ~s: ~p", [JobId, _Reason]),
            Acc
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec process_command(any()) -> any().
process_command(Cmd) ->
    case ra:process_command(fax_ra:ra_node(), Cmd) of
        {'timeout', _} ->
            {'error', 'timeout'};
        {'error', _}=Error ->
            Error;
        {'ok', Reply, _ServerId} ->
            Reply
    end.

-spec query_state(fun()) -> any().
query_state(Fun) ->
    case ra:local_query(fax_ra:ra_node(), Fun) of
        {'timeout', _} ->
            {'error', 'timeout'};
        {'error', _}=Error ->
            Error;
        {'ok', {_RaIdxTerm, Reply}, _ServerId} ->
            Reply
    end.
