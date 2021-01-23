%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2021, 2600Hz
%%% @doc
%%% @author Luis Azedo
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fax_ra).
-behaviour(ra_machine).

-compile({no_auto_import, [apply/3]}).

-export([init/1
        ,apply/3
        ,state_enter/2
        ,tick/2
        ,overview/1
        ]).

-export([start/0, stop/0]).

-export([members/0, members/1
        ,id/0, id/1
        ,app_nodes/0
        ,make_ra_conf/1
        ,maybe_join/0
        ,pending_accounts/0
        ,join/0
        ,restart/0
        ,start_new/0
        ,delete/0
        ,process_account/1
        ,process_accounts/0, process_accounts/1
        ,ra_name/0, ra_name/1
        ,start_worker/3
        ,available_nodes/0
        ]).


-export([account_workers/1
        ,remove_worker/3
        ,remove_stale_worker/2

        ,force_remove_worker/2
        ,kill_worker/2

        ,worker_info/2
        ]).

-include("fax.hrl").

-type state() :: map().

-type job_id() :: kz_term:ne_binary().
-type account_id() :: kz_term:ne_binary().
-type start_worker_ret() :: kz_types:startlink_ret() | {'badrpc', any()}.

-type command() :: {'remove_job', {account_id(), job_id(), reference()}} |
                   {'restart_job', {account_id(), job_id(), OldRef::reference(), NewRef::reference()}} |
                   {'timeout', {'start_retry', {account_id(), job_id(), reference()}}} |
                   {'start_worker', {account_id(), job_id(), reference()}} |
                   {'start_worker_result', {account_id(), job_id(), reference(), node(), start_worker_ret()}} |
                   ra_machine:effect().

-define(DEFAULT_LIMITS(AccountId)
       ,kapps_account_config:get_global(AccountId, ?CONFIG_CAT, <<"max_outbound">>, 10)
       ).
-define(TICK_INTERVAL, 5 * ?MILLISECONDS_IN_SECOND).

-define(RPC_CALL_TIMEOUT, 5 * ?MILLISECONDS_IN_SECOND).
-define(START_RETRY, 10 * ?MILLISECONDS_IN_SECOND).


-spec account_workers(AccountId::kz_term:ne_binary()) ->
         {'ok', Jobs::map()} |
         {'error', 'timeout' | 'noproc' | 'not_found'}.
account_workers(AccountId) ->
    process_command({'account_workers', AccountId}).

-spec worker_info(AccountId::kz_term:ne_binary(), JobId::kz_term:ne_binary()) ->
         {'ok', Job::map()} |
         {'error', 'timeout' | 'noproc' | 'not_found'}.
worker_info(AccountId, JobId) ->
    process_command({'worker_info', {AccountId, JobId}}).

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
    process_command({'force_remove_job', {AccountId, JobId}}).

-spec kill_worker(AccountId::kz_term:ne_binary(), JobId::kz_term:ne_binary()) ->
         {'ok', Job::map()} |
         {'error', 'timeout' | 'noproc' | 'not_found'}.
kill_worker(AccountId, JobId) ->
    process_command({'kill_worker', {AccountId, JobId}}).

%% @private
-spec process_command(any()) -> any().
process_command(Cmd) ->
    process_command(members(), Cmd).

process_command([], _Cmd) ->
    {'error', 'server_unavailable'};
process_command([Server | Servers], Cmd) ->
    case ra:process_command(Server, Cmd) of
        {'timeout', _} ->
            lager:warning("timeout on server ~p when processing command ~p", [Server, Cmd]),
            %% idk why it always receive timeout althought I' getting
            %% the reply in the shell.
            %% process_command(Servers, Cmd);
            {'error', 'timeout'};
        {'error', _Error} ->
            process_command(Servers, Cmd);
        {'ok', Reply, _ServerId} ->
            Reply
    end.

-spec init(map()) -> state().
init(#{name := Name}) ->
    #{name => Name,
      accounts => #{},
      pids => #{},
      workers => #{},
      jobs => #{}
     }.

-spec apply(ra_machine:command_meta_data(), command(), state()) ->
          {state(), Reply :: term(), ra_machine:effects()} |
          {state(), Reply :: term()}.

apply(#{index := _RaftIdx}
     ,{'account_workers', AccountId}
     ,State
     ) ->
    case find_account_in_state(AccountId, State) of
        {'ok', #{jobs := Jobs}} ->
            {State, {'ok', Jobs}};
        _ ->
            {State, {'error', 'not_found'}}
    end;
apply(#{index := _RaftIdx}
     ,{'worker_info', {AccountId, JobId}}
     ,State
     ) ->
    kz_log:put_callid(JobId),
    case get_account_job_from_state(AccountId, JobId, State) of
        {'ok', {_, Job}} ->
            {State, {'ok', Job}};
        _ ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'down', Pid, 'normal'}
     ,#{pids := Pids, accounts := Accounts} = State0
     ) ->
    case maps:get(Pid, Pids, 'undefined') of
        #{account_id := AccountId, job_id := JobId} ->
            kz_log:put_callid(JobId),
            lager:debug("job ~p terminated normally", [Pid]),
            Account = maps:get(AccountId, Accounts, #{jobs => #{}}),
            Jobs = maps:without([JobId], maps:get('jobs', Account, #{})),
            State = case maps:size(Jobs) of
                        0 -> State0#{pids => maps:without([Pid], Pids)
                                    ,accounts => maps:without([AccountId], Accounts)
                                    };
                        _ -> State0#{pids => maps:without([Pid], Pids)
                                    ,accounts => maps:update(AccountId, Account#{jobs => Jobs}, Accounts)
                                    }
                    end,
            Effects = [{'demonitor', 'process', Pid}
                      ,{'release_cursor', RaftIdx, State}
                      ],
            {State, 'ok', Effects};
        _ ->
            {State0, 'ok'}
    end;

apply(#{index := RaftIdx}
     ,{'down', Pid, Error}
     ,#{pids := Pids, accounts := Accounts} = State
     ) ->
    case maps:get(Pid, Pids, 'undefined') of
        #{account_id := AccountId, job_id := JobId} ->
            case maps:get(AccountId, Accounts, 'undefined') of
                #{jobs := #{JobId := #{ref := Ref, raft := Index} = Job} = Jobs} = Account ->
                    kz_log:put_callid(JobId),
                    Entry = #{error => Error
                             ,node => node(Pid)
                             ,pid => Pid
                             ,ref => Ref
                             ,raft => Index
                             },
                    lager:debug("job ~p terminated with error, try start the worker in ~bms: ~p"
                               ,[Pid, ?START_RETRY, Error]
                               ),
                    Hst = maps:get('history', Job, #{}),
                    History = Hst#{RaftIdx => Entry},
                    NewJob = maps:without(['pid'], Job),
                    NewJobs = Jobs#{JobId => NewJob#{history => History
                                                    ,raft => RaftIdx
                                                    ,retried => maps:get('retried', Job, 0) + 1
                                                    }
                                   },
                    TimerPayload = {'start_retry', {AccountId, JobId, Ref}},
                    NewState = State#{pids => maps:without([Pid], Pids)
                                     ,accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                                     ,timer => [TimerPayload | maps:get('restart_timers', State, [])]
                                     },
                    Effects = [{'timer', TimerPayload, ?START_RETRY}],
                    {NewState, 'ok', Effects};
                _Other ->
                    {State#{pids => maps:without([Pid], Pids)}, {'error', 'not_found'}}
            end;

        _ -> {State, 'ok'}
    end;

apply(#{index := RaftIdx}
     ,{'force_remove_pid', {ForceKill, JobId, Job}}
     ,State0
     ) ->
    kz_log:put_callid(JobId),
    {NewState, Effects0} = maybe_remove_pid_from_state(ForceKill, JobId, Job, State0),
    Effects = Effects0 ++ [{'release_cursor', RaftIdx, NewState}],
    {'ok', NewState, Effects};

apply(#{index := RaftIdx}
     ,{'remove_job', {AccountId, JobId, Ref}}
     ,State0
     ) ->
    kz_log:put_callid(JobId),
    case maybe_remove_job_from_account_state(AccountId, JobId, Ref, State0) of
        {'ok', State1, Job} ->
            {NewState, Effects0} = maybe_remove_pid_from_state('false', JobId, Job, State1),
            Effects = Effects0 ++ [{'release_cursor', RaftIdx, NewState}],
            {NewState, {'ok', Job}, Effects};
        {'error', 'not_found'}=Error ->
            {State0, Error}
    end;

apply(#{index := RaftIdx}
     ,{'force_remove_job', {AccountId, JobId}}
     ,State0
     ) ->
    kz_log:put_callid(JobId),
    case maybe_remove_job_from_account_state(AccountId, JobId, State0) of
        {'ok', State1, Job} ->
            {NewState, Effects0} = maybe_remove_pid_from_state('false', JobId, Job, State1),
            Effects = Effects0 ++ [{'release_cursor', RaftIdx, NewState}],
            {NewState, {'ok', Job}, Effects};
        {'error', 'not_found'}=Error ->
            {State0, Error}
    end;

apply(#{index := RaftIdx}
     ,{'remove_stale_worker', {AccountId, JobId}}
     ,State0
     ) ->
    kz_log:put_callid(JobId),
    case maybe_remove_job_from_account_state(AccountId, JobId, State0) of
        {'ok', State1, Job} ->
            {NewState, Effects0} = maybe_remove_pid_from_state('false', JobId, Job, State1),
            Effects = Effects0 ++ [{'release_cursor', RaftIdx, NewState}],
            {NewState, {'ok', Job}, Effects};
        {'error', 'not_found'}=Error ->
            {State0, Error}
    end;

apply(#{index := RaftIdx}
     ,{'kill_worker', {AccountId, JobId}}
     ,State0
     ) ->
    kz_log:put_callid(JobId),
    case maybe_remove_job_from_account_state(AccountId, JobId, State0) of
        {'ok', State1, Job} ->
            {NewState, Effects0} = maybe_remove_pid_from_state('true', JobId, Job, State1),
            Effects = Effects0 ++ [{'release_cursor', RaftIdx, NewState}],
            {NewState, {'ok', Job}, Effects};
        {'error', 'not_found'}=Error ->
            {State0, Error}
    end;

apply(#{index := RaftIdx}
     ,{'force_remove_pid', Pid}
     ,#{pids := Pids, accounts := Accounts} = State0
     ) ->
    case maps:get(Pid, Pids, 'undefined') of
        #{account_id := AccountId, job_id := JobId} ->
            kz_log:put_callid(JobId),
            lager:debug("removing pid ~p", [Pid]),
            Account = maps:get(AccountId, Accounts, #{jobs => #{}}),
            Jobs = maps:without([JobId], maps:get('jobs', Account, #{})),
            State = case maps:size(Jobs) of
                        0 -> State0#{pids => maps:without([Pid], Pids)
                                    ,accounts => maps:without([AccountId], Accounts)
                                    };
                        _ -> State0#{pids => maps:without([Pid], Pids)
                                    ,accounts => maps:update(AccountId, Account#{jobs => Jobs}, Accounts)
                                    }
                    end,
            Effects = [{'demonitor', 'process', Pid}
                      ,{'release_cursor', RaftIdx, State}
                      ],
            {State, 'ok', Effects};
        _ -> {State0, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'restart_job', {AccountId, JobId, Ref, NewRef}}
     ,#{accounts := Accounts} = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref} = Job} = Jobs} = Account ->
            Entry = #{restarted => 'true'
                     ,raft => RaftIdx
                     },
            lager:debug("restarting the job ~s", [JobId]),
            Hst = maps:get('history', Job, #{}),
            History = Hst#{Ref => Entry},
            NewJobs = Jobs#{JobId => Job#{history => History
                                         ,ref => NewRef
                                         ,retried => maps:get('retried', Job, 0) + 1
                                         }
                           },
            %% FIXME: demonitor old pid? remove old pid from state?
            TimerPayload = {'start_retry', {AccountId, JobId, NewRef}},
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,timer => [TimerPayload | maps:get('restart_timers', State, [])]
                             },
            Effects = [{'timer', TimerPayload, ?START_RETRY}],
            {NewState, 'ok', Effects};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'start_worker_result', {AccountId, JobId, Ref, Node, {'ok', Pid}}}
     ,#{pids := Pids, accounts := Accounts} = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref} = Job} = Jobs} = Account ->
            lager:debug("started job ~s with pid ~p", [JobId, Pid]),
            NewJobs = Jobs#{JobId => Job#{pid => Pid
                                         ,node => Node
                                         }
                           },
            NewPids = Pids#{Pid => #{account_id => AccountId
                                    ,job_id => JobId
                                    ,ref => Ref
                                    ,node => Node
                                    ,index => RaftIdx
                                    }
                           },
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,pids => NewPids
                             },
            Effects = [{'monitor', 'process', Pid}],
            {NewState, 'ok', Effects};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(Meta
     ,{'start_worker_result', {AccountId, JobId, Ref, Node, {'error', {'already_started', Pid}}}}
     ,State
     ) ->
    kz_log:put_callid(JobId),
    lager:debug("pid ~p is already started", [Pid]),
    apply(Meta, {'start_worker_result', {AccountId, JobId, Ref, Node, {'ok', Pid}}}, State);

apply(#{index := RaftIdx}
     ,{'start_worker_result', {AccountId, JobId, Ref, Node, Error}}
     ,#{accounts := Accounts} = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref, raft := Index} = Job} = Jobs} = Account ->
            lager:debug("failed to start job ~s, will try to starting it again in ~bms: ~p"
                       ,[JobId, ?START_RETRY, Error]
                       ),
            Entry = #{error => Error
                     ,node => Node
                     ,raft => Index
                     ,ref => Ref
                     },
            Hst = maps:get('history', Job, #{}),
            History = Hst#{RaftIdx => Entry},
            NewJobs = Jobs#{JobId => Job#{history => History
                                         ,raft => RaftIdx
                                         ,retried => maps:get('retried', Job, 0) + 1
                                         }
                           },
            TimerPayload = {'start_retry', {AccountId, JobId, Ref}},
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,timer => [TimerPayload | maps:get('restart_timers', State, [])]
                             },
            Effects = [{'timer', TimerPayload, ?START_RETRY}],
            {NewState, 'ok', Effects};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := _RaftIdx}
     ,{'timeout', {'start_retry', {AccountId, JobId, Ref}}}
     ,#{accounts := Accounts} = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref} = Job}} ->
            lager:debug("trying to start job ~s for the ~b time", [JobId, maps:get('retried', Job, 0)]),
            Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, Ref]}],
            {State, 'ok', Effects};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'start_worker', {AccountId, JobId, Ref}}
     ,#{accounts := Accounts} = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := P}} ->
            {State, {'error', {'exists', P}}};
        #{limit := Limit, jobs := #{} = Jobs} = Account
          when Limit > map_size(Jobs) ->
            NewJobs = Jobs#{JobId => #{ref => Ref
                                      ,raft => RaftIdx
                                      }
                           },
            Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, Ref]}],
            {State#{accounts => maps:update(AccountId, Account#{jobs => NewJobs}, Accounts)}, 'ok', Effects};
        #{limit := Limit} ->
            {State#{}, {'error', {'limit', Limit}}};
        _NoAccount ->
            Account = #{jobs => #{JobId => #{ref => Ref, raft => RaftIdx}}
                       ,limit => ?DEFAULT_LIMITS(AccountId)
                       },
            Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, Ref]}],
            {State#{accounts => Accounts#{AccountId => Account}}, 'ok', Effects}
    end;

apply(_Meta, _Msg, State) ->
    lager:debug("recieved unknown command: ~p", [_Msg]),
    {State, {'error', 'unknown_command'}, []}.

%% @private
-spec maybe_account_is_done(AccountId::kz_term:ne_binary(), Account::map(), Accounts::map()) -> Accounts::map().
maybe_account_is_done(AccountId, #{jobs := Jobs}, Accounts) when map_size(Jobs) =:= 0 ->
    maps:without([AccountId], Accounts);
maybe_account_is_done(AccountId, Account, Accounts) ->
    Accounts#{AccountId => Account}.

%% @private
-spec maybe_remove_job_from_account_state(kz_term:ne_binary(), kz_term:ne_binary(), state()) ->
          {'ok', state(), Job::map()} |
          {'error', 'not_found'}.
maybe_remove_job_from_account_state(AccountId, JobId, State) ->
    case get_account_job_from_state(AccountId, JobId, State) of
        {'ok', AccountJob} ->
            remove_job_from_account_state(AccountId, JobId, AccountJob, State);
        {'error', 'not_found'} ->
            {'error', 'not_found'}
    end.

%% @private
-spec maybe_remove_job_from_account_state(kz_term:ne_binary(), kz_term:ne_binary(), reference(), state()) ->
          {'ok', state(), Job::map()} |
          {'error', 'not_found'}.
maybe_remove_job_from_account_state(AccountId, JobId, Ref, State) ->
    case get_account_job_from_state(AccountId, JobId, State) of
        {'ok', {_Account, #{ref := Ref}}=AccountJob} ->
            remove_job_from_account_state(AccountId, JobId, AccountJob, State);
        {'error', 'not_found'} ->
            {'error', 'not_found'}
    end.

%% @private
-spec remove_job_from_account_state(kz_term:ne_binary(), kz_term:ne_binary(), tuple(), state()) ->
          {'ok', state(), map()}.
remove_job_from_account_state(AccountId, JobId
                             ,{Account, Job}
                             ,#{accounts := Accounts} = State
                             ) ->
    lager:debug("removing job ~s", [JobId]),
    NewJobs = maps:without([JobId], maps:get('jobs', Account, #{})),
    NewState = State#{accounts =>
                      maybe_account_is_done(AccountId, Account#{jobs => NewJobs}, Accounts)
                     },
    {'ok', NewState, Job}.

%% @private
-spec maybe_remove_pid_from_state(boolean(), job_id(), map(), state()) ->
          {state(), ra_machine:effects()}.
maybe_remove_pid_from_state(ShouldKill, _JobId
                           ,#{pid := Pid, node := Node}
                           ,#{pids := Pids}=State
                           ) ->
    case rpc:call(Node, 'erlang', 'is_process_alive', [Pid], ?RPC_CALL_TIMEOUT) of
        'false' ->
            lager:debug("job ~p is not alive, removing it from state", [Pid]),
            {State#{pids => maps:without([Pid], Pids)}, [{'demonitor', 'process', Pid}]};
        'true' when ShouldKill ->
            KillResult = rpc:call(Node, 'supervisor', 'terminate_child', ['fax_worker_sup', Pid]),
            lager:debug("job ~p is still alive, force exiting it result in ~p", [Pid, KillResult]),
            {State, [{'demonitor', 'process', Pid}]};
        'true' ->
            lager:debug("job ~p is still alive, not removing pid", [Pid]),
            {State, []};
        {'badrpc', _} when ShouldKill ->
            KillResult = rpc:call(Node, 'supervisor', 'terminate_child', ['fax_worker_sup', Pid]),
            lager:debug("failed to query job ~p aliveness on node ~p, force exiting it result in ~p"
                        ,[Pid, Node, KillResult]
                       ),
            {State, [{'demonitor', 'process', Pid}]};
        {'badrpc', Error} ->
            lager:debug("failed to query job ~p aliveness on node ~p, not removing pid: ~p"
                        ,[Pid, Node, Error]
                       ),
            {State, []}
    end.

%% @private
-spec find_account_in_state(kz_term:ne_binary(), state()) ->
          {'ok', Account::map()} |
          {'error', 'not_found'}.
find_account_in_state(AccountId, #{accounts := Accounts}) ->
    case maps:get(AccountId, Accounts, 'undefined') of
        'undefined' -> {'error', 'not_found'};
        #{} = Account -> {'ok', Account}
    end;
find_account_in_state(_, _) ->
    {'error', 'not_found'}.

%% @private
-spec get_account_job_from_state(kz_term:ne_binary(), kz_term:ne_binary(), state()) ->
          {'ok', {Account::map(), Job::map()}} |
          {'error', 'not_found'}.
get_account_job_from_state(AccountId, JobId, State) ->
    case find_account_in_state(AccountId, State) of
        {'ok', #{jobs := #{JobId := Job}} = Account} ->
            {'ok', {Account, Job}};
        _ ->
            {'error', 'not_found'}
    end.

-spec state_enter(ra_server:ra_state(), state()) -> ra_machine:effects().
state_enter('leader', #{pids := PMap}=State) ->
    Pids = maps:keys(PMap),
    Mons = [{'monitor', 'process', P} || P <- Pids],
    NodeMons = lists:usort([{'monitor', 'node', node(P)} || P <- Pids]),
    Timers = [{'timer', TimerPayload, ?START_RETRY} || TimerPayload <- maps:get('restart_timers', State, [])],
    Effects = Mons ++ NodeMons ++ Timers,
    Effects;
state_enter(_, _) -> [].

-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, _) ->
    _ = erlang:spawn(?MODULE, 'process_accounts', []),
    [].

-spec overview(state()) -> map().
overview(#{pids := Pids
          ,accounts := Accounts
          }) ->
    #{type => ?MODULE,
      num_workers => maps:size(Pids),
      num_accounts => maps:size(Accounts),
      workers => Pids,
      accounts => Accounts
     }.

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
    AccountIds = pending_accounts(),
    process_accounts(AccountIds).

-spec process_accounts(kz_term:ne_binaries()) -> 'ok'.
process_accounts(AccountIds) ->
    lists:foreach(fun process_account/1, AccountIds).

-spec process_account(kz_term:ne_binary()) -> 'ok'.
process_account(AccountId) ->
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
    Failed = lists:foldl(fun(JobId, Acc) -> start_account_worker(AccountId, JobId, Acc) end, [], LockedJobs),
    maybe_rollback_to_pending(AccountId, Failed).

-spec start_account_worker(kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binaries()) -> kz_term:ne_binaries().
start_account_worker(AccountId, JobId, Acc) ->
    case ra:process_command(ra_name(), {'start_worker', {AccountId, JobId, make_ref()}}) of
        {'ok', {'error', Error}, _ServerId} -> %% {limit, Limit} or {'exists', P}
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
    LockedJobs = lists:foldl(fun(JobId, Acc) -> update_job_state(JobId, <<"locked">>, Acc) end, [], JobIds),
    lager:debug("locked ~b pending jobs for account ~s", [length(LockedJobs), AccountId]),
    LockedJobs.

-spec maybe_rollback_to_pending(kz_term:ne_binary(), kz_term:ne_binaries()) -> kz_term:ne_binaries().
maybe_rollback_to_pending(_, []) ->
    'ok';
maybe_rollback_to_pending(AccountId, JobIds) ->
    lager:debug("failed to ~b jobs start worker for account ~s, rolling back to pending", [length(JobIds), AccountId]),
    case lists:foldl(fun(JobId, Acc) -> update_job_state(JobId, <<"pending">>, Acc) end, [], JobIds) of
        [] -> 'ok';
        Failed ->
            lager:error("failed to rollback to pending: ~s", [kz_binary:join(Failed)])
    end.

-spec update_job_state(kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binaries()) -> kz_term:ne_binaries().
update_job_state(JobId, State, Acc) ->
    Options = [{'should_create', 'false'}
              ,{'ensure_saved', 'true'}
              ,{'update', [{<<"pvt_job_status">>, State}
                          ,{<<"pvt_modified">>, kz_time:now_s()}
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

cluster_name() -> <<"faxes">>.

-spec ra_name() -> 'fax'.
ra_name() -> 'fax'.

-spec ra_name(non_neg_integer()) -> atom().
ra_name(0) -> ra_name();
ra_name(I) -> kz_term:to_atom(io_lib:format("~s~B", [ra_name(), I]), 'true').

-spec id() -> {atom(), node()}.
id() -> {ra_name(), node()}.

-spec id(non_neg_integer()) -> {atom(), node()}.
id(0) -> {ra_name(), node()};
id(I) -> {ra_name(I), node()}.

-spec members() -> [{'fax', node()}].
members() ->
    [{ra_name(), N} || N <- [node() | app_nodes()]].

-spec members(non_neg_integer()) -> [{'fax', node()}].
members(I) ->
    [{ra_name(X), N} || N <- [node() | app_nodes()],
                        X <- lists:seq(0, I)
    ].

ra_machine() ->
    {'module', ?MODULE, ra_machine_config()}.

ra_machine_config() -> #{}.

-spec make_ra_conf({atom(), node()}) -> map().
make_ra_conf(ServerId) ->
    ClusterName = cluster_name(),
    Members = members(),
    UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
    #{cluster_name => ClusterName,
      id => ServerId,
      uid => UId,
      initial_members => Members,
      log_init_args => #{uid => UId},
      tick_timeout => ?TICK_INTERVAL,
      machine => ra_machine()
     }.

-spec app_nodes() -> [node()].
app_nodes() ->
    Nodes = nodes(),
    {Replies, Bad} = rpc:multicall(Nodes, 'kapps_controller', 'list_apps', []),
    [Node || {Node, Apps} <- lists:zip(Nodes -- Bad, Replies), lists:member(?APP, Apps)].


-spec restart() -> boolean().
restart() ->
    Name = id(),
    case ra:restart_server(Name) of
        'ok' -> 'true';
        {'error', {'already_started', _}} -> 'true';

        {'error', 'not_started'} -> 'false';
        {'error', 'name_not_registered'} -> 'false';
        Err ->
            lager:warning("recover: ra ~w could not be restarted ~w", [Name, Err]),
            'false'
    end.

-spec start() -> any().
start() ->
    ra:start(),
    case restart() of
        'false' -> maybe_join();
        _ -> 'ok'
    end.

-spec maybe_join() -> any().
maybe_join() ->
    case app_nodes() of
        [] -> start_new();
        _ -> join()
    end.

-spec join() -> 'ok' | {'error', any()}.
join() ->
    RaConf = make_ra_conf(id()),
    case ra:start_server(RaConf) of
        'ok' -> join_add_member();
        Else -> Else
    end.

join_add_member() ->
    case ra:add_member(members(), id()) of
        {'ok', _, _Leader} -> 'ok';
        Else -> Else
    end.

%% lifted from ra/src/ra.hrl
-type ra_server_id() :: atom() | {Name :: atom(), Node :: node()}.

-spec delete() ->
          {'ok', ra_server_id()} | {'error', term()}.
delete() ->
    {'ok', Members, _Leader} = ra:members(ra_name()),
    ra:delete_cluster(Members).

-spec start_new() -> 'ok' | {'error', 'cluster_not_formed'}.
start_new() ->
    RaConfs = [make_ra_conf(ServerId) || ServerId <- members()],
    case ra:start_cluster(RaConfs) of
        {'ok', _, _} -> 'ok';
        Else -> Else
    end.

-spec stop() -> 'ok' | {'error', 'nodedown'}.
stop() ->
    ra:stop_server(id()).

-spec start_worker(kz_term:ne_binary(), kz_term:ne_binary(), reference()) -> pid().
start_worker(AccountId, JobId, Ref) ->
    kz_process:spawn(fun start_worker/1 , [{AccountId, JobId, Ref}]).

start_worker({AccountId, JobId, Ref}) ->
    lager:debug("starting worker for job ~s (account id: ~s)", [JobId, AccountId]),
    Node = rand_node(),
    Reply = rpc:call(Node, 'fax_worker_sup', 'start_fax_job', [AccountId, JobId], ?RPC_CALL_TIMEOUT),
    ra:process_command(id(), {'start_worker_result', {AccountId, JobId, Ref, Node, Reply}}).

-spec available_nodes() -> [node()].
available_nodes() ->
    {'ok', Members, _Leader} = ra:members(ra_name()),
    lists:usort([Node || {_, Node} <- Members, lists:member(Node, nodes())]).

-spec rand_node() -> node().
rand_node() ->
    rand_node(available_nodes()).

-spec rand_node([node()]) -> node().
rand_node([]) -> node();
rand_node(Nodes) ->
    Size = length(Nodes),
    Selected = rand:uniform(Size),
    lists:nth(Selected, Nodes).
