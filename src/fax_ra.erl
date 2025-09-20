%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2025, 2600Hz
%%% @doc RA (Raft) integration
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
        ,init_aux/1
        ,handle_aux/6
        ]).

-export([ra_name/0
        ,ra_node/0
        ]).

-export([start_worker/3]).

-include("fax.hrl").

-type state() :: map().

-type job_id() :: kz_term:ne_binary().
-type account_id() :: kz_term:ne_binary().
-type start_worker_ret() :: kz_types:startlink_ret() | {'badrpc', any()}.

-type command() :: {'force_remove_job', {boolean(), account_id(), job_id()}} |
                   {'force_remove_pid', pid()} |
                   {'force_remove_pid', {ForceKill::boolean(), job_id(), Job::map()}} |
                   {'remove_job', {account_id(), job_id(), reference()}} |
                   {'remove_stale_worker', {account_id(), job_id()}} |
                   {'restart_job', {account_id(), job_id(), OldRef::reference(), NewRef::reference()}} |
                   {'start_worker', {account_id(), job_id(), reference()}} |
                   {'start_worker_result', {account_id(), job_id(), reference(), node(), start_worker_ret()}}.

-define(DEFAULT_LIMITS(AccountId)
       ,kapps_account_config:get_global(AccountId, ?CONFIG_CAT, <<"max_outbound">>, 10)
       ).
-define(IS_ALIVE_RPC_CALL_TIMEOUT, 10 * ?MILLISECONDS_IN_SECOND).
-define(START_WORKER_RPC_CALL_TIMEOUT, 30 * ?MILLISECONDS_IN_SECOND).


%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec init(map()) -> state().
init(#{name := Name}) ->
    #{name => Name,
      accounts => #{},
      pids => #{},
      workers => #{},
      jobs => #{},
      retry => #{}
     }.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec apply(ra_machine:command_meta_data(), command(), state()) ->
          {state(), Reply :: term(), ra_machine:effects()} |
          {state(), Reply :: term()}.

apply(#{index := RaftIdx}
     ,{'down', Pid, 'normal'}
     ,#{pids := Pids, accounts := Accounts} = State
     ) ->
    case find_job_from_pid(Pid, Pids, Accounts) of
        {ok, {AccountId, JobId}} ->
            kz_log:put_callid(JobId),
            lager:debug("job ~p terminated normally", [Pid]),
            Account = maps:get(AccountId, Accounts, #{}),
            Jobs = maps:without([JobId], maps:get('jobs', Account, #{})),
            NewState = case maps:size(Jobs) of
                           0 -> State#{pids => maps:without([Pid], Pids)
                                      ,accounts => maps:without([AccountId], Accounts)
                                      };
                           _ -> State#{pids => maps:without([Pid], Pids)
                                      ,accounts => maps:update(AccountId, Account#{jobs => Jobs}, Accounts)
                                      }
                       end,
            Effects = [{'demonitor', 'process', Pid}
                      ,{'release_cursor', RaftIdx, State}
                      ],
            {NewState, 'ok', Effects};
        _ ->
            {State#{pids => maps:without([Pid], Pids)}, 'ok'}
    end;

apply(#{index := RaftIdx}
     ,{'down', Pid, Error}
     ,#{pids := Pids, accounts := Accounts} = State
     ) ->
    case find_job_from_pid(Pid, Pids, Accounts) of
        {ok, {AccountId, JobId}} ->
            case maps:get(AccountId, Accounts, 'undefined') of
                #{jobs := #{JobId := #{ref := Ref, raft := Index} = Job} = Jobs} = Account ->
                    kz_log:put_callid(JobId),
                    Entry = #{error => Error
                             ,node => node(Pid)
                             ,pid => Pid
                             ,ref => Ref
                             ,raft => Index
                             },
                    lager:debug("job ~p terminated with error, will retry it later: ~p", [Pid, Error]),
                    Hst = maps:get('history', Job, #{}),
                    History = Hst#{RaftIdx => Entry},
                    NewJob = maps:without(['pid'], Job),
                    NewJobs = Jobs#{JobId => NewJob#{history => History
                                                    ,raft => RaftIdx
                                                    ,retried => maps:get('retried', Job, 0) + 1
                                                    }
                                   },
                    Retry = add_job_retry(AccountId, JobId, Ref, State),
                    NewState = State#{pids => maps:without([Pid], Pids)
                                     ,accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                                     ,retry => Retry
                                     },
                    {NewState, 'ok', []};
                _Other ->
                    {State#{pids => maps:without([Pid], Pids)}, {'error', 'not_found'}}
            end;

        _ -> {State#{pids => maps:without([Pid], Pids)}, 'ok'}
    end;

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
     ,{'force_remove_job', {ForceKill, AccountId, JobId}}
     ,State0
     ) ->
    kz_log:put_callid(JobId),
    case maybe_remove_job_from_account_state(AccountId, JobId, State0) of
        {'ok', State1, Job} ->
            {NewState, Effects0} = maybe_remove_pid_from_state(ForceKill, JobId, Job, State1),
            Effects = Effects0 ++ [{'release_cursor', RaftIdx, NewState}],
            {NewState, {'ok', Job}, Effects};
        {'error', 'not_found'}=Error ->
            {State0, Error}
    end;

apply(#{index := RaftIdx}
     ,{'remove_stale_worker', {AccountId, JobId}}
     ,#{pids := Pids} = State0
     ) ->
    kz_log:put_callid(JobId),
    case get_account_job_from_state(AccountId, JobId, State0) of
        {'ok', {Account, Job}} ->
            case is_worker_alive(Job) of
                'true' ->
                    {State0, {'error', 'process_is_alive'}};
                'false' ->
                    Pid = maps:get('pid', Job, 'undefined'),
                    {'ok', State1, _} = remove_job_from_account_state(AccountId, JobId, {Account, Job}, State0),
                    Effects = [{'demonitor', 'process', Pid}
                              ,{'release_cursor', RaftIdx, State1}
                              ],
                    {State1#{pids => maps:without([Pid], Pids)}, {'ok', Job}, Effects};
                'no_pid' ->
                    {'ok', State1, _} = remove_job_from_account_state(AccountId, JobId, {Account, Job}, State0),
                    Effects = [{'release_cursor', RaftIdx, State1}],
                    {State1, {'ok', Job}, Effects};
                {'badrpc', _}=Bad ->
                    {State0, Bad}
            end;
        {'error', 'not_found'}=Error ->
            {State0, Error}
    end;

apply(#{index := RaftIdx}
     ,{'force_remove_pid', {ForceKill, JobId, Job}}
     ,State0
     ) ->
    kz_log:put_callid(JobId),
    {NewState, Effects0} = maybe_remove_pid_from_state(ForceKill, JobId, Job, State0),
    Effects = Effects0 ++ [{'release_cursor', RaftIdx, NewState}],
    {NewState, 'ok', Effects};

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
            Retry = remove_retry_from_account(AccountId, JobId, State0),
            State = case maps:size(Jobs) of
                        0 -> State0#{pids => maps:without([Pid], Pids)
                                    ,accounts => maps:without([AccountId], Accounts)
                                    ,retry => Retry
                                    };
                        _ -> State0#{pids => maps:without([Pid], Pids)
                                    ,accounts => maps:update(AccountId, Account#{jobs => Jobs}, Accounts)
                                    ,retry => Retry
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
            Retry = add_job_retry(AccountId, JobId, NewRef, State),
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,retry => Retry
                             },
            {NewState, 'ok', [{aux, restart_job}]};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'restart_job', {AccountId, JobId}}
     ,#{accounts := Accounts, pids := Pids} = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref, pid := Pid} = Job} = Jobs} = Account ->
            Entry = #{restarted => 'true'
                     ,raft => RaftIdx
                     },
            lager:debug("restarting the job ~s", [JobId]),
            Hst = maps:get('history', Job, #{}),
            History = Hst#{Ref => Entry},
            NewRef = make_ref(),
            NewJobs = Jobs#{JobId => Job#{history => History
                                         ,ref => NewRef
                                         ,retried => maps:get('retried', Job, 0) + 1
                                         }
                           },
            Retry = add_job_retry(AccountId, JobId, NewRef, State),
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,retry => Retry
                             ,pids => maps:without([Pid], Pids)
                             },
            {NewState, 'ok', [{aux, restart_job}]};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'start_worker_result', {AccountId, JobId, Ref, Node, {'ok', Pid}}}
     ,#{pids := Pids, accounts := Accounts} = State
     ) ->
    kz_log:put_callid(JobId),
    Retry = remove_retry_from_account(AccountId, JobId, State),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref} = Job} = Jobs} = Account ->
            lager:debug("started job ~s with pid ~p", [JobId, Pid]),
            NewJobs = Jobs#{JobId => Job#{pid => Pid
                                         ,node => Node
                                         ,started => kz_time:start_time()
                                         }
                           },
            NewPids = Pids#{Pid => #{account_id => AccountId
                                    ,job_id => JobId
                                    ,ref => Ref
                                    ,node => Node
                                    ,index => RaftIdx
                                    ,started => kz_time:start_time()
                                    }
                           },
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,pids => NewPids
                             ,retry => Retry
                             },
            Effects = [{'monitor', 'process', Pid}],
            {NewState, 'ok', Effects};
        _Else ->
            {State#{retry => Retry}, {'error', 'not_found'}}
    end;

apply(_Meta
     ,{'start_worker_result', {AccountId, JobId, _Ref, _Node, {'error', {'already_started', Pid}}}}
     ,State
     ) ->
    kz_log:put_callid(JobId),
    lager:debug("pid ~p is already started", [Pid]),
    State#{retry => remove_retry_from_account(AccountId, JobId, State)};

apply(#{index := RaftIdx}
     ,{'start_worker_result', {AccountId, JobId, Ref, Node, Error}}
     ,#{accounts := Accounts} = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref, raft := Index} = Job} = Jobs} = Account ->
            lager:notice("failed to start job ~s, will try it later: ~p", [JobId, Error]),
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
            Retry = add_job_retry(AccountId, JobId, Ref, State),
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,retry => Retry
                             },
            {NewState, 'ok', [{aux, start_error}]};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'start_worker', {AccountId, JobId, Ref}}
     ,#{accounts := Accounts
       } = State
     ) ->
    kz_log:put_callid(JobId),
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := Job}} ->
            {State, {'error', {'exists', Job}}};
        #{limit := Limit, jobs := #{} = Jobs} = Account
          when Limit > map_size(Jobs) ->
            NewJobs = Jobs#{JobId => #{ref => Ref
                                      ,raft => RaftIdx
                                      }
                           },
            Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, Ref]}],
            {State#{accounts => maps:update(AccountId, Account#{jobs => NewJobs}, Accounts)}, 'ok', Effects};
        #{limit := _Limit} ->
            lager:debug("account ~s hit the worker limits ~b, saving the job for retry", [AccountId, _Limit]),
            Retry = add_job_retry(AccountId, JobId, Ref, State),
            {State#{retry => Retry}, 'ok', [{aux, {retry, AccountId, JobId, Ref}}]};
        _NoAccount ->
            Account = #{jobs => #{JobId => #{ref => Ref, raft => RaftIdx}}
                       ,limit => ?DEFAULT_LIMITS(AccountId)
                       },
            Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, Ref]}],
            {State#{accounts => Accounts#{AccountId => Account}}, 'ok', Effects}
    end;

apply(_Meta, _Msg, State) ->
    lager:debug("received unknown command: ~p", [_Msg]),
    {State, {'error', 'unknown_command'}, []}.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec state_enter(ra_server:ra_state(), state()) -> ra_machine:effects().
state_enter('leader' = RAState, #{accounts := Accounts, pids := PMap}) ->
    lager:debug("state enter ~s", [RAState]),
    RunningPids = maps:keys(PMap),
    AccountPids = accounts_pids(Accounts),
    Pids = lists:usort(RunningPids ++ AccountPids),
    Mons = [{'monitor', 'process', P} || P <- Pids],
    NodeMons = lists:usort([{'monitor', 'node', node(P)} || P <- Pids, node(P) =/= node()]),
    Effects = Mons ++ NodeMons ++ [{aux, init}],
    Effects;
state_enter(RAState, _State) ->
    lager:debug("state enter ~s", [RAState]),
    [].

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, _State) ->
    _ = erlang:spawn('fax_ra_action', 'process_accounts', []),
    [].

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec init_aux(Name :: atom()) -> term().
init_aux(Name) -> #{name => Name, retries => #{}}.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec handle_aux(ra_server:ra_state(),
                 {call, From :: ra:from()} | cast,
                 Command :: term(),
                 AuxState,
                 LogState,
                 MacState :: state()) ->
          {no_reply, AuxState, LogState}
              when AuxState :: term(),
                   LogState :: ra_log:state().
handle_aux(leader, cast, tick, #{retries := Pending} = AuxState, LogState, #{accounts := Accounts, retry := Retry}) ->
    case maps:size(Retry) of
        0 ->
            {no_reply, AuxState#{retries => #{}}, LogState};
        _N ->
            #{retry := RetryMap} = retry_keys(Retry),
            #{retry := ToRetry} = process_retry(Accounts, Retry),
            ToStart = maps:without(maps:keys(Pending), ToRetry),
            StillPending = maps:with(maps:keys(RetryMap), Pending),
            NewPending = maps:merge(StillPending, ToStart),
            start_workers(ToStart),
            {no_reply, AuxState#{retries => NewPending}, LogState}
    end;
handle_aux(_RA, _Type, _Command, AuxState, LogState, _MacState) ->
    {no_reply, AuxState, LogState}.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec maybe_remove_account(AccountId::kz_term:ne_binary(), Account::map(), Accounts::map()) -> Accounts::map().
maybe_remove_account(AccountId, #{jobs := Jobs}, Accounts) when map_size(Jobs) =:= 0 ->
    maps:without([AccountId], Accounts);
maybe_remove_account(AccountId, Account, Accounts) ->
    Accounts#{AccountId => Account}.

-spec maybe_remove_job_from_account_state(kz_term:ne_binary(), kz_term:ne_binary(), state()) ->
          {'ok', state(), Job::map()} |
          {'error', 'not_found'}.
maybe_remove_job_from_account_state(AccountId, JobId, State0) ->
    State = State0#{retry => remove_retry_from_account(AccountId, JobId, State0)},
    case get_account_job_from_state(AccountId, JobId, State) of
        {'ok', AccountJob} ->
            remove_job_from_account_state(AccountId, JobId, AccountJob, State);
        {'error', 'not_found'} ->
            {'error', 'not_found'}
    end.

-spec maybe_remove_job_from_account_state(kz_term:ne_binary(), kz_term:ne_binary(), reference(), state()) ->
          {'ok', state(), Job::map()} |
          {'error', 'not_found'}.
maybe_remove_job_from_account_state(AccountId, JobId, Ref, State0) ->
    State = State0#{retry => remove_retry_from_account(AccountId, JobId, State0)},
    case get_account_job_from_state(AccountId, JobId, State) of
        {'ok', {_Account, #{ref := Ref}}=AccountJob} ->
            remove_job_from_account_state(AccountId, JobId, AccountJob, State);
        {'error', 'not_found'} ->
            {'error', 'not_found'}
    end.

-spec remove_job_from_account_state(kz_term:ne_binary(), kz_term:ne_binary(), tuple(), state()) ->
          {'ok', state(), map()}.
remove_job_from_account_state(AccountId, JobId
                             ,{Account, Job}
                             ,#{accounts := Accounts} = State0
                             ) ->
    lager:debug("removing job ~s", [JobId]),
    State = State0#{retry => remove_retry_from_account(AccountId, JobId, State0)},
    NewJobs = maps:without([JobId], maps:get('jobs', Account, #{})),
    NewState = State#{accounts =>
                          maybe_remove_account(AccountId, Account#{jobs => NewJobs}, Accounts)
                     },
    {'ok', NewState, Job}.

-spec maybe_remove_pid_from_state(boolean(), job_id(), map(), state()) ->
          {state(), ra_machine:effects()}.
maybe_remove_pid_from_state(ShouldKill, _JobId, #{pid := Pid} = Job, #{pids := Pids}=State) ->
    Node = maps:get('node', Job, node()),
    case is_worker_alive(Job) of
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
        Error ->
            lager:debug("failed to query job ~p aliveness on node ~p, not removing pid: ~p"
                       ,[Pid, Node, Error]
                       ),
            {State, []}
    end;
maybe_remove_pid_from_state(_, _, _Job, State) ->
    lager:debug("job has no pid"),
    {State, []}.

is_worker_alive(#{pid := Pid}=Job) ->
    rpc:call(maps:get('node', Job, node()), 'erlang', 'is_process_alive', [Pid], ?IS_ALIVE_RPC_CALL_TIMEOUT);
is_worker_alive(_) ->
    'no_pid'.

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

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
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

-spec process_retry(map(), map()) -> map().
process_retry(Accounts, Retry) ->
    maps:without([accounts,account_id,count,max], maps:fold(fun process_account_retry/3, #{retry => #{}, accounts => Accounts}, Retry)).

process_account_retry(AccountId, Retry, #{accounts := Accounts} = Map) ->
    case maps:get(AccountId, Accounts, undefined) of
        #{limit := Limit, jobs := #{} = Jobs} when Limit > map_size(Jobs) ->
            maps:fold(fun process_account_job_retry/3, Map#{account_id => AccountId, count => 0, max => Limit - map_size(Jobs)}, Retry);
        #{limit := _Limit, jobs := #{} = _Jobs} ->
            Map;
        _Other ->
            maps:fold(fun process_account_job_retry/3, Map#{account_id => AccountId, count => 0, max => ?DEFAULT_LIMITS(AccountId)}, Retry)
    end.

process_account_job_retry(JobId, Job, #{max := Max, count := Count, account_id := AccountId, retry := Retry} = Acc) ->
    #{retry_ref := RetryRef, start := StartTime, sleep := Sleep} = Job,
    case Count < Max
        andalso kz_time:elapsed_ms(StartTime, kz_time:start_time()) > Sleep
    of
        true ->
            Acc#{count => Count + 1, retry => Retry#{RetryRef => Job#{account_id => AccountId, job_id => JobId}}};
        false ->
            Acc
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec start_worker(kz_term:ne_binary(), kz_term:ne_binary(), reference()) -> pid().
start_worker(AccountId, JobId, Ref) ->
    kz_process:spawn(fun start_worker/1 , [{AccountId, JobId, Ref}]).

start_worker({AccountId, JobId, Ref}) ->
    lager:debug("starting worker for job ~s (account id: ~s)", [JobId, AccountId]),
    Node = rand_node(),
    Reply = erpc:call(Node, 'fax_worker_sup', 'start_fax_job', [AccountId, JobId], ?START_WORKER_RPC_CALL_TIMEOUT),
    ra:pipeline_command(ra_node(), {'start_worker_result', {AccountId, JobId, Ref, Node, Reply}}).

start_workers(ToStart) ->
    maps:fold(fun start_workers/3, ok, ToStart).

start_workers(_Ref, #{account_id := AccountId, job_id := JobId, reference := Ref}, _) ->
    kz_process:spawn(fun start_worker/1 , [{AccountId, JobId, Ref}]);
start_workers(_Ref, Map, _) ->
    lager:error("unexpected not matched => ~p", [{_Ref, Map}]).

retry_at_least() -> 30 * ?MILLISECONDS_IN_SECOND.
retry_random() -> rand:uniform(30) * ?MILLISECONDS_IN_SECOND.
retry_sleep() -> retry_at_least() + retry_random().

retry_info(Ref) ->
    #{reference => Ref, retry_ref => make_ref(), start => kz_time:start_time(), sleep => retry_sleep()}.

retry(State) ->
    maps:get(retry, State, #{}).

retry_account(AccountId, State) ->
    maps:get(AccountId, retry(State), undefined).

add_retry_to_account(AccountId, JobId, Data, State) ->
    case retry_account(AccountId, State) of
        undefined -> maps:put(AccountId, maps:put(JobId, Data, #{}), retry(State));
        Retry -> maps:put(AccountId, maps:put(JobId, Data, Retry), retry(State))
    end.

remove_retry_from_account(AccountId, JobId, State) ->
    case retry_account(AccountId, State) of
        undefined ->
            retry(State);
        Retry ->
            case maps:without([JobId], Retry) of
                Map when map_size(Map) =:= 0 ->
                    maps:without([AccountId], retry(State));
                Map ->
                    maps:put(AccountId, Map, retry(State))
            end
    end.

retry_job(AccountId, JobId, State) ->
    case retry_account(AccountId, State) of
        undefined -> undefined;
        Retry -> maps:get(JobId, Retry, undefined)
    end.

add_job_retry(AccountId, JobId, Ref, State) ->
    case retry_job(AccountId, JobId, State) of
        #{reference := Ref} ->
            lager:debug("jobId ~s in account ~s was already in retry, restarting timer for reference ~p", [JobId, AccountId, Ref]),
            add_retry_to_account(AccountId, JobId, retry_info(Ref), State);
        undefined ->
            lager:debug("starting timer for retrying jobId ~s in account ~s for reference ~p", [JobId, AccountId, Ref]),
            add_retry_to_account(AccountId, JobId, retry_info(Ref), State);
        #{reference := Other} ->
            lager:debug("jobId ~s in account ~s was already in retry but with another reference, dropping ~p, assigning ~p and refstarting timer", [JobId, AccountId, Other, Ref]),
            add_retry_to_account(AccountId, JobId, retry_info(Ref), State)
    end.

retry_keys(Retry) ->
    maps:without([account_id], maps:fold(fun account_retry_keys/3, #{retry => #{}}, Retry)).

account_retry_keys(AccountId, Retry, Map) ->
    maps:fold(fun job_retry_key/3, Map#{account_id => AccountId}, Retry).

job_retry_key(JobId, #{retry_ref := Ref} = Job, #{retry := Retry, account_id := AccountId} = Map) ->
    Map#{retry => Retry#{Ref => Job#{account_id => AccountId, job_id => JobId}}}.

accounts_pids(Accounts) ->
    maps:fold(fun account_pids/3, [], Accounts).

account_pids(_AccountId, #{jobs := Jobs}, Pids) ->
    maps:fold(fun job_pid/3, Pids, Jobs).

job_pid(_JobId, #{pid := Pid}, Pids) -> [Pid | Pids];
job_pid(_JobId, _Job, Pids) -> Pids.

find_job_from_pid(Pid, Pids, Accounts) ->
    case maps:get(Pid, Pids, undefined) of
        #{account_id := AccountId, job_id := JobId} -> {ok, {AccountId, JobId}};
        undefined -> find_job_from_pid_in_accounts(Pid, Accounts)
    end.

find_job_from_pid_in_accounts(Pid, Accounts) ->
    case maps:fold(fun find_job_from_pid_in_account/3, #{pid => Pid}, Accounts) of
        #{found := #{account_id := AccountId, job_id := JobId}} -> {ok, {AccountId, JobId}};
        _else -> {error, not_found}
         end.

find_job_from_pid_in_account(AccountId, #{jobs := Jobs}, Map) ->
    maps:fold(fun find_job_from_pid_in_account_jobs/3, Map#{account_id => AccountId}, Jobs).

find_job_from_pid_in_account_jobs(JobId, #{pid := Pid}, #{pid := Pid, account_id := AccountId} = Map) ->
    Map#{found => #{account_id => AccountId, job_id => JobId}};
find_job_from_pid_in_account_jobs(_JobId, _Job, Map) -> Map.

-spec ra_name() -> ?FAX_RA_NAME.
ra_name() -> ?FAX_RA_NAME.

-spec ra_node() -> {?FAX_RA_NAME, node()}.
ra_node() ->
    {?FAX_RA_NAME, node()}.

-spec available_nodes() -> [node()].
available_nodes() ->
    {'ok', Members, _Leader} = ra:members(ra_node()),
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
