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

-export([start_worker/3]).

%% Internal
-export([process_start_retry/1]).

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
                   {'start_worker_result', {account_id(), job_id(), reference(), node(), start_worker_ret()}} |
                   ra_machine:effect().

-define(DEFAULT_LIMITS(AccountId)
       ,kapps_account_config:get_global(AccountId, ?CONFIG_CAT, <<"max_outbound">>, 10)
       ).
-define(RPC_CALL_TIMEOUT, 5 * ?MILLISECONDS_IN_SECOND).

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
      jobs => #{}
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
                    lager:debug("job ~p terminated with error, will retry it later: ~p", [Pid, Error]),
                    Hst = maps:get('history', Job, #{}),
                    History = Hst#{RaftIdx => Entry},
                    NewJob = maps:without(['pid'], Job),
                    NewJobs = Jobs#{JobId => NewJob#{history => History
                                                    ,raft => RaftIdx
                                                    ,retried => maps:get('retried', Job, 0) + 1
                                                    }
                                   },
                    NewState = State#{pids => maps:without([Pid], Pids)
                                     ,accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                                     ,start_retry => maps:put({AccountId, JobId, Ref}
                                                             ,'true'
                                                             ,maps:get('start_retry', State, #{})
                                                             )
                                     },
                    {NewState, 'ok', []};
                _Other ->
                    {State#{pids => maps:without([Pid], Pids)}, {'error', 'not_found'}}
            end;

        _ -> {State, 'ok'}
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
                    {'ok', State1, _} =
                        remove_job_from_account_state(AccountId, JobId, {Account, Job}, State0),
                    Effects = [{'demonitor', 'process', Pid}
                              ,{'release_cursor', RaftIdx, State1}
                              ],
                    {State1#{pids => maps:without([Pid], Pids)}, {'ok', Job}, Effects};
                'no_pid' ->
                    {'ok', State1, _} =
                        remove_job_from_account_state(AccountId, JobId, {Account, Job}, State0),
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
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,start_retry => maps:put({AccountId, JobId, NewRef}
                                                     ,'true'
                                                     ,maps:get('start_retry', State, #{})
                                                     )
                             },
            {NewState, 'ok', []};
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
                             ,start_retry => maps:without([{AccountId, JobId, Ref}]
                                                         ,maps:get('start_retry', State, #{})
                                                         )
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
            lager:debug("failed to start job ~s, will try it later: ~p", [JobId, Error]),
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
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                             ,start_retry => maps:put({AccountId, JobId, Ref}
                                                     ,'true'
                                                     ,maps:get('start_retry', State, #{})
                                                     )
                             },
            {NewState, 'ok', []};
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
    lager:debug("received unknown command: ~p", [_Msg]),
    {State, {'error', 'unknown_command'}, []}.

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec maybe_account_is_done(AccountId::kz_term:ne_binary(), Account::map(), Accounts::map()) -> Accounts::map().
maybe_account_is_done(AccountId, #{jobs := Jobs}, Accounts) when map_size(Jobs) =:= 0 ->
    maps:without([AccountId], Accounts);
maybe_account_is_done(AccountId, Account, Accounts) ->
    Accounts#{AccountId => Account}.

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
    rpc:call(maps:get('node', Job, node()), 'erlang', 'is_process_alive', [Pid], ?RPC_CALL_TIMEOUT);
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
-spec state_enter(ra_server:ra_state(), state()) -> ra_machine:effects().
state_enter('leader', #{pids := PMap}) ->
    Pids = maps:keys(PMap),
    Mons = [{'monitor', 'process', P} || P <- Pids],
    NodeMons = lists:usort([{'monitor', 'node', node(P)} || P <- Pids]),
    Effects = Mons ++ NodeMons,
    Effects;
state_enter(_, _) -> [].

-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, State) ->
    _ = erlang:spawn('fax_ra_action', 'process_accounts', []),
    _ = erlang:spawn(?MODULE, 'process_start_retry', [maps:get('start_retry', State, #{})]),
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

-spec process_start_retry(map()) -> 'ok'.
process_start_retry(Map) ->
    maps:fold(fun({AccountId, JobId, Ref}, _, _) ->
                      _ = start_worker(AccountId, JobId, Ref),
                      'ok'
              end
             ,'ok'
             ,Map
             ).

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec start_worker(kz_term:ne_binary(), kz_term:ne_binary(), reference()) -> pid().
start_worker(AccountId, JobId, Ref) ->
    kz_process:spawn(fun start_worker/1 , [{AccountId, JobId, Ref}]).

start_worker({AccountId, JobId, Ref}) ->
    lager:debug("starting worker for job ~s (account id: ~s)", [JobId, AccountId]),
    Node = fax_ra_node:rand_node(),
    Reply = rpc:call(Node, 'fax_worker_sup', 'start_fax_job', [AccountId, JobId], ?RPC_CALL_TIMEOUT),
    ra:process_command(fax_ra_node:id(), {'start_worker_result', {AccountId, JobId, Ref, Node, Reply}}).
