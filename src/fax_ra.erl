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

-include("fax.hrl").

-type state() :: map().

-define(DEFAULT_LIMITS(AccountId), kapps_account_config:get_global(AccountId, ?CONFIG_CAT, <<"max_outbound">>, 10)).
-define(TICK_INTERVAL, 5 * ?MILLISECONDS_IN_SECOND).

-spec init(map()) -> state().
init(#{name := Name}) ->
    #{name => Name,
      accounts => #{},
      pids => #{},
      workers => #{},
      jobs => #{}
     }.

-spec apply(ra_machine:command_meta_data(), term(), state()) ->
          {state(), Reply :: term(), ra_machine:effects()} |
          {state(), Reply :: term()}.

apply(#{index := RaftIdx}, {'down', Pid, 'normal'}, #{pids := Pids, accounts := Accounts} = State0) ->
    case maps:get(Pid, Pids, 'undefined') of
        #{account_id := AccountId, job_id := JobId} ->
            lager:debug("job ~s (~p) for account ~s has been finished", [JobId, Pid, AccountId]),
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
        _ -> {State0, 'ok'}
    end;

apply(#{index := RaftIdx}, {'down', Pid, Error}, #{pids := Pids, accounts := Accounts} = State) ->
    case maps:get(Pid, Pids, 'undefined') of
        #{account_id := AccountId, job_id := JobId} ->
            case maps:get(AccountId, Accounts, 'undefined') of
                #{jobs := #{JobId := #{ref := Ref, raft := Index} = Job} = Jobs} = Account ->
                    Entry = #{error => Error
                             ,node => node(Pid)
                             ,pid => Pid
                             ,ref => Ref
                             ,raft => Index
                             },
                    lager:debug("job ~s (~p) for account ~s terminate with error ~p, starting the worker again"
                               ,[JobId, Pid, AccountId, Error]
                               ),
                    Hst = maps:get('history', Job, #{}),
                    History = Hst#{RaftIdx => Entry},
                    NewJob = maps:without(['pid'], Job),
                    NewJobs = Jobs#{JobId => NewJob#{history => History
                                                    ,raft => RaftIdx
                                                    }
                                   },
                    NewState = State#{pids => maps:without([Pid], Pids)
                                     ,accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}
                                     },
                    Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, Ref]}],
                    {NewState, 'ok', Effects};
                _Other -> {State, {'error', 'not_found'}}
            end;

        _ -> {State, ok}
    end;

apply(#{index := RaftIdx}
     ,{'remove_job', {AccountId, JobId, Ref}}
     ,#{accounts := Accounts} = State
     ) ->
    case Accounts of
        #{AccountId := #{jobs := #{JobId := #{ref := Ref}} = Jobs} = Account} ->
            lager:debug("removing job ~s with account ~s", [JobId, AccountId]),
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => maps:without([JobId], Jobs)}}},
            {NewState, 'ok',[{'release_cursor', RaftIdx, NewState}]};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'restart_job', {AccountId, JobId, Ref, NewRef}}
     ,#{accounts := Accounts} = State
     ) ->
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref} = Job} = Jobs} = Account ->
            Entry = #{restarted => 'true'
                     ,raft => RaftIdx
                     },
            lager:debug("restarting the job ~s for account ~s", [JobId, AccountId]),
            Hst = maps:get('history', Job, #{}),
            History = Hst#{Ref => Entry},
            NewJobs = Jobs#{JobId => Job#{history => History
                                         ,ref => NewRef
                                         }
                           },
            NewState = State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}},
            Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, NewRef]}],
            {NewState, 'ok', Effects};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'start_worker_result', {AccountId, JobId, Ref, Node, {ok, Pid}}}
     ,#{pids := Pids, accounts := Accounts} = State
     ) ->
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref} = Job} = Jobs} = Account ->
            lager:debug("started job ~s for account ~s with pid ~p", [JobId, AccountId, Pid]),
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

apply(#{index := RaftIdx}
     ,{'start_worker_result', {AccountId, JobId, Ref, Node, Error}}
     ,#{accounts := Accounts} = State
     ) ->
    case maps:get(AccountId, Accounts, 'undefined') of
        #{jobs := #{JobId := #{ref := Ref, raft := Index} = Job} = Jobs} = Account ->
            lager:debug("failed to start job ~s for account, trying to start it again: ~p", [JobId, AccountId, Error]),
            Entry = #{error => Error
                     ,node => Node
                     ,raft => Index
                     ,ref => Ref
                     },
            Hst = maps:get('history', Job, #{}),
            History = Hst#{RaftIdx => Entry},
            NewJobs = Jobs#{JobId => Job#{history => History
                                         ,raft => RaftIdx
                                         }
                           },
            Effects = [{'mod_call', ?MODULE, 'start_worker', [AccountId, JobId, Ref]}],
            {State#{accounts => Accounts#{AccountId => Account#{jobs => NewJobs}}}, 'ok', Effects};
        _Else ->
            {State, {'error', 'not_found'}}
    end;

apply(#{index := RaftIdx}
     ,{'start_worker', {AccountId, JobId, Ref}}
     ,#{accounts := Accounts} = State
     ) ->
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
    {State, 'ok'}.

-spec state_enter(ra_server:ra_state(), state()) -> ra_machine:effects().
state_enter('leader', #{pids := PMap}) ->
    Pids = maps:keys(PMap),
    Mons = [{'monitor', 'process', P} || P <- Pids],
    NodeMons = lists:usort([{'monitor', 'node', node(P)} || P <- Pids]),
    Effects = Mons ++ NodeMons,
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
process_accounts([]) -> 'ok';
process_accounts(AccountIds) ->
    _ = [process_account(AccountId) || AccountId <- AccountIds],
    'ok'.

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

-spec ra_name() -> atom().
ra_name() -> 'fax'.

-spec ra_name(non_neg_integer()) -> atom().
ra_name(0) -> ra_name();
ra_name(I) -> kz_term:to_atom(io_lib:format("~s~B", [ra_name(), I]), 'true').

-spec id() -> tuple().
id() -> {ra_name(), node()}.

-spec id(non_neg_integer()) -> tuple().
id(0) -> {ra_name(), node()};
id(I) -> {ra_name(I), node()}.

-spec members() -> any().
members() ->
    [{ra_name(), N} || N <- [node() | app_nodes()]].

-spec members(non_neg_integer()) -> any().
members(I) ->
    [{ra_name(X), N} || N <- [node() | app_nodes()],
                        X <- lists:seq(0, I)
    ].

ra_machine() ->
    {'module', ?MODULE, ra_machine_config()}.

ra_machine_config() -> #{}.

-spec make_ra_conf(any()) -> any().
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


-spec restart() -> any().
restart() ->
    Name = id(),
    case ra:restart_server(Name) of
        'ok' ->
            'ok';
        {'error', Err1}
          when Err1 == 'not_started'
               orelse Err1 == 'name_not_registered' ->
            'false';

        {'error', {'already_started', _}} ->
            'ok';
        Err ->
            lager:warning("recover: ra ~w could not be restarted ~w", [Name, Err])
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

-spec join() -> any().
join() ->
    RaConf = make_ra_conf(id()),
    case ra:start_server(RaConf) of
        'ok' ->
            case ra:add_member(members(), id()) of
                {'ok', _, _Leader} -> 'ok';
                Else -> Else
            end;
        Else -> Else
    end.

-spec delete() -> any().
delete() ->
    {'ok', Members, _Leader} = ra:members(ra_name()),
    ra:delete_cluster(Members).

-spec start_new() -> any().
start_new() ->
    RaConfs = [make_ra_conf(ServerId) || ServerId <- members()],
    case ra:start_cluster(RaConfs) of
        {'ok', _, _} -> 'ok';
        Else -> Else
    end.

-spec stop() -> any().
stop() ->
    ra:stop_server(id()).

-spec start_worker(kz_term:ne_binary(), kz_term:ne_binary(), reference()) -> pid().
start_worker(AccountId, JobId, Ref) ->
    kz_process:spawn(fun start_worker/1 , [{AccountId, JobId, Ref}]).

start_worker({AccountId, JobId, Ref}) ->
    lager:debug("starting worker for job ~s (account id: ~s)", [JobId, AccountId]),
    Node = rand_node(),
    Reply = rpc:call(Node, 'fax_worker_sup', 'start_fax_job', [AccountId, JobId]),
    ra:process_command(id(), {'start_worker_result', {AccountId, JobId, Ref, Node, Reply}}).

-spec available_nodes() -> list().
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
