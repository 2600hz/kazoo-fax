%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2025, 2600Hz
%%% @doc Worker to pull jobs
%%% @author Karl Anderson
%%% @author Luis Azedo
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fax_worker).
-behaviour(gen_listener).

-export([start_link/2]).

-export([handle_tx_resp/2
        ,handle_job_status_query/2
        ,handle_fax_event/2
        ]).
-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,handle_event/2
        ,terminate/2
        ,code_change/3
        ]).

-include("fax.hrl").

-define(MOVE_RETRY_INTERVAL, 5 * ?MILLISECONDS_IN_SECOND).
-define(MAX_MOVE_RETRY, 5).
-define(MAX_MOVE_NOTIFY_MSG
       ,"failed to move fax outbound document ~s from faxes database to account ~s modb"
       ).

-record(state, {queue_name :: kz_term:api_binary()
               ,job_id :: kz_term:ne_binary()
               ,job :: kz_term:api_object()
               ,account_id :: kz_term:api_binary()
               ,status :: kz_term:api_ne_binary()
               ,fax_status :: kz_term:api_object()
               ,pages :: kz_term:api_integer()
               ,page = 0 :: integer()
               ,file :: kz_term:api_ne_binary()
               ,callid :: kz_term:ne_binary()
               ,stage :: kz_term:api_binary()
               ,resp :: kz_term:api_object()
               ,move_retry = 0 :: integer()
               ,error :: kz_term:api_ne_binary()
               ,call = kapps_call:new() :: kapps_call:call()
               ,call_id :: kz_term:api_ne_binary()
               }).
-type state() :: #state{}.

-type release_ret() :: {'ok', kz_json:object(), kz_json:object()} | kz_datamgr:data_error().

-define(ORIGINATE_TIMEOUT, ?MILLISECONDS_IN_MINUTE * 2).
-define(NEGOTIATE_TIMEOUT, ?MILLISECONDS_IN_MINUTE * 2).
-define(PAGE_TIMEOUT, kapps_config:get_integer(?CONFIG_CAT, <<"default_page_timeout_ms">>, ?MILLISECONDS_IN_MINUTE * 10)).

-define(BINDINGS(JobId), [{'self', []}
                         ,{'fax', [{'restrict_to', ['query_status']}]}
                         ]).

-define(RESPONDERS, [{{?MODULE, 'handle_tx_resp'}
                     ,[{<<"resource">>, <<"offnet_resp">>}]
                     }
                    ,{{?MODULE, 'handle_job_status_query'}
                     ,[{<<"fax">>, <<"query_status">>}]
                     }
                    ,{{?MODULE, 'handle_fax_event'}
                     ,[{<<"call_event">>, <<"CHANNEL_FAX_STATUS">>}]
                     }
                    ]).
-define(QUEUE_NAME, <<>>).
-define(QUEUE_OPTIONS, []).
-define(CONSUME_OPTIONS, []).

-define(DEFAULT_RETRY_PERIOD, kapps_config:get_integer(?CONFIG_CAT, <<"default_retry_period">>, 300)).
-define(DEFAULT_RETRY_COUNT, kapps_config:get_integer(?CONFIG_CAT, <<"default_retry_count">>, 3)).
-define(DEFAULT_COMPARE_FIELD, kapps_config:get_binary(?CONFIG_CAT, <<"default_compare_field">>, <<"result_cause">>)).

-define(CALLFLOW_LIST, <<"callflows/listing_by_number">>).
-define(ENSURE_CID_KEY, <<"ensure_valid_caller_id">>).
-define(DEFAULT_ENSURE_CID, kapps_config:get_is_true(?CONFIG_CAT, ?ENSURE_CID_KEY, 'true')).

-define(NOTIFICATION_OUTBOUND_EMAIL, [<<"notifications">>
                                     ,<<"outbound">>
                                     ,<<"email">>
                                     ,<<"send_to">>
                                     ]
       ).
-define(NOTIFICATION_EMAIL, [<<"notifications">>
                            ,<<"email">>
                            ,<<"send_to">>
                            ]
       ).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Starts the server.
%% @end
%%------------------------------------------------------------------------------
-spec start_link(kz_term:ne_binary(), kz_term:ne_binary()) -> kz_types:startlink_ret().
start_link(AccountId, JobId) ->
    gen_listener:start_link(?MODULE
                           ,[{'bindings', ?BINDINGS(JobId)}
                            ,{'responders', ?RESPONDERS}
                            ,{'queue_name', ?QUEUE_NAME}
                            ,{'queue_options', ?QUEUE_OPTIONS}
                            ,{'consume_options', ?CONSUME_OPTIONS}
                            ]
                           ,[AccountId, JobId]
                           ).

-spec handle_tx_resp(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_tx_resp(JObj, Props) ->
    Srv = props:get_value('server', Props),
    gen_server:cast(Srv, {'tx_resp', kz_api:msg_id(JObj), JObj}).

-spec handle_fax_event(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_fax_event(JObj, Props) ->
    Srv = props:get_value('server', Props),
    JobId = props:get_value('job_id', Props),
    Event = kz_call_event:application_event(JObj),
    gen_server:cast(Srv, {'fax_status', Event, JobId, JObj}).

-spec handle_job_status_query(kz_json:object(), kz_term:proplist()) -> 'ok'.
handle_job_status_query(JObj, Props) ->
    'true' = kapi_fax:query_status_v(JObj),
    Srv = props:get_value('server', Props),
    JobId = kapi_fax:job_id(JObj),
    Queue = kz_api:server_id(JObj),
    MsgId = kz_api:msg_id(JObj),
    gen_server:cast(Srv, {'query_status', JobId, Queue, MsgId, JObj}).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Initializes the server.
%% @end
%%------------------------------------------------------------------------------
-spec init([kz_term:ne_binary() | kz_term:ne_binary()]) -> {'ok', state()}.
init([AccountId, JobId]) ->
    kz_log:put_callid(JobId),
    {'ok', #state{callid = JobId
                 ,job_id = JobId
                 ,account_id = AccountId
                 ,stage = ?FAX_ACQUIRE
                 }
    }.

%%------------------------------------------------------------------------------
%% @doc Handling call messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_call(any(), kz_term:pid_ref(), state()) -> kz_types:handle_call_ret_state(state()).
handle_call(_Request, _From, State) ->
    {'reply', {'error', 'not_implemented'}, State}.

%%------------------------------------------------------------------------------
%% @doc Handling cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_cast(any(), state()) -> kz_types:handle_cast_ret_state(state()).
handle_cast({'tx_resp', JobId, JObj}, #state{job_id=JobId
                                            ,job=Job
                                            ,resp='undefined'
                                            }=State) ->
    case kz_call_event:response_message(JObj) of
        <<"SUCCESS">> ->
            Call = kapps_call:from_json(kz_json:get_json_value(<<"Call">>, JObj)),
            CallId = kapps_call:call_id_direct(Call),
            kz_events:bind_call_id(CallId),
            lager:debug("received successful attempt to originate fax, continue processing"),
            send_status(State, <<"received successful attempt to originate fax">>),
            {'noreply'
            ,State#state{stage = ?FAX_NEGOTIATE
                        ,status = <<"negotiating">>
                        ,call = Call
                        ,call_id = CallId
                        }
            ,?NEGOTIATE_TIMEOUT
            };
        _Else ->
            lager:debug("received failed attempt to tx fax, releasing job: ~s", [_Else]),
            send_error_status(State, kz_call_event:error_message(JObj)),
            {'noreply', release(State, release_failed_job('tx_resp', JObj, Job))}
    end;
handle_cast({'tx_resp', JobId2, _}, #state{job_id=JobId}=State) ->
    lager:debug("received txresp for ~s but this JobId is ~s", [JobId2, JobId]),
    {'noreply', State};
handle_cast({'fax_status', <<"negociateresult">>, JobId, JObj}, State) ->
    Data = kz_call_event:application_data(JObj),
    TransferRate = kz_json:get_integer_value(<<"Fax-Transfer-Rate">>, Data, 1),
    lager:debug("fax status - negotiate result - ~s : ~p",[JobId, TransferRate]),
    Status = list_to_binary(["Fax negotiated at ", kz_term:to_list(TransferRate)]),
    send_status(State, Status, Data),
    {'noreply', State#state{status=Status
                           ,fax_status=Data
                           ,stage = ?FAX_SEND
                           }, ?PAGE_TIMEOUT
    };
handle_cast({'fax_status', <<"pageresult">>, JobId, JObj}
           ,#state{pages=Pages}=State
           ) ->
    Data = kz_call_event:application_data(JObj),
    Page = kz_json:get_integer_value(<<"Fax-Transferred-Pages">>, Data, 0),
    lager:debug("fax status - page result - ~s : ~p : ~p"
               ,[JobId, Page, kz_time:now_s()]
               ),
    Status = list_to_binary(["Sent Page ", kz_term:to_list(Page), " of ", kz_term:to_list(Pages)]),
    send_status(State#state{page=Page}, Status, Data),
    {'noreply', State#state{page=Page
                           ,status=Status
                           ,fax_status=Data
                           }, ?PAGE_TIMEOUT
    };
handle_cast({'fax_status', <<"result">>, JobId, JObj}, #state{job_id=JobId}=State) ->
    {'noreply', handle_fax_result(JObj, State)};
handle_cast({'fax_status', Event, JobId, _JObj}, State) ->
    lager:debug_unsafe("fax status ~s - ~s event not handled => ~s",[JobId, Event, kz_json:encode(_JObj)]),
    {'noreply', State};
handle_cast({'query_status', JobId, Queue, MsgId, _JObj}
           ,#state{status=Status
                  ,job_id=JobId
                  ,account_id=AccountId
                  ,fax_status=Data
                  }=State
           ) ->
    lager:debug("query fax status ~s handled by this queue",[JobId]),
    send_reply_status(Queue, MsgId, JobId, Status, AccountId, Data),
    {'noreply', State};
handle_cast({'query_status', JobId, Queue, MsgId, _JObj}, State) ->
    lager:debug("query fax status ~s not handled by this queue",[JobId]),
    Status = list_to_binary(["Fax ", JobId, " not being processed by this Queue"]),
    send_reply_status(Queue, MsgId, JobId, Status, <<"*">>,'undefined'),
    {'noreply', State};
handle_cast('attempt_transmission', #state{job_id = JobId}=State) ->
    case kz_datamgr:open_doc(?KZ_FAXES_DB, JobId) of
        {'ok', JObj} ->
            lager:debug("acquired job ~s", [JobId]),
            Status = <<"preparing">>,
            NewState = State#state{job = JObj
                                  ,status = Status
                                  ,page = 0
                                  ,fax_status = 'undefined'
                                  ,stage = ?FAX_PREPARE
                                  },
            send_status(NewState, <<"job acquired">>, ?FAX_START, 'undefined'),
            gen_server:cast(self(), 'send'),
            {'noreply', NewState};
        {'error', Reason} ->
            lager:debug("failed to acquire job ~s: ~p", [JobId, Reason]),
            gen_server:cast(self(), 'error'),
            {'noreply', State#state{error=kz_term:to_binary(Reason)}}
    end;
handle_cast('send', #state{job_id=JobId
                          ,job=JObj
                          ,queue_name=Q
                          ,callid=CallId
                          }=State) ->
    send_status(State, <<"ready to send">>, ?FAX_SEND, 'undefined'),
    send_fax(JobId, kz_json:set_value(<<"Call-ID">>, CallId, JObj), Q),
    {'noreply', State#state{stage=?FAX_ORIGINATE}, ?ORIGINATE_TIMEOUT};
handle_cast({'error', 'invalid_number', Number}, #state{job=JObj
                                                       }=State) ->
    lager:debug("destination number ~s invalid in sending fax", [Number]),
    send_error_status(State, <<"invalid fax number">>),
    {'noreply', release(State, release_failed_job('invalid_number', Number, JObj))};
handle_cast({'error', 'invalid_cid', Number}, #state{job=JObj
                                                    }=State) ->
    lager:debug("CIDNum ~s invalid in sending fax", [Number]),
    send_error_status(State, <<"invalid fax cid number">>),
    {'noreply', release(State, release_failed_job('invalid_cid', Number, JObj))};
handle_cast({'error', 'notify', Message}, #state{job=JObj
                                                ,job_id=JobId
                                                }=State) ->
    Props = kz_json:to_proplist(JObj),
    kz_notify:detailed_alert(<<"error in fax job ~s : ~s">>, [JobId, Message], Props),
    {'noreply', release(State, release_failed_job('notify', Message, JObj))};
handle_cast({'gen_listener', {'created_queue', QueueName}}, State) ->
    lager:debug("fax worker discovered queue name ~s", [QueueName]),
    gen_server:cast(self(), 'attempt_transmission'),
    {'noreply', State#state{queue_name=QueueName}};
handle_cast({'gen_listener',{'is_consuming',_IsConsuming}}, State) ->
    lager:debug("fax worker is consuming : ~p", [_IsConsuming]),
    {'noreply', State};
handle_cast('error', State) ->
    {'stop', 'normal', State};
handle_cast('stop', State) ->
    gen_listener:cast(self(), 'move_doc'),
    {'noreply', State};
handle_cast('move_doc', #state{account_id=AccountId
                              ,job_id=JobId
                              ,job=JObj
                              ,move_retry=?MAX_MOVE_RETRY
                              } = State) ->
    Props = kz_json:to_proplist(JObj),
    kz_notify:detailed_alert(?MAX_MOVE_NOTIFY_MSG, [JobId, AccountId], Props),
    gen_listener:cast(self(), 'notify'),
    {'noreply', State};
handle_cast('move_doc', #state{job=JObj, move_retry=Tries} = State) ->
    case maybe_move_doc(JObj, kzd_fax:job_status(JObj)) of
        {'ok', Doc} ->
            gen_listener:cast(self(), 'notify'),
            {'noreply', State#state{job=Doc}};
        {'error', Error} ->
            lager:error("error moving fax doc to modb : ~p", [Error]),
            timer:sleep(?MOVE_RETRY_INTERVAL),
            gen_listener:cast(self(), 'move_doc'),
            {'noreply', State#state{move_retry=Tries + 1}}
    end;
handle_cast('notify', #state{job=JObj, resp=Resp} = State) ->
    maybe_notify(JObj, Resp, kzd_fax:job_status(JObj)),
    {'stop', 'normal', State};
handle_cast(_Msg, State) ->
    lager:debug("unhandled cast: ~p", [_Msg]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Handling all non call/cast messages.
%% @end
%%------------------------------------------------------------------------------
-spec handle_info(any(), state()) -> kz_types:handle_info_ret_state(state()).
handle_info('timeout', #state{stage='undefined'}=State) ->
    {'noreply', State};
handle_info('timeout', #state{stage=Stage, job=JObj}=State) ->
    lager:debug("timeout waiting in stage ~s", [Stage]),
    {'noreply', release(State, release_failed_job('job_timeout', Stage, JObj))};
handle_info(_Info, State) ->
    lager:debug("fax worker unhandled message: ~p", [_Info]),
    {'noreply', State}.

%%------------------------------------------------------------------------------
%% @doc Allows listener to pass options to handlers.
%% @end
%%------------------------------------------------------------------------------
-spec handle_event(kz_json:object(), state()) -> gen_listener:handle_event_return().
handle_event(JObj, #state{job_id=JobId}) ->
    case kz_json:get_first_defined([[<<"Resource-Response">>, <<"Call-ID">>], <<"Call-ID">>], JObj) of
        'undefined' -> kz_log:put_callid(JobId);
        CallId -> kz_log:put_callid(<<JobId/binary, "|", CallId/binary>>)
    end,
    {'reply', [{job_id, JobId}]}.

%%------------------------------------------------------------------------------
%% @doc This function is called by a `gen_server' when it is about to
%% terminate. It should be the opposite of `Module:init/1' and do any
%% necessary cleaning up. When it returns, the `gen_server' terminates
%% with Reason. The return value is ignored.
%%
%% @end
%%------------------------------------------------------------------------------
-spec terminate(any(), state()) -> 'ok'.
terminate('normal' = _Reason, #state{error=Error, stage=Stage})
  when Error =/= 'undefined' ->
    lager:debug("fax worker ~p terminating on stage ~s with reason : ~p", [self(), Stage, _Reason]);
terminate('normal' = _Reason, #state{stage=Stage}) ->
    lager:debug("fax worker ~p terminated on stage ~s with reason : ~p", [self(), Stage, _Reason]);
terminate(_Reason, #state{job=JObj, stage=Stage}) ->
    _ = release_failed_job('uncontrolled_termination', 'undefined', kzd_faxes:set_retries(JObj, 0)),
    lager:debug("fax worker ~p terminated on stage ~s with reason : ~p", [self(), Stage, _Reason]).

%%------------------------------------------------------------------------------
%% @doc Convert process state when code is changed.
%% @end
%%------------------------------------------------------------------------------
-spec code_change(any(), state(), any()) -> {'ok', state()}.
code_change(_OldVsn, State, _Extra) ->
    {'ok', State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
-spec release_failed_job(atom(), any(), kz_json:object()) -> release_ret().
release_failed_job('tx_resp', Resp, JObj) ->
    Msg = kz_json:get_first_defined([<<"Error-Message">>, <<"Response-Message">>], Resp),
    <<"sip:", Code/binary>> = kz_json:get_value(<<"Response-Code">>, Resp, <<"sip:500">>),
    Result = [{<<"success">>, 'false'}
             ,{<<"result_code">>, kz_term:to_integer(Code)}
             ,{<<"result_text">>, Msg}
             ,{<<"pages_sent">>, 0}
             ,{<<"time_elapsed">>, elapsed_time(JObj)}
             ],
    KVs = [{[<<"Application-Data">>, <<"Fax-Result-Text">>], Msg}],
    release_job(Result, JObj, kz_json:set_values(KVs, Resp));
release_failed_job('invalid_number', Number, JObj) ->
    Msg = kz_term:to_binary(io_lib:format("invalid fax number: ~s", [Number])),
    Result = [{<<"success">>, 'false'}
             ,{<<"result_code">>, 400}
             ,{<<"result_text">>, Msg}
             ,{<<"pages_sent">>, 0}
             ,{<<"time_elapsed">>, elapsed_time(JObj)}
             ],
    release_job(Result, JObj);
release_failed_job('invalid_cid', Number, JObj) ->
    Msg = kz_term:to_binary(io_lib:format("invalid fax cid number: ~s", [Number])),
    Result = [{<<"success">>, 'false'}
             ,{<<"result_code">>, 400}
             ,{<<"result_text">>, Msg}
             ,{<<"pages_sent">>, 0}
             ,{<<"time_elapsed">>, elapsed_time(JObj)}
             ],
    release_job(Result, JObj);
release_failed_job('notify', Message, JObj) ->
    Msg = kz_term:to_binary(io_lib:format("system error : ~s", [Message])),
    Result = [{<<"success">>, 'false'}
             ,{<<"result_code">>, 400}
             ,{<<"result_text">>, Msg}
             ,{<<"pages_sent">>, 0}
             ,{<<"time_elapsed">>, elapsed_time(JObj)}
             ],
    release_job(Result, JObj);
release_failed_job('uncontrolled_termination', _, JObj) ->
    Msg = <<"process terminated. please contact your support for details">>,
    Result = [{<<"success">>, 'false'}
             ,{<<"result_code">>, 500}
             ,{<<"result_text">>, Msg}
             ,{<<"pages_sent">>, 0}
             ,{<<"time_elapsed">>, elapsed_time(JObj)}
             ],
    release_job(Result, JObj);
release_failed_job('fax_result', Resp, JObj) ->
    ApplicationData = kz_json:get_json_value(<<"Application-Data">>, Resp),
    Result = props:filter_undefined(
               [{<<"time_elapsed">>, elapsed_time(JObj)}
               | fax_util:fax_properties(ApplicationData)
               ]),
    release_job(Result, JObj, Resp);
release_failed_job('job_timeout', 'undefined', JObj) ->
    release_failed_job('job_timeout', <<"undefined">>, JObj);
release_failed_job('job_timeout', Reason, JObj) ->
    Result = [{<<"success">>, 'false'}
             ,{<<"result_code">>, 500}
             ,{<<"result_text">>, <<"fax job timed out - ", Reason/binary>>}
             ,{<<"pages_sent">>, 0}
             ,{<<"time_elapsed">>, elapsed_time(JObj)}
             ,{<<"fax_bad_rows">>, 0}
             ,{<<"fax_speed">>, 0}
             ,{<<"fax_receiver_id">>, <<>>}
             ,{<<"fax_error_correction">>, 'false'}
             ],
    release_job(Result, JObj).

-spec release_successful_job(kz_json:object(), kz_json:object()) -> release_ret().
release_successful_job(Resp, JObj) ->
    <<"sip:", Code/binary>> = kz_json:get_value(<<"Hangup-Code">>, Resp, <<"sip:200">>),
    Result = props:filter_undefined(
               [{<<"time_elapsed">>, elapsed_time(JObj)}
               ,{<<"result_code">>, kz_term:to_integer(Code)}
               ,{<<"result_cause">>, kz_json:get_value(<<"Hangup-Cause">>, Resp)}
               ,{<<"pvt_delivered_date">>,
                 case kz_json:is_true([<<"Application-Data">>, <<"Fax-Success">>], Resp) of
                     'true' -> kz_time:now_s();
                     'false' -> 'undefined'
                 end
                }
               | fax_util:fax_properties(kz_json:get_json_value(<<"Application-Data">>, Resp, Resp))
               ]),
    release_job(Result, JObj, Resp).

-spec release_job(kz_term:proplist(), kz_json:object()) -> release_ret().
release_job(Result, JObj) ->
    release_job(Result, JObj, kz_json:new()).

-spec release_job(kz_term:proplist(), kz_json:object(), kz_json:object()) -> release_ret().
release_job(Result, JObj, Resp) ->
    Success = props:is_true(<<"success">>, Result, 'false'),
    Updaters = [fun increment_attempts/2
               ,fun(_Fax, Acc) -> [{<<"tx_result">>, kz_json:from_list(Result)} | Acc] end
               ,fun remove_pvt_queue/2
               ,fun apply_reschedule_logic/2
               ,fun(Fax, Acc) -> update_job_status(Fax, Acc, Success) end
               ],
    Updates = lists:foldl(fun(F, Acc) -> F(JObj, Acc) end, [], Updaters),
    Update = [{'update', Updates}
             ,{'ensure_saved', 'true'}
             ,{'should_create', 'false'}
             ],
    case kz_datamgr:update_doc(?KZ_FAXES_DB, kz_doc:id(JObj), Update) of
        {'ok', Saved} -> {'ok', Resp, Saved};
        Error -> Error
    end.

-spec update_job_status(kz_json:object(), kz_json:flat_proplist(), boolean()) ->
          kz_json:flat_proplist().
update_job_status(_JObj, Updates, 'true') ->
    lager:debug("releasing job with status: completed"),
    [{<<"pvt_job_status">>, <<"completed">>} | Updates];
update_job_status(JObj, Updates, 'false') ->
    Attempts = props:get_integer_value(<<"attempts">>, Updates),
    Retries = kz_json:get_integer_value(<<"retries">>, JObj, 1),
    lager:debug("unsuccessful job has attempted ~b of ~b retries", [Attempts, Retries]),
    case Retries - Attempts >= 1 of
        'true' ->
            lager:debug("releasing job with status: pending"),
            [{<<"pvt_job_status">>, <<"pending">>} | Updates];
        'false' ->
            lager:info("releasing job with status: failed"),
            [{<<"pvt_job_status">>, <<"failed">>} | Updates]
    end.

-spec increment_attempts(kz_json:object(), kz_json:flat_proplist()) ->
          kz_json:flat_proplist().
increment_attempts(JObj, Updates) ->
    Attempts = kz_json:get_integer_value(<<"attempts">>, JObj, 0),
    [{<<"attempts">>, Attempts + 1} | Updates].

-spec remove_pvt_queue(kz_json:object(), kz_json:flat_proplist()) ->
          kz_json:flat_proplist().
remove_pvt_queue(_JObj, Updates) ->
    [{<<"pvt_queue">>, 'null'} | Updates].

-spec apply_reschedule_logic(kz_json:object(), kz_json:flat_proplist()) ->
          kz_json:flat_proplist().
apply_reschedule_logic(JObj, Updates) ->
    Map = kapps_config:get_json(?CONFIG_CAT, <<"reschedule">>, kz_json:new()),
    case apply_reschedule_rules(kz_json:get_values(Map), set_default_update_fields(JObj)) of
        {'no_rules', []} ->
            lager:debug("no rules applied in fax reschedule logic"),
            Updates;
        {'ok', Us} ->
            lager:debug("rule '~s' applied in fax reschedule logic"
                       ,[kz_json:get_value(<<"reschedule_rule">>, JObj)]
                       ),
            Us ++ Updates
    end.

-spec apply_reschedule_rules({kz_json:objects(), kz_json:path()}, kz_json:object()) ->
          {'ok', kz_json:flat_proplist()} |
          {'no_rules', kz_json:flat_proplist()}.
apply_reschedule_rules({[], _}, _JObj) -> {'no_rules', []};
apply_reschedule_rules({[Rule | Rules], [Key | Keys]}, JObj) ->
    Attempts = kz_json:get_integer_value(<<"attempts">>, JObj, 0),
    Result = kz_json:get_value(<<"tx_result">>, JObj, kz_json:new()),
    Field = kz_json:get_value(<<"compare-field">>, Rule, ?DEFAULT_COMPARE_FIELD),
    ValueList = kz_json:get_value(<<"compare-values">>, Rule, []),
    ResultValue = kz_json:get_value(Field, Result),
    Attempt = get_attempt_value(kz_json:get_value(<<"attempt">>, Rule)),
    RetryAfter = kz_json:get_integer_value(<<"retry-after">>, Rule, ?DEFAULT_RETRY_PERIOD),
    Retries = kz_json:get_integer_value(<<"retries">>, Rule, ?DEFAULT_RETRY_COUNT),
    NewRetries = kz_json:get_integer_value(<<"new-retry-count">>, Rule, Retries),
    case (Attempt =:= Attempts
          orelse Attempt =:= -1
         )
        andalso lists:member(ResultValue, ValueList)
    of
        'true' ->
            {'ok', [{<<"retry_after">>, RetryAfter}
                   ,{<<"retries">>, NewRetries}
                   ,{<<"reschedule_rule">>, Key}
                   ]
            };
        'false' ->
            apply_reschedule_rules({Rules, Keys}, JObj)
    end.

-spec get_attempt_value(kz_term:api_binary() | integer()) -> integer().
get_attempt_value(X) when is_integer(X) -> X;
get_attempt_value('undefined') -> -1;
get_attempt_value(<<"any">>) -> -1;
get_attempt_value(X) -> kz_term:to_integer(X).

-spec set_default_update_fields(kz_json:object()) -> kz_json:object().
set_default_update_fields(JObj) ->
    kz_json:set_values([{<<"pvt_modified">>, kz_time:now_s()}
                       ,{<<"retry_after">>, ?DEFAULT_RETRY_PERIOD}
                       ]
                      ,JObj
                      ).

-spec maybe_notify(kz_json:object(), kz_json:object(), kz_term:ne_binary()) -> any().
maybe_notify(JObj, Resp, <<"completed">>) ->
    Message = notify_fields(JObj, Resp),
    kapps_notify_publisher:cast(Message, fun kapi_notifications:publish_fax_outbound/1);
maybe_notify(JObj, Resp, <<"failed">>) ->
    Message = props:filter_undefined(
                [{<<"Fax-Error">>, fax_error(kz_json:merge_jobjs(JObj, Resp))}
                | notify_fields(JObj, Resp)
                ]),
    kapps_notify_publisher:cast(Message, fun kapi_notifications:publish_fax_outbound_error/1);
maybe_notify(_JObj, _Resp, Status) ->
    lager:debug("notify status ~p not handled", [Status]).

-spec maybe_move_doc(kz_json:object(), kz_term:ne_binary()) ->
          {'ok', kz_json:object()} |
          kz_datamgr:data_error().
maybe_move_doc(JObj, <<"completed">>) ->
    try_move_doc(JObj);
maybe_move_doc(JObj, <<"failed">>) ->
    try_move_doc(JObj);
maybe_move_doc(JObj, _) ->
    {'ok', JObj}.

-spec try_move_doc(kz_json:object()) ->
          {'ok', kz_json:object()} |
          kz_datamgr:data_error().
try_move_doc(JObj) ->
    try
        move_doc(JObj)
    catch
        What:Why:ST ->
            kz_log:log_stacktrace(ST),
            {error, {'EXIT', {What, Why}}}
    end.

-spec move_doc(kz_json:object()) ->
          {'ok', kz_json:object()} |
          kz_datamgr:data_error().
move_doc(JObj) ->
    FromId = kz_doc:id(JObj),
    {Year, Month, _D} = kz_term:to_date(kz_doc:created(JObj)),
    FromDB = kz_doc:account_db(JObj),
    AccountId = kz_doc:account_id(JObj),
    AccountMODb = kazoo_modb:get_modb(AccountId, Year, Month),
    ToDB = kzs_util:format_account_modb(AccountMODb, 'encoded'),
    ToId = ?MATCH_MODB_PREFIX(kz_term:to_binary(Year), kz_date:pad_month(Month), FromId),
    Options = ['override_existing_document'
              ,{'transform', fun move_to_outbox/2}
              ],
    lager:debug("moving fax outbound document ~s from faxes to ~s with id ~s", [FromId, AccountMODb, ToId]),
    case kazoo_modb:move_doc(FromDB, {<<"fax">>, FromId}, ToDB, ToId, Options) of
        {'ok', _}=OK -> OK;
        {'error', 'conflict'} ->
            handle_move_conflict(JObj, FromDB, FromId, ToDB, ToId);
        {'error', _}=Error -> Error
    end.

-spec handle_move_conflict(kz_json:object(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary()) ->
          {'ok', kz_json:object()} |
          kz_datamgr:data_error().
handle_move_conflict(SourceJObj, FromDB, FromId, ToDB, ToId) ->
    lager:info("moving ~s to ~s/~s conflicted", [FromId, ToDB, ToId]),
    case kz_datamgr:open_cache_doc(ToDB, ToId) of
        {'ok', MovedDoc} ->
            handle_if_moved_successfully(SourceJObj, FromDB, FromId, ToDB, ToId, MovedDoc);
        {'error', _E}=Error ->
            lager:debug("failed to open moved doc: ~p", [_E]),
            Error
    end.

-spec handle_if_moved_successfully(kz_json:object(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_json:object()) ->
          {'ok', kz_json:object()} |
          {'error', 'conflict'}.
handle_if_moved_successfully(SourceJObj, FromDB, FromId, ToDB, ToId, MovedDoc) ->
    case kz_doc:are_equal(SourceJObj, MovedDoc) of
        'true' ->
            _Deleted = kz_datamgr:del_doc(FromDB, FromId),
            lager:debug("deleted from doc: ~p", [_Deleted]),
            {'ok', MovedDoc};
        'false' ->
            lager:info("docs don't match enough, removing destination and retrying"),
            _ = kz_datamgr:del_doc(ToDB, ToId),
            {'error', 'conflict'}
    end.

-spec move_to_outbox(kz_json:object(), kz_json:object()) -> kz_json:object().
move_to_outbox(_SourceJObj, DestJObj) ->
    kz_json:set_value(<<"folder">>, <<"outbox">>, DestJObj).

-spec fax_error(kz_json:object()) -> kz_term:api_binary().
fax_error(JObj) ->
    kz_json:get_first_defined([[<<"Application-Data">>, <<"Fax-Result-Text">>]
                              ,[<<"tx_result">>, <<"result_text">>]
                              ]
                             ,JObj
                             ).

-spec notify_emails(kz_json:object()) -> kz_term:ne_binaries().
notify_emails(JObj) ->
    Emails = kz_json:get_first_defined([?NOTIFICATION_OUTBOUND_EMAIL
                                       ,?NOTIFICATION_EMAIL
                                       ]
                                      ,JObj
                                      ,[]
                                      ),
    fax_util:notify_email_list(Emails).

-spec fax_hangup_code(kz_json:object()) -> kz_term:api_integer().
fax_hangup_code(JObj) ->
    case kz_json:get_ne_binary_value(<<"Hangup-Code">>, JObj) of
        <<"sip:", Code/binary>> -> kz_term:to_integer(Code);
        _Else -> undefined
    end.

-spec fax_hangup_cause(kz_json:object()) -> kz_term:api_ne_binary().
fax_hangup_cause(JObj) ->
    kz_json:get_ne_binary_value(<<"Hangup-Cause">>, JObj).

-spec notify_fields(kz_json:object(), kz_json:object()) -> kz_term:proplist().
notify_fields(JObj, Resp) ->
    FaxFields = [{<<"Fax-Hangup-Code">>, fax_hangup_code(Resp)}
                ,{<<"Fax-Hangup-Cause">>, fax_hangup_cause(Resp)}
                | fax_fields(kz_json:get_value(<<"Application-Data">>, Resp))
                ],

    ToNumber = kz_term:to_binary(kz_json:get_value(<<"to_number">>, JObj)),
    ToName = kz_term:to_binary(kz_json:get_value(<<"to_name">>, JObj, ToNumber)),
    Notify = [E || E <- notify_emails(JObj), not kz_term:is_empty(E)],

    props:filter_empty(
      [{<<"Caller-ID-Name">>, kz_json:get_value(<<"from_name">>, JObj)}
      ,{<<"Caller-ID-Number">>, kz_json:get_value(<<"from_number">>, JObj)}
      ,{<<"Callee-ID-Number">>, ToNumber}
      ,{<<"Callee-ID-Name">>, ToName }
      ,{<<"Account-ID">>, kz_doc:account_id(JObj)}
      ,{<<"Account-DB">>, kz_doc:account_db(JObj)}
      ,{<<"Fax-JobId">>, kz_doc:id(JObj)}
      ,{<<"Fax-ID">>, kz_doc:id(JObj)}
      ,{<<"FaxBox-ID">>, kz_json:get_value(<<"faxbox_id">>, JObj)}
      ,{<<"Fax-Notifications">>
       ,kz_json:from_list([{<<"email">>, kz_json:from_list([{<<"send_to">>, Notify}])}])
       }
      ,{<<"Fax-Info">>, kz_json:from_list(FaxFields) }
      ,{<<"Call-ID">>, kz_json:get_value(<<"Call-ID">>, Resp)}
      ,{<<"Fax-Timestamp">>, kz_time:now_s()}
      ,{<<"Fax-Timezone">>, kz_json:get_value(<<"fax_timezone">>, JObj)}
      | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
      ]).

-spec fax_fields(kz_term:api_object()) -> kz_term:proplist().
fax_fields('undefined') -> [];
fax_fields(JObj) ->
    [{K,V} || {<<"Fax-", _/binary>> = K, V} <- kz_json:to_proplist(JObj)].

-spec elapsed_time(kz_json:object()) -> non_neg_integer().
elapsed_time(JObj) ->
    Now = kz_time:now_s(),
    Created = kz_doc:created(JObj, Now),
    Now - Created.

-type validate_fax_error() :: {'error', 'invalid_cid', kz_term:ne_binary()}
                            | {'error', 'invalid_number', kz_term:ne_binary()}
                            | 'error'.
-type validate_fax_result() :: 'true' | validate_fax_error().

-spec validate_to_number(kz_json:object()) -> validate_fax_result().
validate_to_number(FaxDoc) ->
    case get_did(FaxDoc) of
        'undefined' -> {'error', 'invalid_number', <<"(undefined)">>};
        _Else -> 'true'
    end.

-spec validate_fax_status(kz_json:object()) -> validate_fax_result().
validate_fax_status(FaxDoc) ->
    validate_fax_job_status(kzd_fax:job_status(FaxDoc)).

-spec validate_fax_job_status(kz_term:api_ne_binary()) -> validate_fax_result().
validate_fax_job_status('undefined') ->
    {'error', 'notify', <<"job status is undefined">>};
validate_fax_job_status(<<"failed">>) ->
    {'error', 'notify', <<"job already in failed status">>};
validate_fax_job_status(_Else) -> 'true'.

-spec should_validate_caller_id(kz_json:object()) -> boolean().
should_validate_caller_id(JObj) ->
    kz_json:is_true(?ENSURE_CID_KEY, JObj, ?DEFAULT_ENSURE_CID).

-spec validate_caller_id(kz_json:object()) -> validate_fax_result().
validate_caller_id(JObj) ->
    validate_caller_id(JObj, should_validate_caller_id(JObj)).

-spec validate_caller_id(kz_json:object(), boolean()) -> validate_fax_result().
validate_caller_id(_JObj, 'false') -> 'true';
validate_caller_id(JObj, 'true') -> ensure_valid_caller_id(JObj).

-spec ensure_valid_caller_id(kz_json:object()) -> validate_fax_result().
ensure_valid_caller_id(JObj) ->
    ensure_valid_caller_id(kzd_fax:from_number(JObj), kz_doc:account_id(JObj)).

-spec ensure_valid_caller_id(kz_term:api_ne_binary(), kz_term:api_ne_binary()) -> validate_fax_result().
ensure_valid_caller_id(_CIDNumber, 'undefined') ->
    {'error', 'notify', <<"fax document without account id">>};
ensure_valid_caller_id('undefined', _AccountId) ->
    {'error', 'invalid_cid', <<"(undefined)">>};
ensure_valid_caller_id(CIDNumber, AccountId) ->
    case fax_util:is_valid_caller_id(CIDNumber, AccountId) of
        'true' -> 'true';
        'false' -> {'error', 'invalid_cid', CIDNumber}
    end.

-spec validate_fax(kz_json:object()) -> validate_fax_result().
validate_fax(JObj) ->
    Routines =[fun validate_fax_status/1
              ,fun validate_caller_id/1
              ,fun validate_to_number/1
              ],
    validate_fax(JObj, Routines).

-spec validate_fax(kz_json:object(), list()) -> validate_fax_result().
validate_fax(_JObj, []) -> 'true';
validate_fax(JObj, [Fun | Funs]) ->
    case Fun(JObj) of
        'true' -> validate_fax(JObj, Funs);
        Msg -> Msg
    end.

-spec send_fax(kz_term:ne_binary(), kz_json:object(), kz_term:ne_binary()) -> 'ok'.
send_fax(JobId, JObj, Q) ->
    case validate_fax(JObj) of
        'true' -> send_fax(JobId, JObj, Q, get_did(JObj));
        Msg -> gen_server:cast(self(), Msg)
    end.

-spec send_fax(kz_term:ne_binary(), kz_json:object(), kz_term:ne_binary(), kz_term:api_ne_binary()) -> 'ok'.
send_fax(_JobId, _JObj, _Q, 'undefined') ->
    gen_server:cast(self(), {'error', 'invalid_number', <<"(undefined)">>});
send_fax(JobId, JObj, Q, ToDID) ->
    IgnoreEarlyMedia = 'true',
    ToNumber = kz_term:to_binary(kz_json:get_value(<<"to_number">>, JObj)),
    ToName = kz_term:to_binary(kz_json:get_value(<<"to_name">>, JObj, ToNumber)),
    CallId = kz_json:get_value(<<"Call-ID">>, JObj),
    ETimeout = kz_term:to_binary(kapps_config:get_integer(?CONFIG_CAT, <<"endpoint_timeout">>, 40)),
    AccountId =  kz_doc:account_id(JObj),
    AccountRealm = kzd_accounts:fetch_realm(AccountId),
    Request = props:filter_undefined(
                [{<<"Outbound-Caller-ID-Name">>, kz_json:get_value(<<"from_name">>, JObj)}
                ,{<<"Outbound-Caller-ID-Number">>, kz_json:get_value(<<"from_number">>, JObj)}
                ,{<<"Outbound-Callee-ID-Number">>, ToNumber}
                ,{<<"Outbound-Callee-ID-Name">>, ToName }
                ,{<<"Account-ID">>, AccountId}
                ,{<<"Account-Realm">>, AccountRealm}
                ,{<<"To-DID">>, ToDID}
                ,{<<"Fax-Identity-Number">>, kz_json:get_value(<<"fax_identity_number">>, JObj)}
                ,{<<"Fax-Identity-Name">>, kz_json:get_value(<<"fax_identity_name">>, JObj)}
                ,{<<"Fax-Timezone">>, kzd_fax_box:timezone(JObj)}
                ,{<<"Flags">>, get_flags(JObj)}
                ,{<<"Resource-Type">>, <<"originate">>}
                ,{<<"Hunt-Account-ID">>, get_hunt_account_id(AccountId)}
                ,{<<"Msg-ID">>, JobId}
                ,{<<"Ignore-Early-Media">>, IgnoreEarlyMedia}
                ,{<<"Custom-Channel-Vars">>, resource_ccvs(JobId)}
                ,{<<"Custom-SIP-Headers">>, kz_json:get_value(<<"custom_sip_headers">>, JObj)}
                ,{<<"Export-Custom-Channel-Vars">>, [<<"Account-ID">>]}
                ,{<<"Application-Name">>, <<"fax">>}
                ,{<<"Timeout">>,ETimeout}
                ,{<<"Application-Data">>, get_proxy_url(JobId)}
                ,{<<"Bypass-E164">>, kz_json:is_true(<<"bypass_e164">>, JObj)}
                ,{<<"Fax-T38-Enabled">>, 'false'}
                | kz_api:default_headers(Q, ?APP_NAME, ?APP_VERSION)
                ]),
    lager:debug("sending fax originate request ~s with call-id ~s", [JobId, CallId]),
    kapi_offnet_resource:publish_req(Request).

-spec get_flags(kz_json:object()) -> kz_term:ne_binaries().
get_flags(JObj) ->
    Flags = [<<"fax">> | kz_json:get_ne_binaries(<<"flags">>, JObj, [])],
    AccountId =  kz_doc:account_id(JObj),
    Funs = [{fun kapps_call:set_account_id/2, AccountId}
           ,{fun kapps_call:set_account_db/2, kzs_util:format_account_db(AccountId)}
           ],
    lists:uniq(Flags ++ kz_attributes:get_flags(?APP_NAME, kapps_call:exec(Funs, kapps_call:new()))).

-spec get_hunt_account_id(kz_term:ne_binary()) -> kz_term:api_binary().
get_hunt_account_id(AccountId) ->
    AccountDb = kzs_util:format_account_db(AccountId),
    Options = [{'key', <<"no_match">>}, 'include_docs'],
    case kz_datamgr:get_results(AccountDb, ?CALLFLOW_LIST, Options) of
        {'ok', [JObj]} -> maybe_hunt_account_id(kz_json:get_value([<<"doc">>, <<"flow">>], JObj), AccountId);
        _ -> default_hunt_account_id(AccountId)
    end.

-spec maybe_hunt_account_id(kz_term:api_object(), kz_term:ne_binary()) -> kz_term:api_binary().
maybe_hunt_account_id('undefined', AccountId) -> default_hunt_account_id(AccountId);
maybe_hunt_account_id(JObj, AccountId) ->
    case kz_json:get_value(<<"module">>, JObj) of
        <<"resources">> ->
            kz_json:get_value([<<"data">>, <<"hunt_account_id">>], JObj, default_hunt_account_id(AccountId));
        _ ->
            maybe_hunt_account_id(kz_json:get_value([<<"children">>, <<"_">>], JObj), AccountId)
    end.

-spec default_hunt_account_id(kz_term:ne_binary()) -> kz_term:api_ne_binary().
default_hunt_account_id(AccountId) ->
    case kapps_call:should_use_local_resources(AccountId) of
        'false' -> 'undefined';
        'true' -> kapps_call:hunt_account_id(AccountId)
    end.

-spec resource_ccvs(kz_term:ne_binary()) -> kz_json:object().
resource_ccvs(JobId) ->
    kz_json:from_list([{<<"Authorizing-ID">>, JobId}
                      ,{<<"Authorizing-Type">>, <<"outbound_fax">>}
                      ,{<<"RTCP-MUX">>, 'false'}
                      ]).

-spec get_did(kz_json:object()) -> kz_term:api_ne_binary().
get_did(JObj) ->
    get_did(kzd_fax:to_number(JObj), bypass_e164(JObj)).

-spec get_did(kz_term:api_ne_binary(), boolean()) -> kz_term:api_ne_binary().
get_did('undefined', _) -> 'undefined';
get_did(Number, 'true') -> Number;
get_did(Number, 'false') -> knm_converters:normalize(Number).

-spec bypass_e164(kz_json:object()) -> boolean().
bypass_e164(JObj) ->
    kz_json:is_true(<<"bypass_e164">>, JObj, 'false').

-spec get_proxy_url(kz_term:ne_binary()) -> kz_term:ne_binary().
get_proxy_url(JobId) ->
    get_proxy_url(JobId, kz_app_config:get_boolean(?APP, [<<"file">>, <<"use_service">>], true)).

get_proxy_url(JobId, true) -> service_proxy_url(JobId);
get_proxy_url(JobId, false) -> host_proxy_url(JobId).

host_proxy_url(JobId) ->
    Hostname = kz_network_utils:get_hostname(),
    Port = integer_to_binary(?PORT),
    list_to_binary(["http://", Hostname, ":", Port, "/fax/", JobId, ".tiff"]).

service_proxy_url(JobId) ->
    service_proxy_url(JobId, kz_app_config:get_json(?APP, [<<"file">>, <<"service">>])).

service_proxy_url(JobId, undefined) ->
    host_proxy_url(JobId);
service_proxy_url(JobId, ServiceJSON) ->
    ServiceProto = kz_json:get_ne_binary_value(<<"proto">>, ServiceJSON, <<"http">>),
    ServiceHost = kz_json:get_ne_binary_value(<<"hostname">>, ServiceJSON, <<"fax-service">>),
    ServicePort = kz_json:get_integer_value(<<"port">>, ServiceJSON),
    ServicePortUrlPart = proto_port_to_url_part(ServiceProto, ServicePort),
    list_to_binary([ServiceProto, "://", ServiceHost, ServicePortUrlPart, "/fax/", JobId, ".tiff"]).

proto_port_to_url_part(<<"http">>, 80) -> <<>>;
proto_port_to_url_part(<<"https">>, 443) -> <<>>;
proto_port_to_url_part(_, undefined) -> <<>>;
proto_port_to_url_part(_, Port) -> list_to_binary([":", kz_term:to_binary(Port)]).

-spec send_status(state(), kz_term:ne_binary()) -> any().
send_status(State, Status) ->
    send_status(State, Status, ?FAX_SEND, 'undefined').

-spec send_error_status(state(), kz_term:ne_binary()) -> any().
send_error_status(State, Status) ->
    send_status(State, Status, ?FAX_ERROR, 'undefined').

-spec send_status(state(), kz_term:ne_binary(), kz_term:api_object()) -> any().
send_status(State, Status, FaxInfo) ->
    send_status(State, Status, ?FAX_SEND, FaxInfo).

-spec send_status(state(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:api_object()) -> any().
send_status(#state{job=JObj
                  ,page=Page
                  ,job_id=JobId
                  ,account_id=AccountId
                  }
           ,Status, FaxState, FaxInfo) ->
    FaxboxId = kz_json:get_value(<<"faxbox_id">>, JObj),
    CloudJobId = kz_json:get_value(<<"cloud_job_id">>, JObj),
    CloudPrinterId = kz_json:get_value(<<"cloud_printer_id">>, JObj),
    Payload = props:filter_undefined(
                [{<<"Job-ID">>, JobId}
                ,{<<"FaxBox-ID">>, FaxboxId}
                ,{<<"Account-ID">>, AccountId}
                ,{<<"Cloud-Job-ID">>, CloudJobId}
                ,{<<"Cloud-Printer-ID">>, CloudPrinterId}
                ,{<<"Status">>, Status}
                ,{<<"Fax-State">>, FaxState}
                ,{<<"Fax-Info">>, FaxInfo}
                ,{<<"Direction">>, ?FAX_OUTGOING}
                ,{<<"Page">>, Page}
                | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
                ]),
    kapi_fax:publish_status(Payload).

-spec send_reply_status(kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:ne_binary(), kz_term:api_object()) -> 'ok'.
send_reply_status(Q, MsgId, JobId, Status, AccountId, JObj) ->
    Payload = props:filter_undefined(
                [{<<"Job-ID">>, JobId}
                ,{<<"Status">>, Status}
                ,{<<"Msg-ID">>, MsgId}
                ,{<<"Account-ID">>, AccountId}
                ,{<<"Fax-Info">>, JObj}
                | kz_api:default_headers(?APP_NAME, ?APP_VERSION)
                ]),
    kapi_fax:publish_targeted_status(Q, Payload).

fax_success(JObj) ->
    kz_json:is_true([<<"Fax-Success">>], kz_call_event:application_data(JObj)).

fax_data(JObj) ->
    kz_call_event:application_data(JObj).

fax_jobid(#state{job_id = JobId}) -> JobId.

fax_job(#state{job = Job}) -> Job.

handle_fax_result(JObj, #state{file=Filepath}=State) ->
    catch(file:delete(Filepath)),
    handle_fax_result(fax_success(JObj), JObj, State).

handle_fax_result('true', JObj, State) ->
    lager:debug("fax status - successfully transmitted fax ~s", [fax_jobid(State)]),
    send_status(State, <<"Fax Successfully sent">>, ?FAX_END, fax_data(JObj)),
    release(State, release_successful_job(JObj, fax_job(State)));
handle_fax_result('false', JObj, State) ->
    lager:debug("fax status - error transmitting fax ~s", [fax_jobid(State)]),
    send_status(State, <<"Error sending fax">>, ?FAX_ERROR, fax_data(JObj)),
    release(State, release_failed_job('fax_result', JObj, fax_job(State))).

release(State, {'ok', Resp, Doc}) ->
    gen_server:cast(self(), 'stop'),
    State#state{resp = Resp, job = Doc};
release(State, {'error', _}) ->
    %% leaving the gen_server:cast here as we may want to differentiate the msg
    gen_server:cast(self(), 'stop'),
    State.
