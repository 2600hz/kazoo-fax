-ifndef(FAX_HRL).

%% Typical includes needed
-include_lib("kazoo_stdlib/include/kz_types.hrl").
-include_lib("kazoo_stdlib/include/kz_log.hrl").
-include_lib("kazoo_stdlib/include/kz_databases.hrl").
-include_lib("kazoo_amqp/include/kz_api_literals.hrl").

-define(APP, 'fax').
-define(APP_NAME, <<"fax">>).
-define(APP_VERSION, <<"4.0.0">>).
-define(CONFIG_CAT, ?APP_NAME).

-define(CACHE_NAME, 'fax_cache').
-define(RETRY_SAVE_ATTACHMENT_DELAY, 5 * ?MILLISECONDS_IN_SECOND).

-define(FAX_CHANNEL_DESTROY_PROPS, [<<"Ringing-Seconds">>, <<"Billing-Seconds">>
                                   ,<<"0">>, <<"Duration-Seconds">>
                                   ,<<"User-Agent">>
                                   ,<<"Hangup-Code">>, <<"Hangup-Cause">>
                                   ,{<<"Custom-Channel-Vars">>, [<<"Resource-ID">>, <<"Ecallmgr-Node">>
                                                                ,<<"Call-ID">>, <<"Fetch-ID">>
                                                                ]}
                                   ]).

-type fax_job() :: kz_json:object().
-type fax_jobs() :: [fax_job()].

-define(FAX_OUTGOING, <<"outgoing">>).
-define(FAX_INCOMING, <<"incoming">>).

-define(FAX_START, <<"start">>).
-define(FAX_ACQUIRE, <<"acquire">>).
-define(FAX_PREPARE, <<"prepare">>).
-define(FAX_ORIGINATE, <<"originate">>).
-define(FAX_NEGOTIATE, <<"negotiate">>).
-define(FAX_SEND, <<"send">>).
-define(FAX_RECEIVE, <<"receive">>).
-define(FAX_END, <<"end">>).
-define(FAX_ERROR, <<"error">>).

-define(OPENXML_MIME_PREFIX, "application/vnd.openxmlformats-officedocument.").
-define(OPENOFFICE_MIME_PREFIX, "application/vnd.oasis.opendocument.").

-define(DEFAULT_ALLOWED_CONTENT_TYPES, [<<"application/pdf">>
                                       ,<<"image/tiff">>
                                       ,kz_json:from_list([{<<"prefix">>, <<"image">>}])
                                       ,kz_json:from_list([{<<"prefix">>, <<?OPENXML_MIME_PREFIX>>}])
                                       ,kz_json:from_list([{<<"prefix">>, <<?OPENOFFICE_MIME_PREFIX>>}])
                                       ,<<"application/msword">>
                                       ,<<"application/vnd.ms-excel">>
                                       ,<<"application/vnd.ms-powerpoint">>
                                       ]).

-define(DEFAULT_DENIED_CONTENT_TYPES, [kz_json:from_list([{<<"prefix">>, <<"image/">>}])]).

-define(SMTP_MSG_MAX_SIZE
       ,kapps_config:get_integer(?CONFIG_CAT, <<"smtp_max_msg_size">>, 10 * ?BYTES_M)
       ).
-define(SMTP_EXTENSIONS, [{"SIZE", kz_term:to_list(?SMTP_MSG_MAX_SIZE)}]).
-define(SMTP_PORT, kapps_config:get_integer(?CONFIG_CAT, <<"smtp_port">>, 19025)).

-define(PORT, kapps_config:get_integer(?CONFIG_CAT, <<"port">>, 30950)).

-define(TMP_DIR
       ,kapps_config:get_binary(?CONFIG_CAT, <<"file_cache_path">>, <<"/tmp/">>)
       ).

-define(FAX_RA_NAME, fax_outbound).
-define(FAX_RA_SYSTEM, faxes).
-define(FAX_RA_SCOPE, faxes).
-define(FAX_RA_MACHINE, fax_ra).
-define(FAX_RA_TICK_INTERVAL, 20 * ?MILLISECONDS_IN_SECOND).
-define(FAX_RA_AUTO_LEAVE, kz_app_config:get_boolean(?APP, [ra, auto_leave], false)).

-define(FAX_HRL, 'true').
-endif.
