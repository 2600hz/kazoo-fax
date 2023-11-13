%%%-----------------------------------------------------------------------------
%%% @copyright (C) 2012-2023, 2600Hz
%%% @doc
%%% @author Karl Anderson
%%%
%%% This Source Code Form is subject to the terms of the Mozilla Public
%%% License, v. 2.0. If a copy of the MPL was not distributed with this
%%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fax_file_proxy).

-export([init/2]).
-export([terminate/3]).

-include("fax.hrl").

-spec init(cowboy_req:req(), any()) ->
          {'ok', cowboy_req:req(), 'ok'}.
init(Req, _Opts) ->
    kz_log:put_callid(kz_binary:rand_hex(16)),
    send_job_file(Req, cowboy_req:path_info(Req)).

send_job_file(Req, [FileId]) ->
    JobId = kz_term:to_binary(filename:basename(FileId, filename:extension(FileId))),
    lager:debug("fetching fax job ~s/~s", [JobId, FileId]),
    {'ok', get_job_file(Req, JobId), 'ok'};
send_job_file(Req, _Else) ->
    lager:debug("not sending job file contents for ~p", [_Else]),
    {'ok', cowboy_req:reply(404, Req), 'ok'}.

get_job_file(Req, JobId) ->
    get_job_file(Req, JobId, kz_datamgr:open_doc(?KZ_FAXES_DB, JobId)).

get_job_file(Req, JobId, {'error', _Reason}) ->
    lager:debug("could not open document '~s' in faxes db : ~p", [JobId, _Reason]),
    cowboy_req:reply(404, Req);
get_job_file(Req, JobId, {'ok', JObj}) ->
    get_attachment(Req, JobId, kz_fax_attachment:fetch_faxable(?KZ_FAXES_DB, JObj)).

get_attachment(Req, JobId, {'ok', Content, ContentType, _Doc}) ->
    lager:debug("sending fax file ~s : ~b bytes", [JobId, size(Content)]),
    cowboy_req:reply(200, #{<<"content-type">> => ContentType}, Content, Req);
get_attachment(Req, JobId, _Error) ->
    lager:debug("could not get fax attachment for document '~s' in faxes db : ~p", [JobId, _Error]),
    cowboy_req:reply(404, Req).

-spec terminate(any(), cowboy_req:req(), any()) -> 'ok'.
terminate(_Reason, _Req, _State) -> 'ok'.
