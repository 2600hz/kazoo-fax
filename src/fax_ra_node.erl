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
-module(fax_ra_node).

-export([start/0, stop/0]).

-export([members/0, members/1
        ,id/0, id/1
        ,app_nodes/0
        ,make_ra_conf/1
        ,maybe_join/0
        ,join/0
        ,restart/0
        ,start_new/0
        ,delete/0
        ,ra_name/0, ra_name/1
        ,available_nodes/0
        ]).

-export([rand_node/0]).

-include("fax.hrl").

-define(TICK_INTERVAL, 20 * ?MILLISECONDS_IN_SECOND).

%%------------------------------------------------------------------------------
%% @doc
%% @end
%%------------------------------------------------------------------------------
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
    {'module', 'fax_ra', ra_machine_config()}.

ra_machine_config() -> #{}.

-spec make_ra_conf({atom(), node()}) -> map().
make_ra_conf(ServerId) ->
    ClusterName = cluster_name(),
    Members = members(),
    UId = ra:new_uid(ra_lib:to_binary(ClusterName)),
    #{cluster_name => ClusterName
     ,id => ServerId
     ,uid => UId
     ,initial_members => Members
     ,log_init_args => #{uid => UId}
     ,tick_timeout => ?TICK_INTERVAL
     ,machine => ra_machine()
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
        _R -> 'ok'
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

-type ra_server_id() :: ra:server_id().

-spec delete() ->
          {'ok', ra_server_id()} | {'error', term()}.
delete() ->
    {'ok', Members, _Leader} = ra:members(id()),
    ra:delete_cluster(Members).

-spec start_new() -> 'ok' | {'error', 'cluster_not_formed'}.
start_new() ->
    RaConfs = [make_ra_conf(ServerId) || ServerId <- members()],
    case ra:start_cluster(fax_ra_node:ra_name(), RaConfs) of
        {'ok', _, _} -> 'ok';
        Else -> Else
    end.

-spec stop() -> 'ok' | {'error', 'nodedown'}.
stop() ->
    ra:stop_server(id()).

-spec available_nodes() -> [node()].
available_nodes() ->
    {'ok', Members, _Leader} = ra:members(id()),
    lists:usort([Node || {_, Node} <- Members, lists:member(Node, nodes())]).

-spec rand_node() -> node().
rand_node() ->
    rand_node(available_nodes()).

-spec rand_node([node()]) -> node().
rand_node([]) -> node();
rand_node(Nodes) ->
    hd(kz_term:shuffle_list(Nodes)).
