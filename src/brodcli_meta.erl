%%%   Copyright (c) 2024 zmstone
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

-module(brodcli_meta).

-export([help/0, main/2]).

-include("brodcli.hrl").

-define(PROGRAM, "brod meta").

opts() ->
    [
        {help, $h, "help", ?undef, "Show 'meta' command usage."},
        {brokers, $b, "brokers", {string, "localhost:9092"}, "Comma separated host:port pairs."},
        {topic, $t, "topic", {string, "*"}, "Topic name, * for ALL topics."},
        {text, $T, "text", ?undef, "Print metadata in free text format."},
        {json, $J, "json", ?undef, "Print metadata in JSON format."},
        {list, $L, "list", ?undef, "List topics, no partition details."},
        {under_replicated, $U, "urp", ?undef, "List only under-replicated partitions."}
        | brodcli_lib:common_opts()
    ].

-define(EXTRA_INFO,
    "Text output schema (out of sync replicas are marked with *):\n"
    "brokers <count>:\n"
    "  <broker-id>: <endpoint>\n"
    "topics <count>:\n"
    "  <name> <count>: [[ERROR] [<reason>]]\n"
    "    <partition>: <leader-broker-id> (replicas[*]...) [<error-reason>]\n"
).

help() ->
    getopt:usage(opts(), ?PROGRAM),
    io:format(standard_error, "~s~n", [?EXTRA_INFO]).

main(Args, Stop) ->
    Parsed = brodcli_lib:parse_cmd_args(opts(), Args, Stop),
    ok = brodcli_lib:with_brod(Parsed, Stop, fun do/3),
    ?STOP(Stop, 0).

do(Args, Brokers, ConnConfig) ->
    Topic = bin(proplists:get_value(topic, Args)),
    Topics =
        case Topic of
            %% fetch all topics
            <<"*">> -> [];
            _ -> [Topic]
        end,
    IsJSON = proplists:get_bool(json, Args),
    IsText = proplists:get_bool(text, Args),
    Format = kf(true, [
        {IsJSON, json},
        {IsText, text},
        {true, text}
    ]),
    IsList = proplists:get_bool(list, Args),
    IsUrp = proplists:get_bool(under_replicated, Args),
    {ok, Metadata} = brod:get_metadata(Brokers, Topics, ConnConfig),
    format_metadata(Metadata, Format, IsList, IsUrp).

format_metadata(Metadata, Format, IsList, IsToListUrp) ->
    Brokers = kf(brokers, Metadata),
    Topics0 = kf(topics, Metadata),
    Cluster = kf(cluster_id, Metadata, ?undef),
    Controller = kf(controller_id, Metadata, ?undef),
    Topics1 =
        case IsToListUrp of
            true -> lists:filter(fun is_ur_topic/1, Topics0);
            false -> Topics0
        end,
    Topics = format_topics(Topics1),
    case Format of
        json ->
            JSON = json_encode([
                {brokers, Brokers},
                {topics, Topics},
                {cluster_id, Cluster},
                {controller_id, Controller}
            ]),
            print([JSON, "\n"]);
        text ->
            CL =
                case Cluster of
                    ?undef -> "";
                    _ -> io_lib:format("cluster_id: ~s\n", [Cluster])
                end,

            CT =
                case Controller of
                    ?undef -> "";
                    _ -> io_lib:format("controller: ~p\n", [Controller])
                end,
            BL = format_broker_lines(Brokers),
            TL = format_topics_lines(Topics, IsList),
            case IsList of
                true -> print(TL);
                false -> print([CL, CT, BL, TL])
            end
    end.

format_error_code(E) when is_atom(E) -> atom_to_list(E);
format_error_code(E) when is_integer(E) -> integer_to_list(E).

format_broker_line(Id, Rack, Endpoint) when
    Rack =:= ?kpro_null orelse Rack =:= <<>>
->
    io_lib:format("  ~p: ~s\n", [Id, Endpoint]);
format_broker_line(Id, Rack, Endpoint) ->
    io_lib:format("  ~p(~s): ~s\n", [Id, Rack, Endpoint]).

format_topic_list_line({Name, Partitions}) when is_list(Partitions) ->
    io_lib:format("  ~s\n", [Name]);
format_topic_list_line({Name, ErrorCode}) ->
    ErrorStr = format_error_code(ErrorCode),
    io_lib:format("  ~s: [ERROR] ~s\n", [Name, ErrorStr]).

format_topics_lines(Topics, true) ->
    Header = io_lib:format("topics [~p]:\n", [length(Topics)]),
    [Header, lists:map(fun format_topic_list_line/1, Topics)];
format_topics_lines(Topics, false) ->
    Header = io_lib:format("topics [~p]:\n", [length(Topics)]),
    [Header, lists:map(fun format_topic_lines/1, Topics)].

format_topic_lines({Name, Partitions}) when is_list(Partitions) ->
    Header = io_lib:format("  ~s [~p]:\n", [Name, length(Partitions)]),
    PartitionsText = format_partitions_lines(Partitions),
    [Header, PartitionsText];
format_topic_lines({Name, ErrorCode}) ->
    ErrorStr = format_error_code(ErrorCode),
    io_lib:format("  ~s: [ERROR] ~s\n", [Name, ErrorStr]).

format_partitions_lines(Partitions0) ->
    Partitions1 =
        lists:map(
            fun({Pnr, Info}) ->
                {binary_to_integer(Pnr), Info}
            end,
            Partitions0
        ),
    Partitions = lists:keysort(1, Partitions1),
    lists:map(fun format_partition_lines/1, Partitions).

format_partition_lines({Partition, Info}) ->
    LeaderNodeId = kf(leader, Info),
    Status = kf(status, Info),
    Isr = kf(isr, Info),
    MaybeWarning =
        case ?IS_ERROR(Status) of
            true -> [" [", atom_to_list(Status), "]"];
            false -> ""
        end,
    IsrFmt = format_list(Isr, ""),
    ReplicaList =
        try
            Osr = kf(osr, Info),
            [IsrFmt, ",", format_list(Osr, "*")]
        catch
            error:{no_such_field, osr} ->
                IsrFmt
        end,
    io_lib:format(
        "~7s: ~2s (~s)~s\n",
        [
            integer_to_list(Partition),
            integer_to_list(LeaderNodeId),
            ReplicaList,
            MaybeWarning
        ]
    ).

format_list(List, Mark) ->
    lists:join(",", lists:map(fun(I) -> [integer_to_list(I), Mark] end, List)).

format_broker_lines(Brokers) ->
    Header = io_lib:format("brokers [~p]:\n", [length(Brokers)]),
    F = fun(Broker) ->
        Id = kf(node_id, Broker),
        Host = kf(host, Broker),
        Port = kf(port, Broker),
        Rack = kf(rack, Broker, <<>>),
        HostStr = fmt_endpoint({Host, Port}),
        format_broker_line(Id, Rack, HostStr)
    end,
    [Header, lists:map(F, Brokers)].

%% Return true if a topics is under-replicated
is_ur_topic(Topic) ->
    ErrorCode = kf(error_code, Topic),
    Partitions = kf(partition_metadata, Topic),
    %% when there is an error, we do not know if
    %% it is under-replicated or not
    %% return true to alert user
    ?IS_ERROR(ErrorCode) orelse lists:any(fun is_ur_partition/1, Partitions).

%% Return true if a partition is under-replicated
is_ur_partition(Partition) ->
    ErrorCode = kf(error_code, Partition),
    Replicas = kf(replicas, Partition),
    Isr = kf(isr, Partition),
    ?IS_ERROR(ErrorCode) orelse lists:sort(Isr) =/= lists:sort(Replicas).

format_topics(Topics) ->
    TL = lists:map(fun format_topic/1, Topics),
    lists:keysort(1, TL).

format_topic(Topic) ->
    TopicName = kf(name, Topic),
    PL = kf(partitions, Topic),
    {TopicName, format_partitions(PL)}.

format_partitions(Partitions) ->
    PL = lists:map(fun format_partition/1, Partitions),
    lists:keysort(1, PL).

format_partition(P) ->
    ErrorCode = kf(error_code, P),
    PartitionNr = kf(partition_index, P),
    LeaderNodeId = kf(leader_id, P),
    Replicas = kf(replica_nodes, P),
    Isr = kf(isr_nodes, P),
    Data = [
        {leader, LeaderNodeId},
        {status, ErrorCode},
        {isr, Isr}
    ],
    Osr =
        case Replicas -- Isr of
            [] -> [];
            X -> [{osr, X}]
        end,
    {integer_to_binary(PartitionNr), Data ++ Osr}.

fmt_endpoint({Host, Port}) ->
    bin(io_lib:format("~s:~B", [Host, Port])).

json_encode(TupleList) ->
    json:encode(json_map(TupleList)).

json_map(List) when is_list(List) ->
    case lists:map(fun json_map/1, List) of
        [{_, _} | _] = R ->
            maps:from_list(R);
        R ->
            R
    end;
json_map({Key, Value}) ->
    {Key, json_map(Value)};
json_map(X) ->
    X.

print(IoData) ->
    brodcli_lib:print(IoData).

-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

-spec kf(kpro:field_name(), kpro:struct(), kpro:field_value()) ->
    kpro:field_value().
kf(FieldName, Struct, Default) ->
    kpro:find(FieldName, Struct, Default).

bin(X) -> iolist_to_binary(X).
