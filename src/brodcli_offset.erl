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

-module(brodcli_offset).

-export([opts/0, help/0, main/3]).

-include("brodcli.hrl").

-define(PROGRAM, "brod offset").

opts() ->
    [
        {help, $h, "help", ?undef, "Show 'meta' command usage."},
        {brokers, $b, "brokers", {string, "localhost:9092"}, "Comma separated host:port pairs."},
        {topic, $t, "topic", string, "Topic name, * for ALL topics."},
        {partition, $p, "partition", {string, "*"}, "Partition number, * for ALL partitions."},
        {time, $T, "time", {string, "latest"},
            "Unix epoch (in milliseconds) of the correlated offset to fetch. "
            "Special values are 'earliest' or 'latest'."},
        {json, $J, "json", {boolean, false}, "Print partition offsets in JSON format."}
        | brodcli_lib:common_opts()
    ].

help() ->
    getopt:usage(opts(), ?PROGRAM).

main(Args, Brokers, ConnConfig) ->
    Partition =
        case proplists:get_value(partition, Args) of
            ("*") -> all;
            ("all") -> all;
            (Num) -> int(Num)
        end,
    Time = offset_time(proplists:get_value(time, Args)),
    Topic = bin(proplists:get_value(topic, Args)),
    ok = start_client(Brokers, ConnConfig),
    try
        resolve_offsets_print(Topic, Partition, Time)
    after
        brod_client:stop(?CLIENT)
    end.

resolve_offsets_print(Topic, all, Time) ->
    Offsets = resolve_offsets(Topic, Time),
    Outputs =
        lists:map(
            fun({Partition, Offset}) ->
                io_lib:format("~p:~p", [Partition, Offset])
            end,
            Offsets
        ),
    print([[Line, "\n"] || Line <- Outputs]);
resolve_offsets_print(Topic, Partition, Time) when is_integer(Partition) ->
    {ok, Offset} = resolve_offset(Topic, Partition, Time),
    print([integer_to_list(Offset), "\n"]).

resolve_offsets(Topic, Time) ->
    {ok, Count} = brod_client:get_partitions_count(?CLIENT, Topic),
    Partitions = lists:seq(0, Count - 1),
    lists:map(
        fun(P) ->
            {ok, Offset} = resolve_offset(Topic, P, Time),
            {P, Offset}
        end,
        Partitions
    ).

resolve_offset(Topic, Partition, Time) ->
    {ok, SockPid} = brod_client:get_leader_connection(?CLIENT, Topic, Partition),
    brod_utils:resolve_offset(SockPid, Topic, Partition, Time).

start_client(BootstrapEndpoints, ClientConfig) ->
    {ok, _} = brod_client:start_link(BootstrapEndpoints, ?CLIENT, ClientConfig),
    ok.

int(Str) ->
    list_to_integer(trim(Str)).

trim_h([$\s | T]) -> trim_h(T);
trim_h(X) -> X.

trim(Str) -> trim_h(lists:reverse(trim_h(lists:reverse(Str)))).

print(IoData) ->
    brodcli_lib:print(IoData).

bin(IoData) -> iolist_to_binary(IoData).

offset_time("latest") -> latest;
offset_time("earliest") -> earliest;
offset_time(T) -> list_to_integer(T).
