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

-module(brodcli_send).

-export([help/0, main/2]).

-include("brodcli.hrl").

-define(PROGRAM, "brod send").

opts() ->
    [
        {help, $h, "help", ?undef, "Show 'send' command usage."},
        {brokers, $b, "brokers", {string, "localhost:9092"}, "Comma separated host:port pairs."},
        {topic, $t, "topic", {string, ""}, "Topic name."},
        {partition, $p, "partition", {string, "0"},
            "Partition number. "
            " random): randomly pick a partition; "
            " hash): hash key to a partition."},
        {key, $k, "key", {string, ""}, "Key to produce."},
        {value, $v, "value", {string, ""},
            "Value to produce. "
            "@/path/to/file: Send a whole file as payload."},
        {acks, ?undef, "acks", {atom, all},
            "Required acks. "
            "all): Require acks from all in-sync replica; "
            "leader): Require acks from only partition leader; "
            "none): Require no acks."},
        {ack_timeout, ?undef, "ack-timeout", {string, "10s"},
            "How long the partition leader should wait for replicas "
            "to ack before sending response to producer."},
        {compression, ?undef, "compression", {atom, none}, "Supported values: none / gzip / snappy"}
        | brodcli_lib:common_opts()
    ].

help() ->
    getopt:usage(opts(), ?PROGRAM).

main(Args, Stop) ->
    Parsed = brodcli_lib:parse_cmd_args(opts(), Args, Stop),
    ok = brodcli_lib:with_brod(Parsed, Stop, fun do/3),
    ?STOP(Stop, 0).

do(Args, Brokers, ConnConfig) ->
    Topic = topic(Args),
    Partition = partition(Args),
    Acks = acks(Args),
    AckTimeout = ack_timeout(Args),
    Compression = compression(Args),
    Key = key(Args),
    Value = value(Args),
    ProducerConfig =
        [
            {required_acks, Acks},
            {ack_timeout, AckTimeout},
            {compression, Compression},
            {min_compression_batch_size, 0},
            {max_linger_ms, 0}
        ],
    ClientConfig =
        [
            {auto_start_producers, true},
            {default_producer_config, ProducerConfig}
            | ConnConfig
        ],
    ok = start_client(Brokers, ClientConfig),
    Msgs = [{brod_utils:epoch_ms(), Key, Value}],
    ok = brod:produce_sync(?CLIENT, Topic, Partition, <<>>, Msgs),
    ok.

start_client(BootstrapEndpoints, ClientConfig) ->
    {ok, _} = brod_client:start_link(BootstrapEndpoints, ?CLIENT, ClientConfig),
    ok.

bin(X) -> iolist_to_binary(X).

topic(Args) ->
    bin(proplists:get_value(topic, Args)).

partition(Args) ->
    parse_partition(proplists:get_value(partition, Args)).

parse_partition("random") ->
    fun(_Topic, PartitionsCount, _Key, _Value) ->
        {_, _, Micro} = os:timestamp(),
        {ok, Micro rem PartitionsCount}
    end;
parse_partition("hash") ->
    fun(_Topic, PartitionsCount, Key, _Value) ->
        Hash = erlang:phash2(Key),
        {ok, Hash rem PartitionsCount}
    end;
parse_partition(I) ->
    try
        list_to_integer(I)
    catch
        _:_ ->
            erlang:throw(bin(["Bad partition: ", I]))
    end.

ack_timeout(Args) ->
    parse_timeout(proplists:get_value(ack_timeout, Args)).

compression(Args) ->
    compression2(proplists:get_value(compression, Args)).

compression2(none) -> no_compression;
compression2(gzip) -> gzip;
compression2(snappy) -> snappy;
compression2(X) -> erlang:throw(bin(["Unknown --compresion value: ", X])).

parse_timeout(Str) ->
    case lists:reverse(Str) of
        "s" ++ R -> int(lists:reverse(R)) * 1000;
        "m" ++ R -> int(lists:reverse(R)) * 60 * 1000;
        _ -> int(Str)
    end.

key(Args) ->
    bin(proplists:get_value(key, Args)).

value(Args) ->
    case bin(proplists:get_value(value, Args)) of
        <<"@", Path/binary>> ->
            {ok, Bin} = file:read_file(Path),
            Bin;
        Bin ->
            Bin
    end.

int(Str) -> list_to_integer(trim(Str)).

trim_h([$\s | T]) -> trim_h(T);
trim_h(X) -> X.

trim(Str) -> trim_h(lists:reverse(trim_h(lists:reverse(Str)))).

acks(Args) ->
    case proplists:get_value(acks, Args) of
        all -> -1;
        leader -> 1;
        none -> 0;
        X -> erlang:throw(bin(["Unknown --acks value: ", X]))
    end.
