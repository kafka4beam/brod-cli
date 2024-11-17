%%%   Copyright (c) 2017-2021 Klarna Bank AB (publ)
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

-module(brodcli).

-export([main/1]).

%% TODO: delete
-export([main/4]).

-include("brodcli.hrl").

-define(PROGNAME, "brod").

%% NOTE: bad indentation at the first line is intended
-define(COMMAND_COMMON_OPTIONS,
    "  --ssl                  Use TLS, validate server using trusted CAs\n"
    "  --ssl-versions=<vsns>  Specify SSL versions. Comma separated versions,\n"
    "                         e.g. 1.3,1.2\n"
    "  --cacertfile=<cacert>  Use TLS, validate server using the given certificate\n"
    "  --certfile=<certfile>  Client certificate in case client authentication\n"
    "                         is enabled in brokers\n"
    "  --keyfile=<keyfile>    Client private key in case client authentication\n"
    "                         is enabled in brokers\n"
    "  --sasl-plain=<file>    Tell brod to use username/password stored in the\n"
    "                         given file, the file should have username and\n"
    "                         password in two lines.\n"
    "  --scram256=<file>      Like sasl-plain option, but to use scram-sha-256\n"
    "  --scram512=<file>      Like sasl-plain option, but to use scram-sha-512\n"
    "  --ebin-paths=<dirs>    Comma separated directory names for extra beams,\n"
    "                         This is to support user compiled message formatters\n"
    "  --no-api-vsn-query     Do not query api version (for kafka 0.9 or earlier)\n"
    "                         Or set KAFKA_VERSION environment variable to 0.9 for\n"
    "                         the same effect\n"
).

-define(FETCH_CMD, "fetch").
-define(FETCH_DOC,
    "usage:\n"
    "  brod fetch [options]\n"
    "\n"
    "options:\n"
    "  -b,--brokers=<brokers> Comma separated host:port pairs\n"
    "                         [default: localhost:9092]\n"
    "  -t,--topic=<topic>     Topic name\n"
    "  -p,--partition=<parti> Partition number\n"
    "  -o,--offset=<offset>   Offset to start fetching from\n"
    "                           latest: From the latest offset (not last)\n"
    "                           earliest: From earliest offset (first)\n"
    "                           last: From offset (latest - 1)\n"
    "                           <integer>: From a specific offset\n"
    "                         [default: last]\n"
    "  -c,--count=<count>     Number of messages to fetch (-1 as infinity)\n"
    "                         [default: 1]\n"
    "  -w,--wait=<seconds>    Time in seconds to wait for one message set\n"
    "                         [default: 5s]\n"
    "  --kv-deli=<deli>       Delimiter for offset, key and value output [default: :]\n"
    "  --msg-deli=<deli>      Delimiter between messages. [default: \\n]\n"
    "  --max-bytes=<bytes>    Max number of bytes kafka should try to accumulate\n"
    "                         within the --wait time\n"
    "                         [default: 1K]\n"
    "  --fmt=<fmt>            Output format. Assume keys and values are utf8 strings\n"
    "                         v:     Print 'V <msg-deli>'\n"
    "                         kv:    Print 'K <kv-deli> V <msg-deli>'\n"
    "                         okv:   Print 'O <kv-deli> K <kv-deli> V <msg-deli>'\n"
    "                         eterm: Pretty print tuple '{Offse, Key, Value}.'\n"
    "                                to a consultable Erlang term format.\n"
    "                         Expr:  An Erlang expression to be evaluated for each\n"
    "                                message. Bound variable to be used in the\n"
    "                                expression: Offset, Key, Value, TsType, Ts.\n"
    "                                Print nothing if the evaluation result in 'ok',\n"
    "                                otherwise print the evaluated io-list.\n"
    "                         [default: v]\n"
    ?COMMAND_COMMON_OPTIONS
    "NOTE: Reaching either --count or --wait limit will cause script to exit\n"
).

-define(PIPE_CMD, "pipe").
-define(PIPE_DOC,
    "usage:\n"
    "  brod pipe [options]\n"
    "\n"
    "options:\n"
    "  -b,--brokers=<brokers> Comma separated host:port pairs\n"
    "                         [default: localhost:9092]\n"
    "  -t,--topic=<topic>     Topic name\n"
    "  -p,--partition=<parti> Partition number [default: 0]\n"
    "                         Special values:\n"
    "                           random: randomly pick a partition\n"
    "                           hash: hash key to a partition\n"
    "  -s,--source=<source>   Data source. Special value:\n"
    "                           stdin: Reads messages from standard-input\n"
    "                           @path/to/file: Reads from file\n"
    "                         [default: stdin]\n"
    "  --prompt               Applicable when --source is stdin, enable input prompt\n"
    "  --no-eof-exit          Do not exit when reaching EOF\n"
    "  --tail                 Applicable when --source is a file\n"
    "                         brod will start from EOF and keep tailing for new bytes\n"
    "  --msg-deli=<msg-deli>  Message delimiter\n"
    "                         NOTE: A message is always delimited when reaching EOF\n"
    "                         [default: \\n]\n"
    "  --kv-deli=<kv-deli>    Key-Value delimiter.\n"
    "                         when not provided, messages are produced with\n"
    "                         only value, key is set to null. [default: none]\n"
    "  --blk-size=<size>      Block size (bytes) when reading bytes from a file.\n"
    "                         Applicable when --source is file and --msg-deli is\n"
    "                         not \\n.  [default: 1M]\n"
    "  --acks=<acks>          Required acks. [default: all]\n"
    "                         Supported values:\n"
    "                           all or -1: Require acks from all in-sync replica\n"
    "                           1: Require acks from only partition leader\n"
    "                           0: Require no acks\n"
    "  --ack-timeout=<time>   How long the partition leader should wait for replicas\n"
    "                         to ack before sending response to producer\n"
    "                         not applicable when --acks is not 'all'\n"
    "                         [default: 10s]\n"
    "  --max-linger-ms=<ms>   Max ms for messages to linger in buffer [default: 200]\n"
    "  --max-linger-cnt=<N>   Max messages to linger in buffer [default: 100]\n"
    "  --max-batch=<bytes>    Max size for one message-set (before compression)\n"
    "                         The value can be either an integer to indicate bytes\n"
    "                         or followed by K/M to indicate KBytes or MBytes\n"
    "                         [default: 1M]\n"
    "  --compression=<compr>  Supported values: none/gzip/snappy [default: none]\n"
    ?COMMAND_COMMON_OPTIONS
    "NOTE: When --source is path/to/file, it by default reads from BOF\n"
    "      unless --tail is given.\n"
).

-define(GROUPS_CMD, "groups").
-define(GROUPS_DOC,
    "usage:\n"
    "  brod groups [options]\n"
    "\n"
    "options:\n"
    "  -b,--brokers=<brokers> Comma separated host:port pairs\n"
    "                         [default: localhost:9092]\n"
    "  --ids=<group-id>       Comma separated group IDs to describe\n"
    "                         [default: all]\n"
    ?COMMAND_COMMON_OPTIONS
).

-define(COMMITS_CMD, "commits").
-define(COMMITS_DOC,
    "usage:\n"
    "  brod commits [options]\n"
    "\n"
    "options:\n"
    "  -b,--brokers=<brokers> Comma separated host:port pairs\n"
    "                         [default: localhost:9092]\n"
    "  -d,--describe          Describe committed offsets,\n"
    "                         otherwise reset commit history\n"
    "  -i,--id=<group-id>     Group ID to describe, or to commit offsets.\n"
    "  -t,--topic=<topic>     Topic name to commit offsets\n"
    "  -o,--offsets=<offsets> latest: commit latest offset for all partitions,\n"
    "                         earliest: commit earliest offset for all partitions,\n"
    "                         Comma separated 'partition:offset' pairs.\n"
    "  -r,--retention=<time>  An integer to indicate the retention for the commit,\n"
    "                         default time unit is seconds. Accepts one char suffix\n"
    "                         as time unit, s=second m=mintue h=hour d=day.\n"
    "                         Default value -1 is to respect kafka config.\n"
    "                         [default: -1]\n"
    "  --protocol=<protocol>  Protocol name to be used when trying to join group.\n"
    "                         [default: roundrobin]\n"
    ?COMMAND_COMMON_OPTIONS
).

-define(DOCS, [
    {?SEND_CMD, ?SEND_DOC},
    {?FETCH_CMD, ?FETCH_DOC},
    {?PIPE_CMD, ?PIPE_DOC},
    {?GROUPS_CMD, ?GROUPS_DOC},
    {?COMMITS_CMD, ?COMMITS_DOC}
]).

-type log_level() :: non_neg_integer().

-type command() :: string().

opts() ->
    [
        {help, $h, "help", ?undef, "Show main command usage."},
        {version, $v, "version", ?undef, "Show version."},
        {verbose, ?undef, "verbose", ?undef, "Log verbosely."},
        {debug, ?undef, "debug", ?undef, "Log debug info."}
    ].

main_usage() ->
    getopt:usage(opts(), ?PROGNAME, "[command ...]"),
    print(
        "commands:\n"
        "  meta:    Inspect topic metadata\n"
        "  offset:  Inspect offsets\n"
        "  fetch:   Fetch messages\n"
        "  send:    Produce messages\n"
        "  pipe:    Pipe file or stdin as messages to kafka\n"
        "  groups:  List/describe consumer group\n"
        "  commits: List/describe committed offsets\n"
        "           or force overwrite existing commits\n"
    ).

main(Args0) ->
    ok = set_log_level(Args0),
    Args = Args0 -- ["--debug", "--verbose"],
    nomatch = main_help(Args),
    nomatch = version(Args),
    nomatch = cmd("meta", Args),
    nomatch = cmd("offset", Args),
    nomatch = cmd("send", Args),
    nomatch = cmd("pipe", Args),
    nomatch = cmd("groups", Args),
    nomatch = cmd("commits", Args),
    case Args of
        [] ->
            main_usage();
        ["-" ++ _ = Opt | _] ->
            logerr("Unknown option: ~ts~n", [Opt]);
        [Cmd | _] ->
            logerr("Unknown command: ~ts~n", [Cmd])
    end,
    halt(1).

main_help(Args) ->
    case find_bool_main_args(["-h", "--help"], Args) of
        true ->
            main_usage(),
            halt(0);
        false ->
            nomatch
    end.

version(Args) ->
    case find_bool_main_args(["-v", "--version"], Args) of
        true ->
            print_version(),
            halt(0);
        false ->
            nomatch
    end.

set_log_level(Args) ->
    IsDebug = lists:member("--debug", Args),
    IsVerbose = lists:member("--verbose", Args),
    LogLevels = [
        {IsDebug, ?LOG_LEVEL_DEBUG},
        {IsVerbose, ?LOG_LEVEL_VERBOSE},
        {true, ?LOG_LEVEL_QUIET}
    ],
    {true, LogLevel} = lists:keyfind(true, 1, LogLevels),
    erlang:put(brodcli_log_level, LogLevel),
    ok.

%% Find -o or --option style boolean flag before any non-option argument.
%% like lists:member/2, but this function stops finding as soon as it
%% sees a sub-command.
%% e.g. `--version meta -b localhost --help' will return `true' for finding `--version'
%% but will return `false' for finding `--help' (because `--help' is after command `meta'
%% so it is considered to be the option for the sub-commnad, but not main.
find_bool_main_args([], _) ->
    false;
find_bool_main_args([Flag | Flags], Args) ->
    do_find_bool_main_args(Flag, Args) orelse find_bool_main_args(Flags, Args).

do_find_bool_main_args(_, []) ->
    false;
do_find_bool_main_args(Flag, [Arg | Args]) ->
    case Flag =:= Arg of
        true ->
            true;
        false ->
            case hd(Arg) of
                $- ->
                    do_find_bool_main_args(Flag, Args);
                _ ->
                    false
            end
    end.

%% The main function calls this /2 implementation, stop with erlang:halt
cmd(Cmd, Args) ->
    cmd(Cmd, Args, halt).

%% The thrid arg can be `exit' in tests.
cmd(Cmd, [Cmd | Args], Stop) ->
    Module = list_to_atom("brodcli_" ++ Cmd),
    %% call Module:help if --help is found
    case lists:member("--help", Args) orelse lists:member("-h", Args) of
        true ->
            ok = Module:help(),
            ?STOP(Stop, 0);
        false ->
            ok = Module:main(Args, Stop)
    end;
cmd(_, _, _) ->
    nomatch.

-spec main(command(), string(), [string()], halt | exit) -> _ | no_return().
main(Command, Doc, Args0, Stop) ->
    IsHelp = lists:member("--help", Args0) orelse lists:member("-h", Args0),
    IsVerbose = lists:member("--verbose", Args0),
    IsDebug = lists:member("--debug", Args0),
    Args = Args0 -- ["--verbose", "--debug"],
    LogLevels = [
        {IsDebug, ?LOG_LEVEL_DEBUG},
        {IsVerbose, ?LOG_LEVEL_VERBOSE},
        {true, ?LOG_LEVEL_QUIET}
    ],
    {true, LogLevel} = lists:keyfind(true, 1, LogLevels),
    erlang:put(brodcli_log_level, LogLevel),
    case IsHelp of
        true ->
            print(Doc);
        false ->
            main(Command, Doc, Args, Stop, LogLevel)
    end.

-spec main(
    command(),
    string(),
    [string()],
    halt | exit,
    log_level()
) -> _ | no_return().
main(Command, Doc, Args, Stop, LogLevel) ->
    ParsedArgs =
        try
            docopt:docopt(Doc, Args, [debug || LogLevel =:= ?LOG_LEVEL_DEBUG])
        catch
            C1:E1:Stack1 ->
                verbose("~p:~p\n~p\n", [C1, E1, Stack1]),
                io:format(user, "~p~n", [{C1, E1, Stack1}]),
                ?STOP(Stop, 2)
        end,
    case LogLevel =:= ?LOG_LEVEL_QUIET of
        true ->
            logger:add_handler(
                brodcli,
                logger_std_h,
                #{config => #{file => "brod.log"}, level => info}
            ),
            logger:remove_handler(default);
        false ->
            ok
    end,
    %% So the linked processes won't take me with them
    %% This is to allow error_logger to write more logs
    %% before stopping init process immediately
    process_flag(trap_exit, true),
    ok = brod:start(),
    try
        Brokers = parse(ParsedArgs, "--brokers", fun parse_brokers/1),
        ConnConfig0 = parse_connection_config(ParsedArgs),
        Paths = parse(ParsedArgs, "--ebin-paths", fun(X) -> X end),
        NoApiQuery =
            parse(ParsedArgs, "--no-api-vsn-query", fun parse_boolean/1) orelse
                ({0, 9} =:= get_kafka_version()),
        ok = code:add_pathsa(Paths),
        SockOpts = [{query_api_versions, not NoApiQuery} | ConnConfig0],
        verbose("connection config: ~p\n", [SockOpts]),
        run(Command, Brokers, SockOpts, ParsedArgs)
    catch
        throw:Reason when is_binary(Reason) ->
            %% invalid options etc.
            logerr([Reason, "\n"]),
            ?STOP(Stop, 1);
        C2:E2:Stack2 ->
            logerr("~p:~p\n~p\n", [C2, E2, Stack2]),
            ?STOP(Stop, 2)
    end.

run(?FETCH_CMD, Brokers, Topic, ConnOpts, Args) ->
    %% not parse_partition/1
    Partition = parse(Args, "--partition", fun int/1),
    Count = parse(Args, "--count", fun int/1),
    Offset0 = parse(Args, "--offset", fun parse_offset_time/1),
    Wait = parse(Args, "--wait", fun parse_timeout/1),
    KvDeli = parse(Args, "--kv-deli", fun parse_delimiter/1),
    MsgDeli = parse(Args, "--msg-deli", fun parse_delimiter/1),
    FmtFun = parse(
        Args,
        "--fmt",
        fun(FmtOption) ->
            parse_fmt(FmtOption, KvDeli, MsgDeli)
        end
    ),
    MaxBytes = parse(Args, "--max-bytes", fun parse_size/1),
    {ok, Conn} = brod:connect_leader(Brokers, Topic, Partition, ConnOpts),
    Offset = resolve_begin_offset(Conn, Topic, Partition, Offset0),
    FetchOpts = #{max_wait_time => Wait, max_bytes => MaxBytes},
    FoldLimits =
        case Count < 0 of
            true -> #{};
            false -> #{message_count => Count}
        end,
    FoldFun =
        fun(M, Acc) ->
            #kafka_message{offset = O, key = K, value = V} = M,
            R =
                case is_function(FmtFun, 3) of
                    true -> FmtFun(O, K, V);
                    false -> FmtFun(M)
                end,
            case R of
                ok -> ok;
                IoData -> print(IoData)
            end,
            {ok, Acc + 1}
        end,
    {FetchedCount, NextOffset, Reason} =
        brod:fold(
            Conn,
            Topic,
            Partition,
            Offset,
            FetchOpts,
            0,
            FoldFun,
            FoldLimits
        ),
    logerr("Fetch loop stopped after ~p messages~n", [FetchedCount]),
    logerr("Continue at Offset: ~p~n", [NextOffset]),
    logerr("Reason: ~p~n", [Reason]);
run(?PIPE_CMD, Brokers, Topic, SockOpts, Args) ->
    Partition = parse(Args, "--partition", fun parse_partition/1),
    Acks = parse(Args, "--acks", fun parse_acks/1),
    AckTimeout = parse(Args, "--ack-timeout", fun parse_timeout/1),
    Compression = parse(Args, "--compression", fun parse_compression/1),
    KvDeli = parse(Args, "--kv-deli", fun parse_delimiter/1),
    MsgDeli = parse(Args, "--msg-deli", fun parse_delimiter/1),
    MaxLingerMs = parse(Args, "--max-linger-ms", fun int/1),
    MaxLingerCnt = parse(Args, "--max-linger-cnt", fun int/1),
    MaxBatch = parse(Args, "--max-batch", fun parse_size/1),
    Source = parse(Args, "--source", fun parse_source/1),
    IsPrompt = parse(Args, "--prompt", fun parse_boolean/1),
    IsTail = parse(Args, "--tail", fun parse_boolean/1),
    IsNoExit = parse(Args, "--no-eof-exit", fun parse_boolean/1),
    BlkSize = parse(Args, "--blk-size", fun parse_size/1),
    ProducerConfig =
        [
            {required_acks, Acks},
            {ack_timeout, AckTimeout},
            {compression, Compression},
            {min_compression_batch_size, 0},
            {max_linger_ms, MaxLingerMs},
            {max_linger_count, MaxLingerCnt},
            {max_batch_size, MaxBatch}
        ],
    ClientConfig =
        [
            {auto_start_producers, true},
            {default_producer_config, ProducerConfig}
        ] ++ SockOpts,
    ok = start_client(Brokers, ClientConfig),
    SendFun =
        fun(?TKV(Ts, Key, Value), PendingAcks) ->
            {ok, CallRef} =
                brod:produce(?CLIENT, Topic, Partition, <<>>, [{Ts, Key, Value}]),
            debug("sent: ~w\n", [CallRef]),
            debug("value: ~P\n", [Value, 9]),
            queue:in(CallRef, PendingAcks)
        end,
    KvDeliForReader =
        case none =:= KvDeli of
            true -> none;
            false -> bin(KvDeli)
        end,
    ReaderArgs = [
        {source, Source},
        {kv_deli, KvDeliForReader},
        {msg_deli, bin(MsgDeli)},
        {prompt, IsPrompt},
        {tail, IsTail},
        {no_exit, IsNoExit},
        {blk_size, BlkSize},
        {retry_delay, 100}
    ],
    {ok, ReaderPid} =
        brodcli_pipe:start_link(ReaderArgs),
    _ = erlang:monitor(process, ReaderPid),
    pipe(ReaderPid, SendFun, queue:new()).

run(?GROUPS_CMD, Brokers, SockOpts, Args) ->
    IDs = parse(Args, "--ids", fun parse_cg_ids/1),
    cg(Brokers, SockOpts, IDs);
run(?COMMITS_CMD, Brokers, SockOpts, Args) ->
    IsDesc = parse(Args, "--describe", fun parse_boolean/1),
    ID = parse(Args, "--id", fun bin/1),
    Topic = parse(
        Args,
        "--topic",
        fun
            (?undef) -> ?undef;
            (Name) -> bin(Name)
        end
    ),
    ok = start_client(Brokers, SockOpts),
    case IsDesc of
        true -> show_commits(ID, Topic);
        false -> reset_commits(ID, Topic, Args)
    end;
run(Cmd, Brokers, SockOpts, Args) ->
    %% Clause for all per-topic commands
    Topic = parse(Args, "--topic", fun bin/1),
    run(Cmd, Brokers, Topic, SockOpts, Args).

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

show_commits(GroupId, Topic) ->
    case brod:fetch_committed_offsets(?CLIENT, GroupId) of
        {ok, PerTopicStructs0} ->
            Pred = fun(S) -> Topic =:= ?undef orelse Topic =:= kf(topic, S) end,
            PerTopicStructs = lists:filter(Pred, PerTopicStructs0),
            lists:foreach(fun print_commits/1, PerTopicStructs);
        {error, Reason} ->
            throw_bin("Failed to fetch committed offsets ~p\n", [Reason])
    end.

reset_commits(ID, Topic, Args) ->
    Retention = parse(Args, "--retention", fun parse_retention/1),
    ProtocolName = parse(Args, "--protocol", fun(X) -> X end),
    Offsets0 = parse(Args, "--offsets", fun parse_commit_offsets_input/1),
    Offsets =
        case is_atom(Offsets0) of
            true -> resolve_offsets(Topic, Offsets0);
            false -> Offsets0
        end,
    Group = [
        {id, ID},
        {topic, Topic},
        {retention, Retention},
        {protocol, ProtocolName},
        {offsets, Offsets}
    ],
    brod_cg_commits:run(?CLIENT, Group).

parse_commit_offsets_input("latest") ->
    latest;
parse_commit_offsets_input("earliest") ->
    earliest;
parse_commit_offsets_input(PartitionOffsets) ->
    Pairs = string:tokens(PartitionOffsets, ","),
    F = fun(Pair) ->
        [Partition, Offset] = string:tokens(Pair, ":"),
        {int(Partition), parse_offset_time(Offset)}
    end,
    lists:map(F, Pairs).

parse_retention("-1") ->
    -1;
parse_retention([_ | _] = R) ->
    case lists:last(R) of
        X when X >= $0 andalso X =< $9 ->
            int(R);
        Unit ->
            int(lists:reverse(tl(lists:reverse(R)))) *
                case Unit of
                    $s -> 1;
                    $S -> 1;
                    $m -> 60;
                    $M -> 60;
                    $h -> 60 * 60;
                    $H -> 60 * 60;
                    $d -> 60 * 60 * 24;
                    $D -> 60 * 60 * 24
                end
    end.

print_commits(Struct) ->
    Topic = kf(name, Struct),
    PartRsps = kf(partitions, Struct),
    print([Topic, ":\n"]),
    print([pp_fmt_struct(1, P) || P <- PartRsps]).

cg(BootstrapEndpoints, SockOpts, all) ->
    %% List all groups
    All = list_groups(BootstrapEndpoints, SockOpts),
    lists:foreach(fun print_cg_cluster/1, All);
cg(BootstrapEndpoints, SockOpts, IDs) ->
    CgClusters = list_groups(BootstrapEndpoints, SockOpts),
    describe_cgs(CgClusters, SockOpts, lists:usort(IDs)).

describe_cgs(_, _SockOpts, []) ->
    ok;
describe_cgs([], _SockOpts, IDs) ->
    logerr("Unknown group IDs: ~s", [infix(IDs, ", ")]);
describe_cgs([{Coordinator, CgList} | Rest], SockOpts, IDs) ->
    %% Get all IDs managed by current coordinator.
    ThisIDs = [ID || #brod_cg{id = ID} <- CgList, lists:member(ID, IDs)],
    ok = do_describe_cgs(Coordinator, SockOpts, ThisIDs),
    IDsRest = IDs -- ThisIDs,
    describe_cgs(Rest, SockOpts, IDsRest).

do_describe_cgs(_Coordinator, _SockOpts, []) ->
    ok;
do_describe_cgs(Coordinator, SockOpts, IDs) ->
    case brod:describe_groups(Coordinator, SockOpts, IDs) of
        {ok, DescArray} ->
            ok = print("~s\n", [fmt_endpoint(Coordinator)]),
            lists:foreach(fun print_cg_desc/1, DescArray);
        {error, Reason} ->
            logerr(
                "Failed to describe IDs [~s] at broker ~s\nreason:~p\n",
                [infix(IDs, ","), fmt_endpoint(Coordinator), Reason]
            )
    end.

print_cg_desc(Desc) ->
    EC = kf(error_code, Desc),
    GroupId = kf(group_id, Desc),
    case ?IS_ERROR(EC) of
        true ->
            logerr("Failed to describe group id=~s\nreason:~p\n", [GroupId, EC]);
        false ->
            D1 = lists:keydelete(error_code, 1, ensure_list(Desc)),
            D = lists:keydelete(group_id, 1, ensure_list(D1)),
            print("  ~s\n~s", [GroupId, pp_fmt_struct(_Indent = 2, D)])
    end.

ensure_list(Struct) when is_map(Struct) -> maps:to_list(Struct);
ensure_list(List) when is_list(List) -> List.

pp_fmt_struct(Indent, Map) when is_map(Map) ->
    pp_fmt_struct(Indent, maps:to_list(Map));
pp_fmt_struct(Indent, Fields0) when is_list(Fields0) ->
    Fields =
        case Fields0 of
            [_] -> Fields0;
            _ -> lists:keydelete(no_error, 2, Fields0)
        end,
    F = fun
        (IsFirst, {N, V}) when is_map(V) ->
            indent_fmt(
                IsFirst,
                Indent,
                "~p:\n~s",
                [N, pp_fmt_struct(Indent + 1, V)]
            );
        (IsFirst, {N, V}) ->
            indent_fmt(
                IsFirst,
                Indent,
                "~p: ~s",
                [N, pp_fmt_struct_value(Indent, V)]
            )
    end,
    [
        F(true, hd(Fields))
        | lists:map(fun(Fi) -> F(false, Fi) end, tl(Fields))
    ].

pp_fmt_struct_value(_Indent, X) when
    is_integer(X) orelse
        is_atom(X) orelse
        is_binary(X) orelse
        X =:= []
->
    [pp_fmt_prim(X), "\n"];
pp_fmt_struct_value(Indent, [H | _] = Array) when is_list(Array) ->
    case is_tuple(H) orelse is_map(H) of
        true ->
            %% array of sub struct
            [
                "\n",
                lists:map(
                    fun(Item) ->
                        pp_fmt_struct(Indent + 1, Item)
                    end,
                    Array
                )
            ];
        false ->
            %% array of primitive values
            [infix([pp_fmt_prim(V) || V <- Array], ","), "\n"]
    end.

pp_fmt_prim([]) ->
    "[]";
pp_fmt_prim(N) when is_integer(N) -> integer_to_list(N);
pp_fmt_prim(A) when is_atom(A) -> atom_to_list(A);
pp_fmt_prim(S) when is_binary(S) ->
    case is_printable(S) of
        true -> S;
        false -> io_lib:format("~w", [S])
    end.

is_printable(B) when is_binary(B) ->
    is_printable(unicode:characters_to_list(B, utf8));
is_printable(S) when is_list(S) ->
    io_lib:printable_unicode_list(S);
is_printable(_) ->
    false.

indent_fmt(true, Indent, Fmt, Args) ->
    io_lib:format(lists:duplicate((Indent - 1) * 2, $\s) ++ "- " ++ Fmt, Args);
indent_fmt(false, Indent, Fmt, Args) ->
    io_lib:format(lists:duplicate(Indent * 2, $\s) ++ Fmt, Args).

print_cg_cluster({Endpoint, Cgs}) ->
    ok = print([fmt_endpoint(Endpoint), "\n"]),
    IoData = [
        io_lib:format("  ~s (~s)\n", [Id, Type])
     || #brod_cg{id = Id, protocol_type = Type} <- Cgs
    ],
    print(IoData).

fmt_endpoint({Host, Port}) ->
    bin(io_lib:format("~s:~B", [Host, Port])).

%% Return consumer groups clustered by group coordinator
%% {CoordinatorEndpoint, [group_id()]}.
list_groups(Brokers, SockOpts) ->
    Cgs = brod:list_all_groups(Brokers, SockOpts),
    lists:keysort(1, lists:foldl(fun do_list_groups/2, [], Cgs)).

do_list_groups({_Endpoint, []}, Acc) ->
    Acc;
do_list_groups({Endpoint, {error, Reason}}, Acc) ->
    logerr(
        "Failed to list groups at kafka ~s\nreason~p",
        [fmt_endpoint(Endpoint), Reason]
    ),
    Acc;
do_list_groups({Endpoint, Cgs}, Acc) ->
    [{Endpoint, Cgs} | Acc].

pipe(ReaderPid, SendFun, PendingAcks0) ->
    PendingAcks1 = flush_pending_acks(PendingAcks0, _Timeout = 0),
    receive
        {pipe, ReaderPid, Messages} ->
            PendingAcks = lists:foldl(SendFun, PendingAcks1, Messages),
            pipe(ReaderPid, SendFun, PendingAcks);
        {'DOWN', _Ref, process, ReaderPid, Reason} ->
            %% Reader is down, flush pending acks
            debug("reader down, reason: ~p\n", [Reason]),
            _ = flush_pending_acks(PendingAcks1, infinity);
        #brod_produce_reply{
            call_ref = CallRef,
            result = brod_produce_req_acked
        } ->
            {{value, CallRef}, PendingAcks} = queue:out(PendingAcks1),
            pipe(ReaderPid, SendFun, PendingAcks)
    end.

flush_pending_acks(Queue, Timeout) ->
    case queue:peek(Queue) of
        empty ->
            Queue;
        {value, CallRef} ->
            case brod:sync_produce_request(CallRef, Timeout) of
                ok ->
                    debug("acked: ~w\n", [CallRef]),
                    {_, Rest} = queue:out(Queue),
                    flush_pending_acks(Rest, Timeout);
                {error, timeout} ->
                    Queue
            end
    end.

resolve_begin_offset(_Sock, _T, _P, Offset) when is_integer(Offset) ->
    Offset;
resolve_begin_offset(Sock, Topic, Partition, last) ->
    Earliest = resolve_begin_offset(Sock, Topic, Partition, earliest),
    Latest = resolve_begin_offset(Sock, Topic, Partition, latest),
    case Latest =:= Earliest of
        true -> erlang:throw(bin("partition is empty"));
        false -> Latest - 1
    end;
resolve_begin_offset(Sock, Topic, Partition, Time) ->
    {ok, Offset} = brod_utils:resolve_offset(Sock, Topic, Partition, Time),
    Offset.

parse_source("stdin") ->
    standard_io;
parse_source("@" ++ Path) ->
    parse_source(Path);
parse_source(Path) ->
    case filelib:is_regular(Path) of
        true -> {file, Path};
        false -> erlang:throw(bin(["bad file ", Path]))
    end.

parse_size(Size) ->
    case lists:reverse(Size) of
        "K" ++ N -> int(lists:reverse(N)) * (1 bsl 10);
        "M" ++ N -> int(lists:reverse(N)) * (1 bsl 20);
        N -> int(lists:reverse(N))
    end.

infix(List, Sep) -> lists:join(Sep, List).

parse_delimiter("none") -> none;
parse_delimiter(EscappedStr) -> eval_str(EscappedStr).

eval_str([]) -> [];
eval_str([$\\, $n | Rest]) -> [$\n | eval_str(Rest)];
eval_str([$\\, $t | Rest]) -> [$\t | eval_str(Rest)];
eval_str([$\\, $s | Rest]) -> [$\s | eval_str(Rest)];
eval_str([C | Rest]) -> [C | eval_str(Rest)].

parse_fmt("v", _KvDel, MsgDeli) ->
    fun(_Offset, _Key, Value) -> [Value, MsgDeli] end;
parse_fmt("kv", KvDeli, MsgDeli) ->
    fun(_Offset, Key, Value) -> [Key, KvDeli, Value, MsgDeli] end;
parse_fmt("okv", KvDeli, MsgDeli) ->
    fun(Offset, Key, Value) ->
        [
            integer_to_list(Offset),
            KvDeli,
            Key,
            KvDeli,
            Value,
            MsgDeli
        ]
    end;
parse_fmt("eterm", _KvDeli, _MsgDeli) ->
    fun(Offset, Key, Value) ->
        io_lib:format("~p.\n", [{Offset, Key, Value}])
    end;
parse_fmt(FunLiteral0, _KvDeli, _MsgDeli) ->
    FunLiteral = ensure_end_with_dot(FunLiteral0),
    {ok, Tokens, _Line} = erl_scan:string(FunLiteral),
    {ok, [Expr]} = erl_parse:parse_exprs(Tokens),
    fun(
        #kafka_message{
            offset = Offset,
            key = Key,
            value = Value,
            ts_type = TsType,
            ts = Ts,
            headers = Headers
        }
    ) ->
        Bindings =
            lists:foldl(
                fun({VarName, VarValue}, Acc) ->
                    erl_eval:add_binding(VarName, VarValue, Acc)
                end,
                erl_eval:new_bindings(),
                [
                    {'Offset', Offset},
                    {'Key', Key},
                    {'Value', Value},
                    {'TsType', TsType},
                    {'Ts', Ts},
                    {'Headers', Headers}
                ]
            ),
        {value, Val, _NewBindings} = erl_eval:expr(Expr, Bindings),
        case Val of
            F when is_function(F, 3) ->
                %% for backward compatibility
                F(Offset, Key, Value);
            V ->
                V
        end
    end.

%% Append a dot to the function literal.
ensure_end_with_dot(Str0) ->
    Str = rstrip(Str0, [$\n, $\t, $\s, $.]),
    Str ++ ".".

rstrip(Str, CharSet) ->
    lists:reverse(lstrip(lists:reverse(Str), CharSet)).

lstrip([], _) ->
    [];
lstrip([C | Rest] = Str, CharSet) ->
    case lists:member(C, CharSet) of
        true -> lstrip(Rest, CharSet);
        false -> Str
    end.

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

parse_acks("all") -> -1;
parse_acks("-1") -> -1;
parse_acks("0") -> 0;
parse_acks("1") -> 1;
parse_acks(X) -> erlang:throw(bin(["Bad --acks value: ", X])).

parse_timeout(Str) ->
    case lists:reverse(Str) of
        "s" ++ R -> int(lists:reverse(R)) * 1000;
        "m" ++ R -> int(lists:reverse(R)) * 60 * 1000;
        _ -> int(Str)
    end.

parse_compression("none") -> no_compression;
parse_compression("gzip") -> gzip;
parse_compression("snappy") -> snappy;
parse_compression(X) -> erlang:throw(bin(["Unknown --compresion value: ", X])).

parse_offset_time("earliest") -> earliest;
parse_offset_time("latest") -> latest;
parse_offset_time("last") -> last;
parse_offset_time(T) -> int(T).

parse_connection_config(Args) ->
    SslBool = parse(Args, "--ssl", fun parse_boolean/1),
    SslVersions = parse(Args, "--ssl-versions", fun parse_ssl_versions/1),
    CaCertFile = parse(Args, "--cacertfile", fun parse_file/1),
    CertFile = parse(Args, "--certfile", fun parse_file/1),
    KeyFile = parse(Args, "--keyfile", fun parse_file/1),
    FilterPred = fun({_, V}) -> V =/= ?undef end,
    SslOpt =
        case SslBool of
            true ->
                Opts =
                    [
                        {cacertfile, CaCertFile},
                        {certfile, CertFile},
                        {keyfile, KeyFile},
                        {versions, SslVersions},
                        {verify, verify_none}
                    ],
                lists:filter(FilterPred, Opts);
            false ->
                false
        end,
    SaslPlain = parse(Args, "--sasl-plain", fun parse_file/1),
    SaslScram256 = parse(Args, "--scram256", fun parse_file/1),
    SaslScram512 = parse(Args, "--scram512", fun parse_file/1),
    SaslOpts0 = [
        {scram_sha_512, SaslScram512},
        {scram_sha_256, SaslScram256},
        {plain, SaslPlain}
    ],
    SaslOpts =
        case lists:filter(FilterPred, SaslOpts0) of
            [] -> [];
            [H | _] -> [{sasl, H}]
        end,
    lists:filter(FilterPred, [{ssl, SslOpt} | SaslOpts]).

parse_boolean(true) -> true;
parse_boolean(false) -> false;
parse_boolean("true") -> true;
parse_boolean("false") -> false;
parse_boolean(?undef) -> false.

parse_cg_ids("") -> [];
parse_cg_ids("all") -> all;
parse_cg_ids(Str) -> [bin(I) || I <- string:tokens(Str, ",")].

parse_ssl_versions(?undef) ->
    parse_ssl_versions("");
parse_ssl_versions(Versions) ->
    case lists:map(fun parse_ssl_version/1, string:tokens(Versions, ", ")) of
        [] ->
            ['tlsv1.2'];
        Vsns ->
            Vsns
    end.

parse_ssl_version("1.2") ->
    'tlsv1.2';
parse_ssl_version("1.3") ->
    'tlsv1.3';
parse_ssl_version("1.1") ->
    'tlsv1.1';
parse_ssl_version(Other) ->
    error({unsupported_tls_version, Other}).

parse_file(?undef) ->
    ?undef;
parse_file(Path) ->
    case filelib:is_regular(Path) of
        true -> Path;
        false -> erlang:throw(bin(["bad file ", Path]))
    end.

parse(Args, OptName, ParseFun) ->
    case lists:keyfind(OptName, 1, Args) of
        {_, Arg} ->
            try
                ParseFun(Arg)
            catch
                C:E:Stack ->
                    verbose("~p:~p\n~p\n", [C, E, Stack]),
                    Reason =
                        case Arg of
                            ?undef -> ["Missing option ", OptName];
                            _ -> ["Failed to parse ", OptName, ": ", Arg]
                        end,
                    erlang:throw(bin(Reason))
            end;
        false ->
            Reason = [OptName, " is missing"],
            erlang:throw(bin(Reason))
    end.

print_version() ->
    _ = application:load(brodcli),
    {_, _, V} = lists:keyfind(brodcli, 1, application:loaded_applications()),
    print([V, "\n"]).

print(IoData) ->
    brodcli_lib:print(IoData).

print(Fmt, Args) ->
    brodcli_lib:print(Fmt, Args).

logerr(IoData) ->
    brodcli_lib:logerr(IoData).

logerr(Fmt, Args) ->
    brodcli_lib:logerr(Fmt, Args).

verbose(Fmt, Args) ->
    case erlang:get(brodcli_log_level) >= ?LOG_LEVEL_VERBOSE of
        true -> logerr("[verbo]: " ++ Fmt, Args);
        false -> ok
    end.

debug(Fmt, Args) ->
    case erlang:get(brodcli_log_level) >= ?LOG_LEVEL_DEBUG of
        true -> logerr("[debug]: " ++ Fmt, Args);
        false -> ok
    end.

int(Str) -> list_to_integer(trim(Str)).

trim_h([$\s | T]) -> trim_h(T);
trim_h(X) -> X.

trim(Str) -> trim_h(lists:reverse(trim_h(lists:reverse(Str)))).

bin(IoData) -> iolist_to_binary(IoData).

parse_brokers(HostsStr) ->
    F = fun(HostPortStr) ->
        Pair = string:tokens(HostPortStr, ":"),
        case Pair of
            [Host, PortStr] -> {Host, list_to_integer(PortStr)};
            [Host] -> {Host, 9092}
        end
    end,
    shuffle(lists:map(F, string:tokens(HostsStr, ","))).

%% Randomize the order.
shuffle(L) ->
    RandList = lists:map(fun(_) -> element(3, os:timestamp()) end, L),
    {_, SortedL} = lists:unzip(lists:keysort(1, lists:zip(RandList, L))),
    SortedL.

-spec kf(kpro:field_name(), kpro:struct()) -> kpro:field_value().
kf(FieldName, Struct) -> kpro:find(FieldName, Struct).

start_client(BootstrapEndpoints, ClientConfig) ->
    {ok, _} = brod_client:start_link(BootstrapEndpoints, ?CLIENT, ClientConfig),
    ok.

-spec throw_bin(string(), [term()]) -> no_return().
throw_bin(Fmt, Args) ->
    erlang:throw(bin(io_lib:format(Fmt, Args))).

get_kafka_version() ->
    case os:getenv("KAFKA_VERSION") of
        false ->
            ?undef;
        Vsn ->
            [Major, Minor | _] = string:tokens(Vsn, "."),
            {list_to_integer(Major), list_to_integer(Minor)}
    end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
