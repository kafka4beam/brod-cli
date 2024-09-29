%%%   Copyright (c) 2017-2021, Klarna Bank AB (publ)
%%%   Copyright (c) 2024 Kafka4beam contributors
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

-module(brodcli_tests).

-include_lib("eunit/include/eunit.hrl").
-include("brodcli.hrl").

%% no crash on 'help', 'version' etc commands
informative_test() ->
    % should not halt
    brod:main(["--help"]),
    run(["--version"]),
    run(["meta", "--help", "--debug"]).

error_test() ->
    put(redirect_stderr, standard_io),
    ?assertExit(N when is_integer(N), run(["-unknown-opt"])).

meta_test() ->
    run(["meta", "-b", "localhost", "-L"]),
    run(["meta", "-b", "localhost", "-t", "test-topic"]).

ssl_test() ->
    run([
        "meta",
        "-b",
        "localhost:9093",
        "-L",
        "--ssl",
        "--cacertfile",
        "test/data/ssl/ca.pem",
        "--keyfile",
        "test/data/ssl/client-key.pem",
        "--certfile",
        "test/data/ssl/client-crt.pem",
        "--ssl-versions",
        "1.2,1.1"
    ]).

offset_test() ->
    Args = ["offset", "-b", "localhost", "-t", "test-topic", "-p", "0"],
    run(Args),
    run(Args ++ ["-T", "latest"]),
    run(Args ++ ["-T", "earliest"]),
    run(Args ++ ["-T", "-1"]),
    run(Args ++ ["-T", "-2"]),
    run(Args ++ ["-T", "0"]).

send_fetch_test() ->
    K = make_ts_str(),
    V = make_ts_str(),
    Args = [
        "--brokers",
        "localhost:9092,localhost:9192",
        "-t",
        "test-topic",
        "-p",
        "0"
    ],
    Output = cmd(["send", "-k", K, "-v", V] ++ Args),
    ?assertEqual(<<"">>, Output),
    FetchOutput = cmd(["fetch", "-c", "1", "--fmt", "kv"] ++ Args),
    ?assertEqual(iolist_to_binary(K ++ ":" ++ V ++ "\n"), FetchOutput),
    ok.

sasl_test_() ->
    case get_kafka_version() of
        {0, 9} -> [];
        _ -> [fun test_sasl/0]
    end.

test_sasl() ->
    ok = file:write_file("sasl.testdata", "alice\necila\n"),
    K = make_ts_str(),
    V = make_ts_str(),
    Output =
        cmd([
            "send",
            "--brokers",
            "localhost:9194,localhost:9094",
            "-t",
            "test-topic",
            "-p",
            "0",
            "--ssl",
            "--cacertfile",
            "test/data/ssl/ca.pem",
            "--keyfile",
            "test/data/ssl/client-key.pem",
            "--certfile",
            "test/data/ssl/client-crt.pem",
            "--sasl-plain",
            "sasl.testdata",
            "-k",
            K,
            "-v",
            V
        ]),
    ?assertEqual(<<"">>, Output),
    FetchOutput =
        cmd([
            "fetch",
            "--brokers",
            "localhost:9092",
            "-t",
            "test-topic",
            "-p",
            "0",
            "-c",
            "1",
            "--fmt",
            "kv"
        ]),
    ?assertEqual(iolist_to_binary([K, ":", V, "\n"]), FetchOutput),
    ok.

fetch_format_fun_test() ->
    T = os:timestamp(),
    Value = term_to_binary(T),
    file:write_file("fetch.testdata", Value),
    run([
        "send",
        "-b",
        "localhost",
        "-t",
        "test-topic",
        "-p",
        "0",
        "-v",
        "@fetch.testdata"
    ]),
    FmtFun = "io_lib:format(\"~p\", [binary_to_term(Value)])",
    Output =
        cmd([
            "fetch",
            "-b",
            "localhost",
            "-t",
            "test-topic",
            "-p",
            "0",
            "-c",
            "1",
            "--fmt",
            FmtFun
        ]),
    Expected = iolist_to_binary(io_lib:format("~p", [T])),
    ?assertEqual(Expected, Output).

fetch_format_expr_test() ->
    T = os:timestamp(),
    Value = term_to_binary(T),
    file:write_file("fetch.testdata", Value),
    cmd([
        "send",
        "-b",
        "localhost",
        "-t",
        "test-topic",
        "-p",
        "0",
        "-v",
        "@fetch.testdata"
    ]),
    FmtExpr = "io_lib:format(\"~p\", [binary_to_term(Value)])",
    Output =
        cmd([
            "fetch",
            "-b",
            "localhost",
            "-t",
            "test-topic",
            "-p",
            "0",
            "-c",
            "1",
            "--fmt",
            FmtExpr
        ]),
    Expected = iolist_to_binary(io_lib:format("~p", [T])),
    ?assertEqual(Expected, Output).

pipe_test() ->
    %% get last offset
    OffsetStr = cmd([
        "offset",
        "-b",
        "localhost",
        "-t",
        "test-topic",
        "-p",
        "0",
        "-T",
        "latest"
    ]),
    %% send file
    PipeCmdOutput =
        cmd([
            "pipe",
            "-b",
            "localhost",
            "-t",
            "test-topic",
            "-p",
            "0",
            "-s",
            "README.md",
            "--kv-deli",
            "none",
            "--msg-deli",
            "'\\n'"
        ]),
    ?assertEqual(<<"">>, PipeCmdOutput),
    FetchedText =
        cmd([
            "fetch",
            "-b",
            "localhost",
            "-t",
            "test-topic",
            "-p",
            "0",
            "-w",
            "100",
            "-c",
            "-1",
            "-o",
            binary_to_list(OffsetStr)
        ]),
    {ok, ReadmeText} = file:read_file("README.md"),
    Split =
        fun(Text) ->
            Lines = binary:split(iolist_to_binary(Text), <<"\n">>, [global]),
            lists:filtermap(
                fun
                    (<<>>) -> false;
                    (Line) -> {true, iolist_to_binary([Line, "\n"])}
                end,
                Lines
            )
        end,
    ExpectedLines = Split(ReadmeText),
    FetchedLines = Split(FetchedText),
    ?assertEqual(hd(ExpectedLines), hd(FetchedLines)),
    ?assertEqual(lists:last(ExpectedLines), lists:last(FetchedLines)),
    ?assertEqual(ExpectedLines, FetchedLines).

groups_test() ->
    assert_no_error(cmd(["groups"])),
    assert_no_error(cmd(["groups", "--ids", "all"])).

commits_describe_test() ->
    assert_no_error(cmd(["commits", "--id", "test-group", "--describe"])).

commits_overwrite_test_() ->
    {timeout, 20, fun() ->
        assert_no_error(
            cmd([
                "commits",
                "--id",
                "test-group",
                "-t",
                "test-topic",
                "-o",
                "0:1",
                "-r",
                "1d",
                "--protocol",
                "range"
            ])
        )
    end}.

assert_no_error(Result) ->
    case binary:match(iolist_to_binary(Result), <<"***">>) of
        nomatch -> ok;
        _ -> erlang:throw(Result)
    end.

make_ts_str() ->
    Ts = os:timestamp(),
    {{Y, M, D}, {H, Min, Sec}} = calendar:now_to_universal_time(Ts),
    {_, _, Micro} = Ts,
    S = io_lib:format(
        "~4.4.0w-~2.2.0w-~2.2.0w:~2.2.0w:~2.2.0w:~2.2.0w.~6.6.0w",
        [Y, M, D, H, Min, Sec, Micro]
    ),
    lists:flatten(S).

get_kafka_version() ->
    case os:getenv("KAFKA_VERSION") of
        false ->
            no_value;
        Vsn ->
            [Major, Minor | _] = string:tokens(Vsn, "."),
            {list_to_integer(Major), list_to_integer(Minor)}
    end.

run(Args0) ->
    Args =
        case kafka_test_helper:kafka_version() of
            {0, Minor} when Minor < 11 ->
                Args0 ++ ["--no-api-vsn-query"];
            _ ->
                Args0
        end,
    _ = cmd(Args),
    ok.

cmd(Args) ->
    Parent = self(),
    IO = erlang:spawn_link(fun() -> io_loop(Parent, []) end),
    put(redirect_stdio, IO),
    try
        brodcli:main(Args, exit),
        % avoid race
        timer:sleep(10),
        IO ! stop,
        Result =
            receive
                {outputs, Outputs} -> Outputs
            after 5000 -> throw(timeout)
            end,
        catch brod:stop_client(?CLIENT_ID),
        Result
    after
        _ = brod:stop(),
        ensure_down(brod_kafka_apis)
    end.

ensure_down(RegName) -> ensure_down(RegName, 10).

ensure_down(RegName, 0) ->
    error({wont_die, RegName});
ensure_down(RegName, N) ->
    case whereis(RegName) of
        undefined ->
            ok;
        Pid ->
            exit(Pid, kill),
            timer:sleep((11 - N) * 10),
            ensure_down(RegName, N - 1)
    end.

io_loop(Parent, Acc0) ->
    receive
        {io_request, From, ReplyAs, Req} ->
            Acc = io(From, ReplyAs, Req, Acc0),
            io_loop(Parent, Acc);
        stop ->
            Parent ! {outputs, iolist_to_binary(Acc0)}
    end.

io(From, ReplyAs, Req, Acc0) ->
    {Reply, Acc} = io(Req, Acc0),
    erlang:send(From, {io_reply, ReplyAs, Reply}),
    Acc.

%% supports only a subset of io requests
io({put_chars, Chars}, Acc) ->
    {ok, [Chars | Acc]};
io({put_chars, _Code, Chars}, Acc) ->
    io({put_chars, Chars}, Acc);
io({setopts, _Opts}, Acc) ->
    {ok, Acc};
io(Unknown, _Acc) ->
    io:format(standard_error, "unknown_io_request: ~p\n", [Unknown]),
    exit({unknown_io_request, Unknown}).

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
