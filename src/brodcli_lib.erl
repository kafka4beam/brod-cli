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

-module(brodcli_lib).

-export([
    common_opts/0,
    with_brod/3,
    logerr/1,
    logerr/2,
    print/1,
    print/2,
    parse_cmd_args/3
]).

-include("brodcli.hrl").

-define(RED, "\e[31m").
-define(YELLOW, "\e[33m").
-define(RESET, "\e[39m").

common_opts() ->
    [
        {ssl, ?undef, "ssl", {atom, false},
            "'true' for mTLS (verify server certificate and host name); "
            "'false' for plaintext TCP; "
            "'insecure' for TLS without verification."},
        {ssl_versions, ?undef, "tls-vsn", {string, ""},
            "Specify SSL versions. Comma separated versions, e.g. '1.3,1.2'."},
        {ssl_cacertfile, ?undef, "cacertfile", {string, ""},
            "Validate server using the given PEM file containing trusted CA certifcates."},
        {ssl_certfile, ?undef, "certfile", {string, ""},
            "Client certificate PEM file. "
            "If the certificate is not directly issued by the root CA, "
            "the certificate should be followed by intermediate CA certificates to form the certificate chain."},
        {ssl_keyfile, ?undef, "keyfile", {string, ""}, "Client private key PEM file."},
        {auth_mechanism, ?undef, "auth-mech", {atom, none},
            "Authentication mechanism: 'none' | 'plain' | 'scram256' | 'scram512'."},
        {credentials, ?undef, "credentials", {string, ""},
            "Colon separated username:password for autentication. "
            "Or 'file://<path>' if username and password are stored in a file "
            "(the file should have username and password in two lines)."},
        {ebin_paths, ?undef, "ebin-paths", {string, ""},
            "Comma separated directory names for extra beams. "
            "This is to support user compiled message formatters."}
    ].

parse_cmd_args(Opts, Args, Stop) ->
    case getopt:parse_and_check(Opts, Args) of
        {ok, {Parsed, _}} ->
            Parsed;
        {error, Reason} ->
            brodcli_lib:logerr("~p~n", [Reason]),
            ?STOP(Stop, 1)
    end.

with_brod(Args, Stop, F) ->
    LogLevel = get(brodcli_log_level),
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
        Brokers = parse_brokers(proplists:get_value(brokers, Args)),
        ConnConfig = parse_connection_config(Args),
        Paths = parse_paths(proplists:get_value(ebin_paths, Args)),
        ok = code:add_pathsa(Paths),
        verbose("connection config: ~0p\n", [ConnConfig]),
        F(Args, Brokers, ConnConfig)
    catch
        throw:Reason when is_binary(Reason) ->
            %% invalid options etc.
            logerr([Reason, "\n"]),
            ?STOP(Stop, 1);
        C2:E2:Stack2 ->
            logerr("~p:~p\n~p\n", [C2, E2, Stack2]),
            ?STOP(Stop, 2)
    end.

parse_brokers(HostsStr) ->
    F = fun(HostPortStr) ->
        Pair = string:tokens(HostPortStr, ":"),
        case Pair of
            [Host, PortStr] -> {Host, list_to_integer(PortStr)};
            [Host] -> {Host, 9092}
        end
    end,
    shuffle(lists:map(F, string:tokens(HostsStr, ","))).

logerr(IoData) ->
    io:put_chars(stderr(), [?RED, "*** ", IoData, ?RESET]).

logerr(Fmt, Args) ->
    io:put_chars(stderr(), io_lib:format(?RED ++ "*** " ++ Fmt ++ ?RESET, Args)).

stderr() ->
    case get(redirect_stderr) of
        undefined -> standard_error;
        Other -> Other
    end.

print(IoData) ->
    io:put_chars(stdio(), IoData).

print(Fmt, Args) ->
    io:put_chars(stdio(), io_lib:format(Fmt, Args)).

stdio() ->
    case get(redirect_stdio) of
        undefined -> user;
        Other -> Other
    end.

%% Parse code paths.
parse_paths(?undef) -> [];
parse_paths("") -> [];
parse_paths(Str) -> string:tokens(Str, ",").

verbose(Fmt, Args) ->
    case erlang:get(brodcli_log_level) >= ?LOG_LEVEL_VERBOSE of
        true -> print(?YELLOW ++ "[verbo]: " ++ Fmt ++ ?RESET, Args);
        false -> ok
    end.

%% Randomize the order.
shuffle(L) ->
    RandList = lists:map(fun(_) -> element(3, os:timestamp()) end, L),
    {_, SortedL} = lists:unzip(lists:keysort(1, lists:zip(RandList, L))),
    SortedL.

parse_connection_config(Args) ->
    parse_ssl(Args) ++ parse_auth(Args).

parse_ssl(Args) ->
    SslFlag = parse(Args, ssl, fun(X) -> X end),
    case SslFlag of
        true ->
            Opts1 = ssl_opts(Args),
            [{ssl, lists:keystore(verify, 1, Opts1, {verify, verify_peer})}];
        insecure ->
            Opts1 = ssl_opts(Args),
            [{ssl, lists:keystore(verify, 1, Opts1, {verify, verify_none})}];
        false ->
            [{ssl, false}]
    end.

ssl_opts(Args) ->
    SslVersions = parse(Args, ssl_versions, fun parse_ssl_versions/1),
    CaCertFile = parse(Args, ssl_cacertfile, fun parse_file/1),
    CertFile = parse(Args, ssl_certfile, fun parse_file/1),
    KeyFile = parse(Args, ssl_keyfile, fun parse_file/1),
    FilterPred = fun({_, V}) -> V =/= ?undef andalso V =/= [] end,
    Opts = [
        {cacertfile, CaCertFile},
        {certfile, CertFile},
        {keyfile, KeyFile},
        {versions, SslVersions},
        {verify, verify_peer},
        {sni, auto}
    ],
    lists:filter(FilterPred, Opts).

parse_auth(Args) ->
    Mechanism = parse(Args, auth_mechanism, fun parse_auth_mechanism/1),
    case Mechanism of
        none ->
            [];
        _ ->
            case parse(Args, credentials, fun parse_auth_credentials/1) of
                {ok, {Username, Password}} ->
                    [{Mechanism, Username, Password}];
                {ok, FilePath} ->
                    [{Mechanism, FilePath}]
            end
    end.

parse_auth_credentials("file://" ++ Path) ->
    {ok, parse_file(Path)};
parse_auth_credentials(UsernamePassword) ->
    case binary:split(bin(UsernamePassword), <<":">>) of
        [Username, Password] ->
            {ok, {Username, fun() -> Password end}};
        _ ->
            throw("invalid username:password format")
    end.

parse_auth_mechanism(none) -> none;
parse_auth_mechanism(plain) -> plain;
parse_auth_mechanism(scram256) -> scram_sha_256;
parse_auth_mechanism(scram512) -> scram_sha_512;
parse_auth_mechanism(Other) -> throw(bin(["unknown_mechanism:", bin(Other)])).

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
parse_file("") ->
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
                            ?undef -> ["Missing option ", atom_to_list(OptName)];
                            _ -> ["Failed to parse ", atom_to_list(OptName), ": ", bin(Arg)]
                        end,
                    erlang:throw(bin(Reason))
            end;
        false ->
            Reason = [atom_to_list(OptName), " is missing"],
            erlang:throw(bin(Reason))
    end.

bin(A) when is_atom(A) -> atom_to_binary(A);
bin(IoData) -> iolist_to_binary(IoData).
