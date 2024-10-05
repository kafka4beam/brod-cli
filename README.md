# brod-cli

Kafka client command line interface based on [brod](https://github.com/kafka4beam/brod)

Disclaimer: This script is NOT designed for use cases where fault-tolerance is a hard requirement.
As it may crash when e.g. kafka cluster is temporarily unreachable,
or (for fetch command) when the partition leader migrates to another broker in the cluster.

## Build

```
make
```

The executable is here: `_build/default/rel/brod/bin/brod`

## Run

The release includes Erlang runtime (erts), but not guarenteed to be portable between different Linux distributions or versions.

In the release directory, `bin/bord` is the main command.

To start an Erlang REPL shell with all the `brod` APIs to play with, execute `bin/brod-i console`.

## Examples (with `alias brod=_build/default/rel/brod/bin/brod`):

### Fetch and print metadata

```sh
brod meta -b localhost
```

### Produce a Message

```sh
brod send -b localhost -t test-topic -p 0 -k "key" -v "value"

```

### Fetch a Message

```sh
brod fetch -b localhost -t test-topic -p 0 --fmt 'io:format("offset=~p, ts=~p, key=~s, value=~s\n", [Offset, Ts, Key, Value])'
```

Bound variables to be used in `--fmt` expression:

- `Offset`: Message offset
- `Key`: Kafka key
- `Value`: Kafka Value
- `TsType`: Timestamp type either `create` or `append`
- `Ts`: Timestamp, `-1` as no value

### Stream Messages to Kafka

Send `README.md` to kafka one line per kafka message

```sh
brod pipe -b localhost:9092 -t test-topic -p 0 -s @./README.md
```

### Resolve Offset

```sh
brod offset -b localhost:9092 -t test-topic -p 0
```

### List or Describe Groups

```sh
# List all groups
brod groups -b localhost:9092

# Describe groups
brod groups -b localhost:9092 --ids group-1,group-2
```

### Display Committed Offsets

```sh
# all topics
brod commits -b localhost:9092 --id the-group-id --describe

# a specific topic
brod commits -b localhost:9092 --id the-group-id --describe --topic topic-name
```

### Commit Offsets

NOTE: This feature is designed for force overwriting commits, not for regular use of offset commit.

```sh
# Commit 'latest' offsets of all partitions with 2 days retention
brod commits -b localhost:9092 --id the-group-id --topic topic-name --offsets latest --retention 2d

# Commit offset=100 for partition 0 and 200 for partition 1
brod commits -b localhost:9092 --id the-group-id --topic topic-name --offsets "0:100,1:200"

# Use --retention 0 to delete commits (may linger in kafka before cleaner does its job)
brod commits -b localhost:9092 --id the-group-id --topic topic-name --offsets latest --retention 0

# Try join an active consumer group using 'range' protocol and steal one partition assignment then commit offset=10000
brod commits -b localhost:9092 -i the-group-id -t topic-name -o "0:10000" --protocol range
```

## TODO

- [ ] Use json module from Erlang/OTP 27 (replace jsone)
- [ ] Switch to https://github.com/jcomellas/getopt (replace optdoc)
- [ ] Eunit
- [ ] Dialyzer
- [ ] GitHub Action
- [ ] Publish Docker image
- [ ] Support SASL-SCRAM auth
