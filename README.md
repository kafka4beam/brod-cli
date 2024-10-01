# brod-cli

Kafka client command line interface based on [brod](https://github.com/kafka4beam/brod)

## Build

```
make
```

The executable is here: `_build/default/rel/brod/bin/brod`

## Run

The release includes Erlang runtime (erts), but not guarenteed to be portable between different Linux distributions or versions.

In the release directory, `bin/bord` is the main command.

To start an Erlang REPL shell with all the `brod` APIs to play with, execute `bin/brod-i console`.

## TODO

- [ ] Use json module from Erlang/OTP 27 (replace jsone)
- [ ] Switch to https://github.com/jcomellas/getopt (replace optdoc)
- [ ] Eunit
- [ ] Dialyzer
- [ ] GitHub Action
- [ ] Publish Docker image
