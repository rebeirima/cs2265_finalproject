## Server

### Build
```shell
cargo build --release
```

### Run
```
./target/release/lsm-tree [--port port] [--data-dir dir]
```

## Client

### Build
```shell
cd lsm-tree-client; cargo build --release
```

### Run
```
./target/release/lsm-tree-client [--port port]
```

## Useful commands

- Record diskio usage and stdout of server:

```shell
./target/release/lsm-tree > out.txt &; sudo fs_usage -w -f filesys $last_pid
```

- Feed `generator` to client:

```
cd lsm-tree-client; ./generator/generator --puts 1000000 --gets 10000 --deletes 20000 --gets-misses-ratio 0.3 --gets-skewness 0.2 --gaussian-ranges | ./target/release/lsm-tree-client
```