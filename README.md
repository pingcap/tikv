# TiKV

[![Build Status](https://circleci.com/gh/pingcap/tikv.svg?style=shield&circle-token=36bab0a8e43edb0941b31c38557d2d9d0d58f708)](https://circleci.com/gh/pingcap/tikv) [![Coverage Status](https://codecov.io/gh/pingcap/tikv/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/tikv) ![GitHub release](https://img.shields.io/github/release/pingcap/tikv.svg)

TiKV (The pronunciation is: /'taɪkeɪvi:/ tai-K-V, etymology: titanium) is a distributed Key-Value database powered by Rust and Raft. TiKV is based on the design of Google Spanner and HBase, but is much simpler without dependency on any distributed file system.

With the implementation of the Raft consensus algorithm in Rust and consensus state stored in RocksDB, TiKV guarantees data consistency. [Placement Driver (PD)](https://github.com/pingcap/pd/), which is introduced to implement sharding, enables automatic data migration. The transaction model is similar to Google's Percolator with some performance improvements. TiKV also provides snapshot isolation (SI), snapshot isolation with lock (SQL: `SELECT ... FOR UPDATE`), and externally consistent reads and writes in distributed transactions.

TiKV has the following key features:

- **Geo-Replication**

    TiKV uses [Raft](http://raft.github.io/) and the Placement Driver to support Geo-Replication.

- **Horizontal scalability**

    With PD and carefully designed Raft groups, TiKV excels in horizontal scalability and can easily scale to 100+ TBs of data.

- **Consistent distributed transactions**

    Similar to Google's Spanner, TiKV supports externally-consistent distributed transactions.

- **Coprocessor support**

    Similar to Hbase, TiKV implements a coprocessor framework to support distributed computing.

- **Cooperates with [TiDB](https://github.com/pingcap/tidb)**

    Thanks to the internal optimization, TiKV and TiDB can work together to be a compelling database solution with high horizontal scalability, externally-consistent transactions, support for RDBMS, and NoSQL design patterns.

## TiKV Adopters

You can view the list of [TiKV Adopters](docs/adopters.md).

## TiKV Roadmap

You can see the [TiKV Roadmap](docs/ROADMAP.md).

## TiKV software stack

![The TiKV software stack](images/tikv_stack.png)

- **Placement Driver:** PD is the cluster manager of TiKV, which periodically checks replication constraints to balance load and data automatically.
- **Store:** There is a RocksDB within each Store and it stores data into the local disk.
- **Region:** Region is the basic unit of Key-Value data movement. Each Region is replicated to multiple Nodes. These multiple replicas form a Raft group.
- **Node:** A physical node in the cluster. Within each node, there are one or more Stores. Within each Store, there are many Regions.

When a node starts, the metadata of the Node, Store and Region are recorded into PD. The status of each Region and Store is reported to PD regularly.

## Try TiKV

TiKV is a component of the [TiDB project](https://github.com/pingcap/tidb). To run TiKV you must build and run it with PD, which is used to manage the cluster. You can use TiKV together with TiDB. You can also use TiKV separately.

We provide multiple deployment methods, but it is recommended to use the Ansible deployment for the production environment. The TiKV documentation is available on [TiKV's wiki page](https://github.com/tikv/tikv/wiki/TiKV-Documentation).

### Testing deployment

- [Try TiKV and TiDB](https://www.pingcap.com/docs/op-guide/docker-compose/)

    You can use [`tidb-docker-compose`](https://github.com/pingcap/tidb-docker-compose/) to quickly test TiKV and TiDB on a single machine. This is the easiest way. For other ways, see [TiDB documentation](https://www.pingcap.com/docs/).

- Try TiKV separately
    - [Deploy TiKV Using Docker Compose](https://www.pingcap.com/docs/tikv/deploy-tikv-docker-compose/): To quickly test TiKV separately without TiDB using [`tidb-docker-compose`](https://github.com/pingcap/tidb-docker-compose/) on a single machine
    - [Deploy TiKV Using Docker](https://www.pingcap.com/docs/tikv/deploy-tikv-using-docker/): To deploy a multi-node TiKV testing cluster using Docker
    - [Deploy TiKV Using Binary Files](https://www.pingcap.com/docs/tikv/deploy-tikv-using-binary/): To deploy a TiKV cluster using binary files on a single node or on multiple nodes

### Production deployment

For the production environment, use [Ansible](https://github.com/pingcap/tidb-ansible) to deploy the cluster.

- [Deploy TiDB Using Ansible](https://www.pingcap.com/docs/op-guide/ansible-deployment/)
- [Deploy TiKV separately Using Ansible](https://www.pingcap.com/docs/tikv/deploy-tikv-using-ansible/)

## Client drivers

Currently, the only interface to TiKV is the [TiDB Go client](https://github.com/pingcap/tidb/tree/master/store/tikv) and the [TiSpark Java client](https://github.com/pingcap/tispark/tree/master/tikv-client/src/main/java/com/pingcap/tikv).

If you want to try the Go client, see [Try Two Types of APIs](https://www.pingcap.com/docs/tikv/go-client-api/).

## Setting up a development workspace

The TiKV codebase is primarily written in Rust, but has components written in C++ (RocksDB) and Go (gRPC). To provide consistency and avoid opinion-based arguments, we make extensive use of linters and automated formatting tools. Additionally, due to Rust's youth we are currently utilizing nightly builds which provide access to many useful features.

### Checking your prerequisites

To build TiKV you'll need to at least have the following installed:

* `git` - Version control
* `rustup` - Rust toolchain manager
* `awk` - Pattern scanning/processing language
* `cmake` - Build tool (required for gRPC)
* `go` - Programming language (required for gRPC)
* `make` - Build tool (run common workflows)
* `clang` or `gcc` - C compiler toolchain

### Getting the repository

```
git clone https://github.com/tikv/tikv.git
cd tikv
# Future instructions assume you are in this repository
```

### Configuring your Rust toolchain

`rustup` is an official toolchain manager for Rust, similar to `rvm` or `rbenv` from the Ruby world.

TiKV uses the version of the Rust toolchain specified in `rust-toolchain`. `rustup` and `cargo` will automatically utilize this file. We also make use of the `rustfmt` and `clippy` components.

```bash
rustup component add rustfmt-preview
```

### Building & testing

While TiKV includes a `Makefile` with common workflows, you are also able to use `cargo` as you would in a normal Rust project.

At this point, you can build TiKV:

```bash
make build
```

During interactive development, you may prefer using `cargo check`, which will do parse, borrow check, and lint run on your code, but not actually compile it. It is particularly handy alongside `cargo-watch` which will run a command each time you change a file.

```bash
cargo install cargo-watch
cargo watch -s "cargo check"
```

When you're ready to test out your changes, use the `dev` task. It will format your codebase, build with `clippy` enabled, and run tests. This should run without failure before you create a PR.

```bash
make dev
```

You can run the full test suite locally, or just run a specific test:

```bash
# Run the full suite
make test
# Run a specific test
cargo test $TESTNAME
```

Our CI systems automatically test all the pull requests, so making sure the full suite passes the test before creating your PR is not strictly required. **All merged PRs must have passed CI test.**

### Getting the rest of the system working

To get other components ([TiDB](https://github.com/pingcap/tidb) and [PD](https://github.com/pingcap/pd)) working, we suggest you follow the [development guide](https://github.com/pingcap/docs/blob/master/dev-guide/development.md), because you need the `pd-server` at least to work alongside `tikv-server` for integration level testing.

### Configuration

Read our configuration guide to learn about various [configuration options](https://github.com/pingcap/docs/blob/master/op-guide/configuration.md).

## Contributing

Contributions are welcome! See [CONTRIBUTING](./CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

For beginners, we have prepared many suitable tasks for you. Checkout our [Help Wanted issues](https://github.com/tikv/tikv/issues?q=is%3Aissue+is%3Aopen+label%3A%22S%3A+HelpWanted%22) for a list, in which we have also marked the difficulty level.

If you are planning something big, for example, relates to multiple components or changes current behaviors, make sure to open an issue to discuss with us before going on.

The TiKV team actively develops and maintains a bunch of dependencies used in TiKV, which you may be also interested in:

- [rust-prometheus](https://github.com/pingcap/rust-prometheus): The Prometheus client for Rust, our metrics collecting and reporting library
- [rust-rocksdb](https://github.com/pingcap/rust-rocksdb): Our RocksDB binding and wrapper for Rust
- [raft-rs](https://github.com/pingcap/raft-rs): The Raft distributed consensus algorithm implemented in Rust
- [grpc-rs](https://github.com/pingcap/grpc-rs): The gRPC library for Rust built on the gRPC C Core library and Rust Futures
- [fail-rs](https://github.com/pingcap/fail-rs): Fail points for Rust

## License

TiKV is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.

## Acknowledgments

- Thanks [etcd](https://github.com/coreos/etcd) for providing some great open source tools.
- Thanks [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
- Thanks [mio](https://github.com/carllerche/mio) for providing metal I/O library for Rust.
- Thanks [rust-clippy](https://github.com/Manishearth/rust-clippy). We do love the great project.
