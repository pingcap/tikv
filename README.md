## TiKV is a distributed KV database powered by Rust.


[![Build Status](https://travis-ci.org/pingcap/tikv.svg?branch=master)](https://travis-ci.org/pingcap/tikv)


TiKV is inspired by the design of Google Spanner and HBase, but much simpler (Doesn't depend on any distributed file system).

- __Geo-Replication__  
TiKV uses Raft to support Geo-Replication. We have ported etcd's raft implementation to Rust.


- __Horizontal scalability__  


- __Consistent distributed transactions__  


- __Coprocessor support__  


- __Working with [TiDB](https://github.com/pingcap/tidb)__  

### Required rust version

This project requires rust nightly, otherwise it can't build.

```
# 1. Install Rust Nightly
curl -s https://static.rust-lang.org/rustup.sh | sh -s -- --channel=nightly

# 2. Install RocksDB Library

wget https://github.com/facebook/rocksdb/archive/rocksdb-4.3.1.tar.gz
tar -xzvf rocksdb-4.3.1.tar.gz

# OR
git clone https://github.com/facebook/rocksdb.git

cd rocksdb
make shared_lib
cp librocksdb.so* /usr/lib64/

# 3. Install TiKV
git clone https://github.com/pingcap/tikv.git
cd tikv
make

# 4. Have A Fun!
```

## Acknowledgments
- Thanks [etcd](https://github.com/coreos/etcd) for providing some great open source tools.
- Thanks [RocksDB](https://github.com/facebook/rocksdb) for their powerful storage engines.
- Thanks [mio](https://github.com/carllerche/mio) for providing metal IO library for Rust.
- Thanks [rust-clippy](https://github.com/Manishearth/rust-clippy). We do love the great project. 
