mod meta;
mod codec;
mod txn;

pub use self::txn::MvccTxn;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: ::storage::engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        ProtoBuf(err: ::protobuf::error::ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: ::util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        KeyIsLocked {primary: ::storage::Key, ts: u64} {
            description("key is locked (backoff or cleanup)")
            display("key is locked (backoff or cleanup) {:?}@{}", primary, ts)
        }
        TxnLockNotFound {description("txn lock not found")}
        WriteConflict {description("write conflict")}
        KeyVersion {description("bad format key(version)")}
        AlreadyCommitted {description("txn already committed")}
        AlreadyRollbacked {description("txn already rollbacked")}
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
