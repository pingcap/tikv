// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Engine:",

    ENGINE => ("Engine", "", ""),
    NOT_IN_RANGE => ("NotInRange", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    IO => ("IO", "", ""),
    CFNAME => ("CFName", "", ""),
    CODEC => ("Codec", "", ""),

    UNDETERMINED => ("Undetermined", "", "")
);
