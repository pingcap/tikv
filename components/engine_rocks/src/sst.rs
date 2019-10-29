// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Result, Iterable, SstExt, SstReader};
use engine_traits::{SeekMode, SeekKey, Iterator};
use engine_traits::IterOptions;
use crate::engine::RocksEngine;
use rocksdb::{SstFileReader, ColumnFamilyOptions};

impl SstExt for RocksEngine {
    type SstReader = RocksSstReader;
}

pub struct RocksSstReader {
    reader: SstFileReader,
}

impl SstReader for RocksSstReader {
    fn open(path: &str) -> Result<Self> {
        let mut reader = SstFileReader::new(ColumnFamilyOptions::new());
        reader.open(path)?;
        Ok(RocksSstReader { reader })
    }
    fn verify_checksum(&self) -> Result<()> {
        self.reader.verify_checksum()?;
        Ok(())
    }
    fn iter(&self) -> Self::Iterator {
        panic!()
    }
}

impl Iterable for RocksSstReader {
    type Iterator = RocksSstIterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iterator> {
        panic!()
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct RocksSstIterator;

impl Iterator for RocksSstIterator {
    fn seek(&mut self, key: SeekKey) -> bool {
        panic!()
    }

    fn seek_for_prev(&mut self, key: SeekKey) -> bool {
        panic!()
    }

    fn prev(&mut self) -> bool {
        panic!()
    }

    fn next(&mut self) -> bool {
        panic!()
    }

    fn key(&self) -> Result<&[u8]> {
        panic!()
    }

    fn value(&self) -> Result<&[u8]> {
        panic!()
    }

    fn valid(&self) -> bool {
        panic!()
    }

    fn status(&self) -> Result<()> {
        panic!()
    }
}
