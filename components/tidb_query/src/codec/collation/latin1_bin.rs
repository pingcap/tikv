// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use super::*;
use bstr::{ByteSlice, B};

/// Collator for latin1_bin collation with padding behavior (trims right spaces).
#[derive(Debug)]
pub struct CollatorLatin1Bin;

impl Collator for CollatorLatin1Bin {
    type Charset = CharsetBinary;

    #[inline]
    fn validate(_bstr: &[u8]) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn write_sort_key<W: BufferWriter>(writer: &mut W, bstr: &[u8]) -> Result<usize> {
        let s = B(bstr).trim_end_with(|c| c == TRIM_PADDING_SPACE);
        writer.write_bytes(s)?;
        Ok(s.len())
    }

    #[inline]
    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering> {
        Ok(B(a)
            .trim_end_with(|c| c == TRIM_PADDING_SPACE)
            .cmp(B(b).trim_end_with(|c| c == TRIM_PADDING_SPACE)))
    }

    #[inline]
    fn sort_hash<H: Hasher>(state: &mut H, bstr: &[u8]) -> Result<()> {
        B(bstr)
            .trim_end_with(|c| c == TRIM_PADDING_SPACE)
            .hash(state);
        Ok(())
    }
}
