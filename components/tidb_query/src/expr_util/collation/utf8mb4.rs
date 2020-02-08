// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::hash::Hasher;
use std::str;

use codec::prelude::*;

use super::{Collator, DecodeError, DecodeOrWriteError};

const GENERAL_CI_PLANE_00: [u16; 256] = [
    0x0000, 0x0001, 0x0002, 0x0003, 0x0004, 0x0005, 0x0006, 0x0007, 0x0008, 0x0009, 0x000A, 0x000B,
    0x000C, 0x000D, 0x000E, 0x000F, 0x0010, 0x0011, 0x0012, 0x0013, 0x0014, 0x0015, 0x0016, 0x0017,
    0x0018, 0x0019, 0x001A, 0x001B, 0x001C, 0x001D, 0x001E, 0x001F, 0x0020, 0x0021, 0x0022, 0x0023,
    0x0024, 0x0025, 0x0026, 0x0027, 0x0028, 0x0029, 0x002A, 0x002B, 0x002C, 0x002D, 0x002E, 0x002F,
    0x0030, 0x0031, 0x0032, 0x0033, 0x0034, 0x0035, 0x0036, 0x0037, 0x0038, 0x0039, 0x003A, 0x003B,
    0x003C, 0x003D, 0x003E, 0x003F, 0x0040, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047,
    0x0048, 0x0049, 0x004A, 0x004B, 0x004C, 0x004D, 0x004E, 0x004F, 0x0050, 0x0051, 0x0052, 0x0053,
    0x0054, 0x0055, 0x0056, 0x0057, 0x0058, 0x0059, 0x005A, 0x005B, 0x005C, 0x005D, 0x005E, 0x005F,
    0x0060, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047, 0x0048, 0x0049, 0x004A, 0x004B,
    0x004C, 0x004D, 0x004E, 0x004F, 0x0050, 0x0051, 0x0052, 0x0053, 0x0054, 0x0055, 0x0056, 0x0057,
    0x0058, 0x0059, 0x005A, 0x007B, 0x007C, 0x007D, 0x007E, 0x007F, 0x0080, 0x0081, 0x0082, 0x0083,
    0x0084, 0x0085, 0x0086, 0x0087, 0x0088, 0x0089, 0x008A, 0x008B, 0x008C, 0x008D, 0x008E, 0x008F,
    0x0090, 0x0091, 0x0092, 0x0093, 0x0094, 0x0095, 0x0096, 0x0097, 0x0098, 0x0099, 0x009A, 0x009B,
    0x009C, 0x009D, 0x009E, 0x009F, 0x00A0, 0x00A1, 0x00A2, 0x00A3, 0x00A4, 0x00A5, 0x00A6, 0x00A7,
    0x00A8, 0x00A9, 0x00AA, 0x00AB, 0x00AC, 0x00AD, 0x00AE, 0x00AF, 0x00B0, 0x00B1, 0x00B2, 0x00B3,
    0x00B4, 0x039C, 0x00B6, 0x00B7, 0x00B8, 0x00B9, 0x00BA, 0x00BB, 0x00BC, 0x00BD, 0x00BE, 0x00BF,
    0x0041, 0x0041, 0x0041, 0x0041, 0x0041, 0x0041, 0x00C6, 0x0043, 0x0045, 0x0045, 0x0045, 0x0045,
    0x0049, 0x0049, 0x0049, 0x0049, 0x00D0, 0x004E, 0x004F, 0x004F, 0x004F, 0x004F, 0x004F, 0x00D7,
    0x00D8, 0x0055, 0x0055, 0x0055, 0x0055, 0x0059, 0x00DE, 0x0053, 0x0041, 0x0041, 0x0041, 0x0041,
    0x0041, 0x0041, 0x00C6, 0x0043, 0x0045, 0x0045, 0x0045, 0x0045, 0x0049, 0x0049, 0x0049, 0x0049,
    0x00D0, 0x004E, 0x004F, 0x004F, 0x004F, 0x004F, 0x004F, 0x00F7, 0x00D8, 0x0055, 0x0055, 0x0055,
    0x0055, 0x0059, 0x00DE, 0x0059,
];

const GENERAL_CI_PLANE_01: [u16; 256] = [
    0x0041, 0x0041, 0x0041, 0x0041, 0x0041, 0x0041, 0x0043, 0x0043, 0x0043, 0x0043, 0x0043, 0x0043,
    0x0043, 0x0043, 0x0044, 0x0044, 0x0110, 0x0110, 0x0045, 0x0045, 0x0045, 0x0045, 0x0045, 0x0045,
    0x0045, 0x0045, 0x0045, 0x0045, 0x0047, 0x0047, 0x0047, 0x0047, 0x0047, 0x0047, 0x0047, 0x0047,
    0x0048, 0x0048, 0x0126, 0x0126, 0x0049, 0x0049, 0x0049, 0x0049, 0x0049, 0x0049, 0x0049, 0x0049,
    0x0049, 0x0049, 0x0132, 0x0132, 0x004A, 0x004A, 0x004B, 0x004B, 0x0138, 0x004C, 0x004C, 0x004C,
    0x004C, 0x004C, 0x004C, 0x013F, 0x013F, 0x0141, 0x0141, 0x004E, 0x004E, 0x004E, 0x004E, 0x004E,
    0x004E, 0x0149, 0x014A, 0x014A, 0x004F, 0x004F, 0x004F, 0x004F, 0x004F, 0x004F, 0x0152, 0x0152,
    0x0052, 0x0052, 0x0052, 0x0052, 0x0052, 0x0052, 0x0053, 0x0053, 0x0053, 0x0053, 0x0053, 0x0053,
    0x0053, 0x0053, 0x0054, 0x0054, 0x0054, 0x0054, 0x0166, 0x0166, 0x0055, 0x0055, 0x0055, 0x0055,
    0x0055, 0x0055, 0x0055, 0x0055, 0x0055, 0x0055, 0x0055, 0x0055, 0x0057, 0x0057, 0x0059, 0x0059,
    0x0059, 0x005A, 0x005A, 0x005A, 0x005A, 0x005A, 0x005A, 0x0053, 0x0180, 0x0181, 0x0182, 0x0182,
    0x0184, 0x0184, 0x0186, 0x0187, 0x0187, 0x0189, 0x018A, 0x018B, 0x018B, 0x018D, 0x018E, 0x018F,
    0x0190, 0x0191, 0x0191, 0x0193, 0x0194, 0x01F6, 0x0196, 0x0197, 0x0198, 0x0198, 0x019A, 0x019B,
    0x019C, 0x019D, 0x019E, 0x019F, 0x004F, 0x004F, 0x01A2, 0x01A2, 0x01A4, 0x01A4, 0x01A6, 0x01A7,
    0x01A7, 0x01A9, 0x01AA, 0x01AB, 0x01AC, 0x01AC, 0x01AE, 0x0055, 0x0055, 0x01B1, 0x01B2, 0x01B3,
    0x01B3, 0x01B5, 0x01B5, 0x01B7, 0x01B8, 0x01B8, 0x01BA, 0x01BB, 0x01BC, 0x01BC, 0x01BE, 0x01F7,
    0x01C0, 0x01C1, 0x01C2, 0x01C3, 0x01C4, 0x01C4, 0x01C4, 0x01C7, 0x01C7, 0x01C7, 0x01CA, 0x01CA,
    0x01CA, 0x0041, 0x0041, 0x0049, 0x0049, 0x004F, 0x004F, 0x0055, 0x0055, 0x0055, 0x0055, 0x0055,
    0x0055, 0x0055, 0x0055, 0x0055, 0x0055, 0x018E, 0x0041, 0x0041, 0x0041, 0x0041, 0x00C6, 0x00C6,
    0x01E4, 0x01E4, 0x0047, 0x0047, 0x004B, 0x004B, 0x004F, 0x004F, 0x004F, 0x004F, 0x01B7, 0x01B7,
    0x004A, 0x01F1, 0x01F1, 0x01F1, 0x0047, 0x0047, 0x01F6, 0x01F7, 0x004E, 0x004E, 0x0041, 0x0041,
    0x00C6, 0x00C6, 0x00D8, 0x00D8,
];

pub struct CollatorUtf8Mb4GeneralCi;

fn general_ci_weight(c: char) -> u16 {
    let mut plane = &GENERAL_CI_PLANE_00;
    let r = c as u32;
    if (r >> 8) > 0 {
        plane = &GENERAL_CI_PLANE_01;
    }
    plane[(r & 0xFF) as usize]
}

impl Collator for CollatorUtf8Mb4GeneralCi {
    fn write_sort_key<W: BufferWriter>(
        bstr: &[u8],
        writer: &mut W,
    ) -> Result<usize, DecodeOrWriteError> {
        let s = str::from_utf8(bstr).map_err(DecodeError::from)?;
        let mut n = 0;
        for ch in s.chars() {
            writer.write_u16_be(general_ci_weight(ch))?;
            n += 1;
        }
        Ok(n * std::mem::size_of::<u16>())
    }

    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering, DecodeError> {
        let sa = str::from_utf8(a)?;
        let sb = str::from_utf8(b)?;
        let sa_chars = sa.chars();
        let sb_chars = sb.chars();
        Ok(sa_chars.cmp_by(sb_chars, |a, b| {
            general_ci_weight(a).cmp(&general_ci_weight(b))
        }))
    }

    fn sort_hash<H: Hasher>(bstr: &[u8], state: &mut H) -> Result<(), DecodeError> {
        use std::hash::Hash;

        bstr.len().hash(state);
        let s = str::from_utf8(bstr)?;
        for ch in s.chars() {
            state.write_u16(general_ci_weight(ch));
        }
        Ok(())
    }
}

pub struct CollatorUtf8Mb4Bin;

impl Collator for CollatorUtf8Mb4Bin {
    fn write_sort_key<W: BufferWriter>(
        bstr: &[u8],
        writer: &mut W,
    ) -> Result<usize, DecodeOrWriteError> {
        str::from_utf8(bstr).map_err(DecodeError::from)?;
        writer.write_bytes(bstr)?;
        Ok(bstr.len())
    }

    fn sort_compare(a: &[u8], b: &[u8]) -> Result<Ordering, DecodeError> {
        str::from_utf8(a)?;
        str::from_utf8(b)?;
        Ok(a.cmp(b))
    }

    fn sort_hash<H: Hasher>(bstr: &[u8], state: &mut H) -> Result<(), DecodeError> {
        str::from_utf8(bstr)?;
        state.write(bstr);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::string_lit_as_bytes)]
    fn test_general_ci() {
        let s1 = "cAfe".as_bytes();
        let s2 = "café".as_bytes();
        type C = CollatorUtf8Mb4GeneralCi;
        assert_eq!(C::sort_key(s1).unwrap(), C::sort_key(s2).unwrap());
        assert_eq!(C::sort_compare(s1, s2).unwrap(), Ordering::Equal);
    }
}
