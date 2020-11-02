use std::cmp::Ordering;
use tikv_util::buffer_vec::BufferVec;

#[derive(Clone, Debug)]
pub struct Set {
    // TODO: Optimize me using Arc or others to prevent deep clone
    data: BufferVec,

    // TIDB makes sure there will be no more than 64 bits
    // https://github.com/pingcap/tidb/blob/master/types/set.go
    value: usize,
}

impl ToString for Set {
    fn to_string(&self) -> String {
        let mut buf: Vec<u8> = Vec::new();
        if self.value > 0 {
            for idx in 0..self.data.len() {
                if self.value & (1 << idx) == 0 {
                    continue;
                }

                if !buf.is_empty() {
                    buf.push(b',');
                }
                buf.extend_from_slice(&self.data[idx]);
            }
        }

        // TODO: Check the requirements and intentions of to_string usage.
        String::from_utf8_lossy(buf.as_slice()).to_string()
    }
}

impl Eq for Set {}

impl PartialEq for Set {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl Ord for Set {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for Set {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

impl crate::codec::data_type::AsMySQLBool for Set {
    #[inline]
    fn as_mysql_bool(
        &self,
        _context: &mut crate::expr::EvalContext,
    ) -> tidb_query_common::error::Result<bool> {
        Ok(self.value > 0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SetRef<'a> {
    data: &'a BufferVec,
    value: usize,
}

impl<'a> Eq for SetRef<'a> {}

impl<'a> PartialEq for SetRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<'a> Ord for SetRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<'a> PartialOrd for SetRef<'a> {
    fn partial_cmp(&self, right: &Self) -> Option<Ordering> {
        Some(self.cmp(right))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let cases = vec![
            (vec!["a", "b", "c"], 0b001, "a"),
            (vec!["a", "b", "c"], 0b011, "a,b"),
            (vec!["a", "b", "c"], 0b111, "a,b,c"),
            (vec!["a", "b", "c"], 0b101, "a,c"),
        ];

        for (data, value, expect) in cases {
            let mut s = Set {
                data: BufferVec::new(),
                value,
            };
            for v in data {
                s.data.push(v);
            }

            assert_eq!(s.to_string(), expect.to_string())
        }
    }
}
