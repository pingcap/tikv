// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryFrom;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::expression::FieldType;

use super::BufferVec;
use crate::coprocessor::codec::data_type::VectorValue;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::codec::Result;

/// A container stores an array of datums, which can be either raw (not decoded), or decoded into
/// the `VectorValue` type.
///
/// TODO:
/// Since currently the data format in response can be the same as in storage, we use this structure
/// to avoid unnecessary repeated serialization / deserialization. In future, Coprocessor will
/// respond all data in Arrow format which is different to the format in storage. At that time,
/// this structure is no longer useful and should be removed.
#[derive(Clone, Debug)]
pub enum LazyBatchColumn {
    Raw(BufferVec),
    Decoded(VectorValue),
}

impl From<VectorValue> for LazyBatchColumn {
    #[inline]
    fn from(vec: VectorValue) -> Self {
        LazyBatchColumn::Decoded(vec)
    }
}

impl LazyBatchColumn {
    /// Creates a new `LazyBatchColumn::Raw` with specified capacity.
    #[inline]
    pub fn raw_with_capacity(capacity: usize) -> Self {
        use codec::number::MAX_VARINT64_LENGTH;
        // We assume that each element *may* has a size of MAX_VAR_INT_LEN + Datum Flag (1 byte).
        LazyBatchColumn::Raw(BufferVec::with_capacity(
            capacity,
            capacity * (MAX_VARINT64_LENGTH + 1),
        ))
    }

    /// Creates a new `LazyBatchColumn::Decoded` with specified capacity and eval type.
    #[inline]
    pub fn decoded_with_capacity_and_tp(capacity: usize, eval_tp: EvalType) -> Self {
        LazyBatchColumn::Decoded(VectorValue::with_capacity(capacity, eval_tp))
    }

    #[inline]
    pub fn is_raw(&self) -> bool {
        match self {
            LazyBatchColumn::Raw(_) => true,
            LazyBatchColumn::Decoded(_) => false,
        }
    }

    #[inline]
    pub fn is_decoded(&self) -> bool {
        match self {
            LazyBatchColumn::Raw(_) => false,
            LazyBatchColumn::Decoded(_) => true,
        }
    }

    #[inline]
    pub fn decoded(&self) -> &VectorValue {
        match self {
            LazyBatchColumn::Raw(_) => panic!("LazyBatchColumn is not decoded"),
            LazyBatchColumn::Decoded(v) => v,
        }
    }

    #[inline]
    pub fn mut_decoded(&mut self) -> &mut VectorValue {
        match self {
            LazyBatchColumn::Raw(_) => panic!("LazyBatchColumn is not decoded"),
            LazyBatchColumn::Decoded(v) => v,
        }
    }

    #[inline]
    pub fn raw(&self) -> &BufferVec {
        match self {
            LazyBatchColumn::Raw(v) => v,
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn mut_raw(&mut self) -> &mut BufferVec {
        match self {
            LazyBatchColumn::Raw(v) => v,
            LazyBatchColumn::Decoded(_) => panic!("LazyBatchColumn is already decoded"),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LazyBatchColumn::Raw(v) => v.len(),
            LazyBatchColumn::Decoded(v) => v.len(),
        }
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        match self {
            LazyBatchColumn::Raw(v) => v.truncate(len),
            LazyBatchColumn::Decoded(v) => v.truncate(len),
        };
    }

    #[inline]
    pub fn clear(&mut self) {
        match self {
            LazyBatchColumn::Raw(v) => v.clear(),
            LazyBatchColumn::Decoded(v) => v.clear(),
        };
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match self {
            LazyBatchColumn::Raw(v) => v.capacity(),
            LazyBatchColumn::Decoded(v) => v.capacity(),
        }
    }

    /// Retains the elements according to a boolean array.
    ///
    /// # Panics
    ///
    /// Panics if `retain_arr` is not long enough.
    #[inline]
    pub fn retain_by_array(&mut self, retain_arr: &[bool]) {
        match self {
            LazyBatchColumn::Raw(v) => v.retain_by_array(retain_arr),
            LazyBatchColumn::Decoded(v) => v.retain_by_array(retain_arr),
        }
    }

    /// Decodes this column in place if the column is not decoded.
    ///
    /// The field type is needed because we use the same `DateTime` structure when handling
    /// Date, Time or Timestamp.
    // TODO: Maybe it's a better idea to assign different eval types for different date types.
    pub fn ensure_decoded(&mut self, time_zone: &Tz, field_type: &FieldType) -> Result<()> {
        if self.is_decoded() {
            return Ok(());
        }

        let eval_type = box_try!(EvalType::try_from(field_type.tp()));

        let mut decoded_column = VectorValue::with_capacity(self.capacity(), eval_type);
        {
            for value in self.raw().iter() {
                decoded_column.push_datum(value, time_zone, field_type)?;
            }
        }
        *self = LazyBatchColumn::Decoded(decoded_column);

        Ok(())
    }

    /// Returns maximum encoded size.
    pub fn maximum_encoded_size(&self) -> Result<usize> {
        match self {
            LazyBatchColumn::Raw(v) => Ok(v.total_len()),
            LazyBatchColumn::Decoded(v) => v.maximum_encoded_size(),
        }
    }

    /// Encodes into binary format.
    // FIXME: Use BufferWriter.
    pub fn encode(
        &self,
        row_index: usize,
        field_type: &FieldType,
        output: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            LazyBatchColumn::Raw(v) => {
                output.extend_from_slice(&v[row_index]);
                Ok(())
            }
            LazyBatchColumn::Decoded(ref v) => v.encode(row_index, field_type, output),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::coprocessor::codec::datum::{Datum, DatumEncoder};

    #[test]
    fn test_lazy_batch_column_clone() {
        use cop_datatype::FieldTypeTp;

        let mut col = LazyBatchColumn::raw_with_capacity(5);
        assert!(col.is_raw());
        assert_eq!(col.len(), 0);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.raw().len(), 0);
        {
            // Clone empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.raw().len(), 0);
        }
        {
            // Empty raw to empty decoded.
            let mut col = col.clone();
            col.ensure_decoded(&Tz::utc(), &FieldTypeTp::Long.into())
                .unwrap();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 0);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.decoded().as_int_slice(), &[]);
            {
                // Clone empty decoded LazyBatchColumn.
                let col = col.clone();
                assert!(col.is_decoded());
                assert_eq!(col.len(), 0);
                assert_eq!(col.capacity(), 5);
                assert_eq!(col.decoded().as_int_slice(), &[]);
            }
        }

        let mut datum_raw_1 = Vec::new();
        DatumEncoder::encode(&mut datum_raw_1, &[Datum::U64(32)], false).unwrap();
        col.mut_raw().push(&datum_raw_1);

        let mut datum_raw_2 = Vec::new();
        DatumEncoder::encode(&mut datum_raw_2, &[Datum::U64(7)], true).unwrap();
        col.mut_raw().push(&datum_raw_2);

        assert!(col.is_raw());
        assert_eq!(col.len(), 2);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.raw().len(), 2);
        assert_eq!(&col.raw()[0], datum_raw_1.as_slice());
        assert_eq!(&col.raw()[1], datum_raw_2.as_slice());
        {
            // Clone non-empty raw LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_raw());
            assert_eq!(col.len(), 2);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.raw().len(), 2);
            assert_eq!(&col.raw()[0], datum_raw_1.as_slice());
            assert_eq!(&col.raw()[1], datum_raw_2.as_slice());
        }
        // Non-empty raw to non-empty decoded.
        col.ensure_decoded(&Tz::utc(), &FieldTypeTp::Long.into())
            .unwrap();
        assert!(col.is_decoded());
        assert_eq!(col.len(), 2);
        assert_eq!(col.capacity(), 5);
        assert_eq!(col.decoded().as_int_slice(), &[Some(32), Some(7)]);
        {
            // Clone non-empty decoded LazyBatchColumn.
            let col = col.clone();
            assert!(col.is_decoded());
            assert_eq!(col.len(), 2);
            assert_eq!(col.capacity(), 5);
            assert_eq!(col.decoded().as_int_slice(), &[Some(32), Some(7)]);
        }
    }
}

#[cfg(test)]
mod benches {
    use super::*;

    #[bench]
    fn bench_lazy_batch_column_push_raw_4bytes(b: &mut test::Bencher) {
        let mut column = LazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 4];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.mut_raw().push(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// Bench performance of cloning a decoded column.
    #[bench]
    fn bench_lazy_batch_column_clone_decoded(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
        use cop_datatype::FieldTypeTp;

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.mut_raw().push(datum_raw.as_slice());
        }

        column
            .ensure_decoded(&Tz::utc(), &FieldTypeTp::LongLong.into())
            .unwrap();

        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }

    /// Bench performance of decoding a raw batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
        use cop_datatype::FieldTypeTp;

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.mut_raw().push(datum_raw.as_slice());
        }

        let ft = FieldTypeTp::LongLong.into();
        let tz = Tz::utc();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.ensure_decoded(test::black_box(&tz), test::black_box(&ft))
                .unwrap();
            test::black_box(&col);
        });
    }

    /// Bench performance of decoding a decoded lazy batch column.
    ///
    /// Note that there is a clone in the bench suite, whose cost should be excluded.
    #[bench]
    fn bench_lazy_batch_column_clone_and_decode_decoded(b: &mut test::Bencher) {
        use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
        use cop_datatype::FieldTypeTp;

        let mut column = LazyBatchColumn::raw_with_capacity(1000);

        let mut datum_raw: Vec<u8> = Vec::new();
        DatumEncoder::encode(&mut datum_raw, &[Datum::U64(0xDEADBEEF)], true).unwrap();

        for _ in 0..1000 {
            column.mut_raw().push(datum_raw.as_slice());
        }

        let ft = FieldTypeTp::LongLong.into();
        let tz = Tz::utc();

        column.ensure_decoded(&tz, &ft).unwrap();

        b.iter(|| {
            let mut col = test::black_box(&column).clone();
            col.ensure_decoded(test::black_box(&tz), test::black_box(&ft))
                .unwrap();
            test::black_box(&col);
        });
    }

    /// A vector based LazyBatchColumn
    #[derive(Clone)]
    struct VectorLazyBatchColumn(Vec<Vec<u8>>);

    impl VectorLazyBatchColumn {
        #[inline]
        pub fn raw_with_capacity(capacity: usize) -> Self {
            VectorLazyBatchColumn(Vec::with_capacity(capacity))
        }

        #[inline]
        pub fn clear(&mut self) {
            self.0.clear();
        }

        #[inline]
        pub fn push_raw(&mut self, raw_datum: &[u8]) {
            self.0.push(raw_datum.to_vec());
        }
    }

    /// Bench performance of pushing 10 bytes to a vector based LazyBatchColumn.
    #[bench]
    fn bench_lazy_batch_column_by_vec_push_raw_10bytes(b: &mut test::Bencher) {
        let mut column = VectorLazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 10];
        b.iter(|| {
            let column = test::black_box(&mut column);
            for _ in 0..1000 {
                column.push_raw(test::black_box(&val))
            }
            test::black_box(&column);
            column.clear();
            test::black_box(&column);
        });
    }

    /// Bench performance of cloning a raw vector based LazyBatchColumn.
    #[bench]
    fn bench_lazy_batch_column_by_vec_clone(b: &mut test::Bencher) {
        let mut column = VectorLazyBatchColumn::raw_with_capacity(1000);
        let val = vec![0; 10];
        for _ in 0..1000 {
            column.push_raw(&val);
        }
        b.iter(|| {
            test::black_box(test::black_box(&column).clone());
        });
    }
}
