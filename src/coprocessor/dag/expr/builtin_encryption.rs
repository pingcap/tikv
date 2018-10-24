// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::Cow;

use super::{EvalContext, Result, ScalarFunc};
use coprocessor::codec::Datum;
use crypto::{
    digest::Digest,
    md5::Md5,
    sha1::Sha1,
    sha2::{Sha224, Sha256, Sha384, Sha512},
};
use rustc_serialize::hex::ToHex;

const SHA0: i64 = 0;
const SHA224: i64 = 224;
const SHA256: i64 = 256;
const SHA384: i64 = 384;
const SHA512: i64 = 512;

impl ScalarFunc {
    pub fn md5<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let mut hasher = Md5::new();
        let mut buff: [u8; 16] = [0; 16];
        hasher.input(input.as_ref());
        hasher.result(&mut buff);
        let md5 = buff[..].to_hex().into_bytes();
        Ok(Some(Cow::Owned(md5)))
    }

    pub fn sha1<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let mut hasher = Sha1::new();
        let mut buff: [u8; 20] = [0; 20];
        hasher.input(input.as_ref());
        hasher.result(&mut buff);
        let sha1 = buff[..].to_hex().into_bytes();
        Ok(Some(Cow::Owned(sha1)))
    }

    pub fn sha2<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let input = try_opt!(self.children[0].eval_string(ctx, row));
        let hash_length = try_opt!(self.children[1].eval_int(ctx, row));

        let sha2 = match hash_length {
            SHA0 | SHA256 => {
                let mut hasher = Sha256::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            SHA224 => {
                let mut hasher = Sha224::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            SHA384 => {
                let mut hasher = Sha384::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            SHA512 => {
                let mut hasher = Sha512::new();
                hasher.input(input.as_ref());
                hasher.result_str().into_bytes()
            }
            _ => return Ok(None),
        };
        Ok(Some(Cow::Owned(sha2)))
    }
}

#[cfg(test)]
mod tests {
    use coprocessor::codec::Datum;
    use coprocessor::dag::expr::tests::{datum_expr, scalar_func_expr};
    use coprocessor::dag::expr::{EvalContext, Expression};
    use tipb::expression::ScalarFuncSig;

    #[test]
    fn test_md5() {
        let cases = vec![
            ("", "d41d8cd98f00b204e9800998ecf8427e"),
            ("a", "0cc175b9c0f1b6a831c399e269772661"),
            ("ab", "187ef4436122d1cc2f40dc2b92f0eba0"),
            ("abc", "900150983cd24fb0d6963f7d28e17f72"),
            ("123", "202cb962ac59075b964b07152d234b70"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::MD5, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "md5('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::MD5, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "md5(NULL)");
    }

    #[test]
    fn test_sha1() {
        let cases = vec![
            ("", "da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            ("a", "86f7e437faa5a7fce15d1ddcb9eaeaea377667b8"),
            ("ab", "da23614e02469a0d7c7bd1bdab5c9c474b1904dc"),
            ("abc", "a9993e364706816aba3e25717850c26c9cd0d89d"),
            ("123", "40bd001563085fc35165329ea1ff5c5ecbdbbeef"),
        ];
        let mut ctx = EvalContext::default();

        for (input_str, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::SHA1, &[input]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "sha1('{:?}')", input_str);
        }

        // test NULL case
        let input = datum_expr(Datum::Null);
        let op = scalar_func_expr(ScalarFuncSig::SHA1, &[input]);
        let op = Expression::build(&mut ctx, op).unwrap();
        let got = op.eval(&mut ctx, &[]).unwrap();
        let exp = Datum::Null;
        assert_eq!(got, exp, "sha1(NULL)");
    }

    #[test]
    fn test_sha2() {
        let cases = vec![
            ("pingcap", 0, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
            ("pingcap", 224, "cd036dc9bec69e758401379c522454ea24a6327b48724b449b40c6b7"),
            ("pingcap", 256, "2871823be240f8ecd1d72f24c99eaa2e58af18b4b8ba99a4fc2823ba5c43930a"),
            ("pingcap", 384, "c50955b6b0c7b9919740d956849eedcb0f0f90bf8a34e8c1f4e071e3773f53bd6f8f16c04425ff728bed04de1b63db51"),
            ("pingcap", 512, "ea903c574370774c4844a83b7122105a106e04211673810e1baae7c2ae7aba2cf07465e02f6c413126111ef74a417232683ce7ba210052e63c15fc82204aad80"),
            ("13572468", 0, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe"),
            ("13572468", 224, "8ad67735bbf49576219f364f4640d595357a440358d15bf6815a16e4"),
            ("13572468", 256, "1c91ab1c162fd0cae60a5bb9880f3e7d5a133a65b6057a644b26973d9c55dcfe"),
            ("13572468.123", 384, "3b4ee302435dc1e15251efd9f3982b1ca6fe4ac778d3260b7bbf3bea613849677eda830239420e448e4c6dc7c2649d89"),
            ("13572468.123", 512, "4820aa3f2760836557dc1f2d44a0ba7596333fdb60c8a1909481862f4ab0921c00abb23d57b7e67a970363cc3fcb78b25b6a0d45cdcac0e87aa0c96bc51f7f96"),
        ];

        let mut ctx = EvalContext::default();

        for (input_str, hash_length_i64, exp_str) in cases {
            let input = datum_expr(Datum::Bytes(input_str.as_bytes().to_vec()));
            let hash_length = datum_expr(Datum::I64(hash_length_i64));

            let op = scalar_func_expr(ScalarFuncSig::SHA2, &[input, hash_length]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::Bytes(exp_str.as_bytes().to_vec());
            assert_eq!(got, exp, "sha2('{:?}', {:?})", input_str, hash_length_i64);
        }

        //test NULL case
        let null_cases = vec![
            (Datum::Null, Datum::I64(224), Datum::Null),
            (Datum::Bytes(b"pingcap".to_vec()), Datum::Null, Datum::Null),
            (
                Datum::Bytes(b"pingcap".to_vec()),
                Datum::I64(123),
                Datum::Null,
            ),
        ];

        for (input, hash_length, exp) in null_cases {
            let op = scalar_func_expr(
                ScalarFuncSig::SHA2,
                &[datum_expr(input.clone()), datum_expr(hash_length.clone())],
            );
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp, "sha2('{:?}', {:?})", input, hash_length);
        }
    }
}
