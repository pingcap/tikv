// Copyright 2017 PingCAP, Inc.
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

use tipb::expression::ScalarFuncSig;
use super::{Error, FnCall, Result};

impl FnCall {
    pub fn check_args(sig: ScalarFuncSig, args: usize) -> Result<()> {
        let (min_args, max_args) = match sig {
            ScalarFuncSig::LTInt |
            ScalarFuncSig::LEInt |
            ScalarFuncSig::GTInt |
            ScalarFuncSig::GEInt |
            ScalarFuncSig::EQInt |
            ScalarFuncSig::NEInt |
            ScalarFuncSig::NullEQInt |
            ScalarFuncSig::LTReal |
            ScalarFuncSig::LEReal |
            ScalarFuncSig::GTReal |
            ScalarFuncSig::GEReal |
            ScalarFuncSig::EQReal |
            ScalarFuncSig::NEReal |
            ScalarFuncSig::NullEQReal |
            ScalarFuncSig::LTDecimal |
            ScalarFuncSig::LEDecimal |
            ScalarFuncSig::GTDecimal |
            ScalarFuncSig::GEDecimal |
            ScalarFuncSig::EQDecimal |
            ScalarFuncSig::NEDecimal |
            ScalarFuncSig::NullEQDecimal |
            ScalarFuncSig::LTString |
            ScalarFuncSig::LEString |
            ScalarFuncSig::GTString |
            ScalarFuncSig::GEString |
            ScalarFuncSig::EQString |
            ScalarFuncSig::NEString |
            ScalarFuncSig::NullEQString |
            ScalarFuncSig::LTTime |
            ScalarFuncSig::LETime |
            ScalarFuncSig::GTTime |
            ScalarFuncSig::GETime |
            ScalarFuncSig::EQTime |
            ScalarFuncSig::NETime |
            ScalarFuncSig::NullEQTime |
            ScalarFuncSig::LTDuration |
            ScalarFuncSig::LEDuration |
            ScalarFuncSig::GTDuration |
            ScalarFuncSig::GEDuration |
            ScalarFuncSig::EQDuration |
            ScalarFuncSig::NEDuration |
            ScalarFuncSig::NullEQDuration |
            ScalarFuncSig::LTJson |
            ScalarFuncSig::LEJson |
            ScalarFuncSig::GTJson |
            ScalarFuncSig::GEJson |
            ScalarFuncSig::EQJson |
            ScalarFuncSig::NEJson |
            ScalarFuncSig::NullEQJson |
            ScalarFuncSig::PlusReal |
            ScalarFuncSig::PlusDecimal |
            ScalarFuncSig::PlusInt |
            ScalarFuncSig::MinusReal |
            ScalarFuncSig::MinusDecimal |
            ScalarFuncSig::MinusInt |
            ScalarFuncSig::MultiplyReal |
            ScalarFuncSig::MultiplyDecimal |
            ScalarFuncSig::MultiplyInt |
            ScalarFuncSig::IfNullInt |
            ScalarFuncSig::IfNullReal |
            ScalarFuncSig::IfNullString |
            ScalarFuncSig::IfNullDecimal |
            ScalarFuncSig::IfNullTime |
            ScalarFuncSig::IfNullDuration |
            ScalarFuncSig::LogicalAnd |
            ScalarFuncSig::LogicalOr |
            ScalarFuncSig::LogicalXor => (2, 2),

            ScalarFuncSig::CastIntAsInt |
            ScalarFuncSig::CastIntAsReal |
            ScalarFuncSig::CastIntAsString |
            ScalarFuncSig::CastIntAsDecimal |
            ScalarFuncSig::CastIntAsTime |
            ScalarFuncSig::CastIntAsDuration |
            ScalarFuncSig::CastIntAsJson |
            ScalarFuncSig::UnaryNot |
            ScalarFuncSig::IntIsFalse |
            ScalarFuncSig::IntIsNull |
            ScalarFuncSig::RealIsTrue |
            ScalarFuncSig::RealIsNull |
            ScalarFuncSig::DecimalIsTrue |
            ScalarFuncSig::DecimalIsNull |
            ScalarFuncSig::StringIsNull |
            ScalarFuncSig::TimeIsNull |
            ScalarFuncSig::DurationIsNull => (1, 1),

            ScalarFuncSig::IfInt |
            ScalarFuncSig::IfReal |
            ScalarFuncSig::IfString |
            ScalarFuncSig::IfDecimal |
            ScalarFuncSig::IfTime |
            ScalarFuncSig::IfDuration => (3, 3),

            _ => unimplemented!(),
        };
        if args < min_args || args > max_args {
            return Err(Error::Other("unexpected arguments"));
        }
        Ok(())
    }
}
