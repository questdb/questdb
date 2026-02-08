/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

use parquet2::{schema::types::PhysicalType, types::NativeType};

use crate::parquet_write::Nullable;

pub const DECIMAL8_NULL: [u8; 1] = i8::MIN.to_le_bytes();
pub const DECIMAL16_NULL: [u8; 2] = i16::MIN.to_le_bytes();
pub const DECIMAL32_NULL: [u8; 4] = i32::MIN.to_le_bytes();
pub const DECIMAL64_NULL: [u8; 8] = i64::MIN.to_le_bytes();
pub const DECIMAL128_NULL: [u8; 16] = {
    let hi = i64::MIN.to_le_bytes();
    let lo = 0i64.to_le_bytes();
    [
        hi[0], hi[1], hi[2], hi[3], hi[4], hi[5], hi[6], hi[7], lo[0], lo[1], lo[2], lo[3],
        lo[4], lo[5], lo[6], lo[7],
    ]
};
pub const DECIMAL256_NULL: [u8; 32] = {
    let hh = i64::MIN.to_le_bytes();
    let hi = 0i64.to_le_bytes();
    let lo = 0i64.to_le_bytes();
    let ll = 0i64.to_le_bytes();
    [
        hh[0], hh[1], hh[2], hh[3], hh[4], hh[5], hh[6], hh[7], hi[0], hi[1], hi[2], hi[3],
        hi[4], hi[5], hi[6], hi[7], lo[0], lo[1], lo[2], lo[3], lo[4], lo[5], lo[6], lo[7],
        ll[0], ll[1], ll[2], ll[3], ll[4], ll[5], ll[6], ll[7],
    ]
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Decimal8(i8);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Decimal16(i16);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Decimal32(i32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Decimal64(i64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Decimal128(i64, u64);

impl Nullable for Decimal128 {
    fn is_null(&self) -> bool {
        self.0 == i64::MIN && self.1 == 0
    }
}

impl NativeType for Decimal128 {
    type Bytes = [u8; 16];

    #[inline]
    fn to_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 16];
        bytes[0..8].copy_from_slice(&self.0.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.1.to_be_bytes());
        bytes
    }

    #[inline]
    fn from_bytes(bytes: Self::Bytes) -> Self {
        let hi = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let lo = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        Self(hi, lo)
    }

    #[inline]
    fn ord(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp(other)
    }

    const TYPE: PhysicalType = PhysicalType::FixedLenByteArray(std::mem::size_of::<Self>());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(C)]
pub struct Decimal256(i64, u64, u64, u64);

impl Nullable for Decimal256 {
    fn is_null(&self) -> bool {
        self.0 == i64::MIN && self.1 == 0 && self.2 == 0 && self.3 == 0
    }
}

impl NativeType for Decimal256 {
    type Bytes = [u8; 32];
    #[inline]
    fn to_bytes(&self) -> Self::Bytes {
        let mut bytes = [0; 32];
        bytes[0..8].copy_from_slice(&self.0.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.1.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.2.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.3.to_be_bytes());
        bytes
    }

    #[inline]
    fn from_bytes(bytes: Self::Bytes) -> Self {
        let hh = i64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let hi = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let lo = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let ll = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        Self(hh, hi, lo, ll)
    }

    #[inline]
    fn ord(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp(other)
    }

    const TYPE: PhysicalType = PhysicalType::FixedLenByteArray(std::mem::size_of::<Self>());
}

macro_rules! native {
    ($type:ty, $inner_type: ident) => {
        impl Nullable for $type {
            fn is_null(&self) -> bool {
                self.0 == $inner_type::MIN
            }
        }

        impl NativeType for $type {
            type Bytes = [u8; std::mem::size_of::<Self>()];
            #[inline]
            fn to_bytes(&self) -> Self::Bytes {
                self.0.to_be_bytes()
            }

            #[inline]
            fn from_bytes(bytes: Self::Bytes) -> Self {
                Self($inner_type::from_be_bytes(bytes))
            }

            #[inline]
            fn ord(&self, other: &Self) -> std::cmp::Ordering {
                self.0.cmp(&other.0)
            }

            const TYPE: PhysicalType = PhysicalType::FixedLenByteArray(std::mem::size_of::<Self>());
        }
    };
}

native!(Decimal8, i8);
native!(Decimal16, i16);
native!(Decimal32, i32);
native!(Decimal64, i64);

#[cfg(test)]
mod tests {
    use super::*;
    use parquet2::types::NativeType;

    /// Helper to test roundtrip for any decimal type
    fn assert_roundtrip<T: NativeType + std::fmt::Debug + PartialEq>(value: T) {
        let bytes = value.to_bytes();
        let restored = T::from_bytes(bytes);
        assert_eq!(value, restored);
    }

    // ==================== Decimal8 Tests ====================

    #[test]
    fn test_decimal8_is_null() {
        assert!(Decimal8(i8::MIN).is_null());
        assert!(!Decimal8(0).is_null());
        assert!(!Decimal8(1).is_null());
        assert!(!Decimal8(-1).is_null());
        assert!(!Decimal8(i8::MAX).is_null());
    }

    #[test]
    fn test_decimal8_roundtrip() {
        assert_roundtrip(Decimal8(i8::MIN));
        assert_roundtrip(Decimal8(-100));
        assert_roundtrip(Decimal8(-1));
        assert_roundtrip(Decimal8(0));
        assert_roundtrip(Decimal8(1));
        assert_roundtrip(Decimal8(100));
        assert_roundtrip(Decimal8(i8::MAX));
    }

    #[test]
    fn test_decimal8_to_from_bytes() {
        assert_eq!(Decimal8(1).to_bytes(), [0x01]);
        assert_eq!(Decimal8(-1).to_bytes(), [0xFF]);
        assert_eq!(Decimal8::from_bytes([0x7F]), Decimal8(127));
        assert_eq!(Decimal8::from_bytes([0x80]), Decimal8(-128));
    }

    // ==================== Decimal16 Tests ====================

    #[test]
    fn test_decimal16_is_null() {
        assert!(Decimal16(i16::MIN).is_null());
        assert!(!Decimal16(0).is_null());
        assert!(!Decimal16(1).is_null());
        assert!(!Decimal16(-1).is_null());
        assert!(!Decimal16(i16::MAX).is_null());
    }

    #[test]
    fn test_decimal16_roundtrip() {
        assert_roundtrip(Decimal16(i16::MIN));
        assert_roundtrip(Decimal16(-1000));
        assert_roundtrip(Decimal16(-1));
        assert_roundtrip(Decimal16(0));
        assert_roundtrip(Decimal16(1));
        assert_roundtrip(Decimal16(1000));
        assert_roundtrip(Decimal16(i16::MAX));
    }

    #[test]
    fn test_decimal16_to_from_bytes() {
        assert_eq!(Decimal16(1).to_bytes(), [0x00, 0x01]);
        assert_eq!(Decimal16(-1).to_bytes(), [0xFF, 0xFF]);
        assert_eq!(Decimal16(0x1234).to_bytes(), [0x12, 0x34]);
        assert_eq!(Decimal16::from_bytes([0x12, 0x34]), Decimal16(0x1234));
    }

    // ==================== Decimal32 Tests ====================

    #[test]
    fn test_decimal32_is_null() {
        assert!(Decimal32(i32::MIN).is_null());
        assert!(!Decimal32(0).is_null());
        assert!(!Decimal32(1).is_null());
        assert!(!Decimal32(-1).is_null());
        assert!(!Decimal32(i32::MAX).is_null());
    }

    #[test]
    fn test_decimal32_roundtrip() {
        assert_roundtrip(Decimal32(i32::MIN));
        assert_roundtrip(Decimal32(-1_000_000));
        assert_roundtrip(Decimal32(-1));
        assert_roundtrip(Decimal32(0));
        assert_roundtrip(Decimal32(1));
        assert_roundtrip(Decimal32(1_000_000));
        assert_roundtrip(Decimal32(i32::MAX));
    }

    #[test]
    fn test_decimal32_to_from_bytes() {
        assert_eq!(Decimal32(1).to_bytes(), [0x00, 0x00, 0x00, 0x01]);
        assert_eq!(Decimal32(-1).to_bytes(), [0xFF, 0xFF, 0xFF, 0xFF]);
        assert_eq!(Decimal32(0x12345678).to_bytes(), [0x12, 0x34, 0x56, 0x78]);
        assert_eq!(
            Decimal32::from_bytes([0x12, 0x34, 0x56, 0x78]),
            Decimal32(0x12345678)
        );
    }

    // ==================== Decimal64 Tests ====================

    #[test]
    fn test_decimal64_is_null() {
        assert!(Decimal64(i64::MIN).is_null());
        assert!(!Decimal64(0).is_null());
        assert!(!Decimal64(1).is_null());
        assert!(!Decimal64(-1).is_null());
        assert!(!Decimal64(i64::MAX).is_null());
    }

    #[test]
    fn test_decimal64_roundtrip() {
        assert_roundtrip(Decimal64(i64::MIN));
        assert_roundtrip(Decimal64(-1_000_000_000_000i64));
        assert_roundtrip(Decimal64(-1));
        assert_roundtrip(Decimal64(0));
        assert_roundtrip(Decimal64(1));
        assert_roundtrip(Decimal64(1_000_000_000_000i64));
        assert_roundtrip(Decimal64(i64::MAX));
    }

    #[test]
    fn test_decimal64_to_from_bytes() {
        assert_eq!(
            Decimal64(1).to_bytes(),
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        );
        assert_eq!(
            Decimal64(-1).to_bytes(),
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            Decimal64(0x0102030405060708).to_bytes(),
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
        assert_eq!(
            Decimal64::from_bytes([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]),
            Decimal64(0x0102030405060708)
        );
    }

    // ==================== Decimal128 Tests ====================

    #[test]
    fn test_decimal128_is_null() {
        assert!(Decimal128(i64::MIN, 0).is_null());
        assert!(!Decimal128(0, 0).is_null());
        assert!(!Decimal128(1, 0).is_null());
        assert!(!Decimal128(0, 1).is_null());
        assert!(!Decimal128(-1, u64::MAX).is_null());
        assert!(!Decimal128(i64::MIN, 1).is_null());
        assert!(!Decimal128(i64::MAX, u64::MAX).is_null());
    }

    #[test]
    fn test_decimal128_roundtrip() {
        assert_roundtrip(Decimal128(0, 0));
        assert_roundtrip(Decimal128(0, 1));
        assert_roundtrip(Decimal128(1, 0));
        assert_roundtrip(Decimal128(-1, u64::MAX));
        assert_roundtrip(Decimal128(i64::MAX, u64::MAX));
        assert_roundtrip(Decimal128(i64::MIN + 1, 0));
        assert_roundtrip(Decimal128(0x12345678, 0xABCDEF01));
    }

    #[test]
    fn test_decimal128_to_from_bytes() {
        // Test specific byte patterns
        assert_eq!(
            Decimal128(0x0102030405060708, 0x090A0B0C0D0E0F10).to_bytes(),
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
                0x0F, 0x10
            ]
        );

        assert_eq!(
            Decimal128::from_bytes([
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
                0x0F, 0x10,
            ]),
            Decimal128(0x0102030405060708, 0x090A0B0C0D0E0F10)
        );

        // -1 should be all 0xFF
        assert_eq!(Decimal128(-1, u64::MAX).to_bytes(), [0xFF; 16]);
    }

    // ==================== Decimal256 Tests ====================

    #[test]
    fn test_decimal256_is_null() {
        assert!(Decimal256(i64::MIN, 0, 0, 0).is_null());
        assert!(!Decimal256(0, 0, 0, 0).is_null());
        assert!(!Decimal256(1, 0, 0, 0).is_null());
        assert!(!Decimal256(0, 1, 0, 0).is_null());
        assert!(!Decimal256(0, 0, 1, 0).is_null());
        assert!(!Decimal256(0, 0, 0, 1).is_null());
        assert!(!Decimal256(i64::MIN, 1, 0, 0).is_null());
        assert!(!Decimal256(i64::MIN, 0, 1, 0).is_null());
        assert!(!Decimal256(i64::MIN, 0, 0, 1).is_null());
        assert!(!Decimal256(-1, u64::MAX, u64::MAX, u64::MAX).is_null());
    }

    #[test]
    fn test_decimal256_roundtrip() {
        assert_roundtrip(Decimal256(0, 0, 0, 0));
        assert_roundtrip(Decimal256(0, 0, 0, 1));
        assert_roundtrip(Decimal256(1, 0, 0, 0));
        assert_roundtrip(Decimal256(0, 1, 0, 0));
        assert_roundtrip(Decimal256(0, 0, 1, 0));
        assert_roundtrip(Decimal256(-1, u64::MAX, u64::MAX, u64::MAX));
        assert_roundtrip(Decimal256(i64::MAX, u64::MAX, u64::MAX, u64::MAX));
        assert_roundtrip(Decimal256(i64::MIN + 1, 0, 0, 0));
        assert_roundtrip(Decimal256(0x12345678, 0xABCDEF01, 0x11223344, 0x55667788));
    }

    #[test]
    fn test_decimal256_to_from_bytes() {
        // Test specific byte patterns
        assert_eq!(
            Decimal256(
                0x0102030405060708,
                0x090A0B0C0D0E0F10,
                0x1112131415161718,
                0x191A1B1C1D1E1F20
            )
            .to_bytes(),
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
                0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C,
                0x1D, 0x1E, 0x1F, 0x20
            ]
        );

        assert_eq!(
            Decimal256::from_bytes([
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
                0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C,
                0x1D, 0x1E, 0x1F, 0x20,
            ]),
            Decimal256(
                0x0102030405060708,
                0x090A0B0C0D0E0F10,
                0x1112131415161718,
                0x191A1B1C1D1E1F20
            )
        );

        // -1 should be all 0xFF
        assert_eq!(
            Decimal256(-1, u64::MAX, u64::MAX, u64::MAX).to_bytes(),
            [0xFF; 32]
        );
    }

    // ==================== PhysicalType Tests ====================

    #[test]
    fn test_physical_types() {
        assert_eq!(Decimal8::TYPE, PhysicalType::FixedLenByteArray(1));
        assert_eq!(Decimal16::TYPE, PhysicalType::FixedLenByteArray(2));
        assert_eq!(Decimal32::TYPE, PhysicalType::FixedLenByteArray(4));
        assert_eq!(Decimal64::TYPE, PhysicalType::FixedLenByteArray(8));
        assert_eq!(Decimal128::TYPE, PhysicalType::FixedLenByteArray(16));
        assert_eq!(Decimal256::TYPE, PhysicalType::FixedLenByteArray(32));
    }

    // ==================== Ordering Tests ====================

    use std::cmp::Ordering;

    #[test]
    fn test_decimal8_ordering() {
        assert_eq!(Decimal8(-1).ord(&Decimal8(0)), Ordering::Less);
        assert_eq!(Decimal8(0).ord(&Decimal8(1)), Ordering::Less);
        assert_eq!(Decimal8(i8::MIN).ord(&Decimal8(i8::MAX)), Ordering::Less);
    }

    #[test]
    fn test_decimal16_ordering() {
        assert_eq!(Decimal16(-1).ord(&Decimal16(0)), Ordering::Less);
        assert_eq!(Decimal16(0).ord(&Decimal16(1)), Ordering::Less);
        assert_eq!(
            Decimal16(i16::MIN).ord(&Decimal16(i16::MAX)),
            Ordering::Less
        );
    }

    #[test]
    fn test_decimal32_ordering() {
        assert_eq!(Decimal32(-1).ord(&Decimal32(0)), Ordering::Less);
        assert_eq!(Decimal32(0).ord(&Decimal32(1)), Ordering::Less);
        assert_eq!(
            Decimal32(i32::MIN).ord(&Decimal32(i32::MAX)),
            Ordering::Less
        );
    }

    #[test]
    fn test_decimal64_ordering() {
        assert_eq!(Decimal64(-1).ord(&Decimal64(0)), Ordering::Less);
        assert_eq!(Decimal64(0).ord(&Decimal64(1)), Ordering::Less);
        assert_eq!(
            Decimal64(i64::MIN).ord(&Decimal64(i64::MAX)),
            Ordering::Less
        );
    }

    #[test]
    fn test_decimal128_ordering() {
        assert_eq!(
            Decimal128(-1, u64::MAX).ord(&Decimal128(0, 0)),
            Ordering::Less
        );
        assert_eq!(Decimal128(0, 0).ord(&Decimal128(0, 1)), Ordering::Less);
        assert_eq!(Decimal128(0, 1).ord(&Decimal128(1, 0)), Ordering::Less);
        assert_eq!(
            Decimal128(i64::MIN, 0).ord(&Decimal128(i64::MAX, u64::MAX)),
            Ordering::Less
        );
    }

    #[test]
    fn test_decimal256_ordering() {
        assert_eq!(
            Decimal256(-1, u64::MAX, u64::MAX, u64::MAX).ord(&Decimal256(0, 0, 0, 0)),
            Ordering::Less
        );
        assert_eq!(
            Decimal256(0, 0, 0, 0).ord(&Decimal256(0, 0, 0, 1)),
            Ordering::Less
        );
        assert_eq!(
            Decimal256(0, 0, 0, 1).ord(&Decimal256(0, 0, 1, 0)),
            Ordering::Less
        );
        assert_eq!(
            Decimal256(0, 0, 1, 0).ord(&Decimal256(0, 1, 0, 0)),
            Ordering::Less
        );
        assert_eq!(
            Decimal256(0, 1, 0, 0).ord(&Decimal256(1, 0, 0, 0)),
            Ordering::Less
        );
        assert_eq!(
            Decimal256(i64::MIN, 0, 0, 0).ord(&Decimal256(i64::MAX, u64::MAX, u64::MAX, u64::MAX)),
            Ordering::Less
        );
    }
}
