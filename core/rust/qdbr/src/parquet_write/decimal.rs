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

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Decimal8(i8);

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Decimal16(i16);

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Decimal32(i32);

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Decimal64(i64);

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Decimal128(i64, i64);

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
        let lo = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
        Self(hi, lo)
    }

    #[inline]
    fn ord(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }

    const TYPE: PhysicalType = PhysicalType::FixedLenByteArray(std::mem::size_of::<Self>());
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Decimal256(i64, i64, i64, i64);

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
        let hi = i64::from_be_bytes(bytes[8..16].try_into().unwrap());
        let lo = i64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let ll = i64::from_be_bytes(bytes[24..32].try_into().unwrap());
        Self(hh, hi, lo, ll)
    }

    #[inline]
    fn ord(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
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
                self.0
                    .partial_cmp(&other.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }

            const TYPE: PhysicalType = PhysicalType::FixedLenByteArray(std::mem::size_of::<Self>());
        }
    };
}

native!(Decimal8, i8);
native!(Decimal16, i16);
native!(Decimal32, i32);
native!(Decimal64, i64);
