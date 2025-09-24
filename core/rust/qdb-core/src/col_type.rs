/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
use crate::error::{CoreError, CoreErrorExt, CoreResult, fmt_err};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Debug, Display, Formatter};

pub const QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG: i32 = 1 << 10;

// Don't forget to update VALUES when modifying this list.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ColumnTypeTag {
    /// Placeholder for unsupported/unknown types.
    /// Note: ColumnType::try_from(i32) rejects code <= 0, so Undefined is not a valid serialized code.
    Undefined = 0,
    Boolean = 1,
    Byte = 2,
    Short = 3,
    Char = 4,
    Int = 5,
    Long = 6,
    Date = 7,
    Timestamp = 8,
    Float = 9,
    Double = 10,
    String = 11,
    Symbol = 12,
    Long256 = 13,
    GeoByte = 14,
    GeoShort = 15,
    GeoInt = 16,
    GeoLong = 17,
    Binary = 18,
    Uuid = 19,
    Long128 = 24,
    IPv4 = 25,
    Varchar = 26,
    Array = 27,
}

impl ColumnTypeTag {
    #[cfg(test)]
    const VALUES: [Self; 24] = [
        Self::Undefined,
        Self::Boolean,
        Self::Byte,
        Self::Short,
        Self::Char,
        Self::Int,
        Self::Long,
        Self::Date,
        Self::Timestamp,
        Self::Float,
        Self::Double,
        Self::String,
        Self::Symbol,
        Self::Long256,
        Self::GeoByte,
        Self::GeoShort,
        Self::GeoInt,
        Self::GeoLong,
        Self::Binary,
        Self::Uuid,
        Self::Long128,
        Self::IPv4,
        Self::Varchar,
        Self::Array,
    ];

    /// If true, the column is encoded with both data and aux vectors.
    pub const fn is_var_size(self) -> bool {
        self.fixed_size().is_none()
    }

    /// Obtains the fixed size required to encode the type on disk in bytes.
    /// If the type is var size, returns None.
    /// N.B. Symbol columns are _also_ considered fixed size.
    pub const fn fixed_size(self) -> Option<usize> {
        match self {
            ColumnTypeTag::Boolean | ColumnTypeTag::GeoByte | ColumnTypeTag::Byte => Some(1),

            ColumnTypeTag::Short | ColumnTypeTag::GeoShort | ColumnTypeTag::Char => Some(2),

            ColumnTypeTag::Float
            | ColumnTypeTag::Int
            | ColumnTypeTag::IPv4
            | ColumnTypeTag::GeoInt
            | ColumnTypeTag::Symbol => Some(4),

            ColumnTypeTag::Double
            | ColumnTypeTag::Long
            | ColumnTypeTag::Date
            | ColumnTypeTag::GeoLong
            | ColumnTypeTag::Timestamp => Some(8),

            ColumnTypeTag::Long128 | ColumnTypeTag::Uuid => Some(16),

            ColumnTypeTag::Long256 => Some(32),

            _ => None,
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            ColumnTypeTag::Undefined => "undefined",
            ColumnTypeTag::Boolean => "boolean",
            ColumnTypeTag::Byte => "byte",
            ColumnTypeTag::Short => "short",
            ColumnTypeTag::Char => "char",
            ColumnTypeTag::Int => "int",
            ColumnTypeTag::Long => "long",
            ColumnTypeTag::Date => "date",
            ColumnTypeTag::Timestamp => "timestamp",
            ColumnTypeTag::Float => "float",
            ColumnTypeTag::Double => "double",
            ColumnTypeTag::String => "string",
            ColumnTypeTag::Symbol => "symbol",
            ColumnTypeTag::Long256 => "long256",
            ColumnTypeTag::GeoByte => "geobyte",
            ColumnTypeTag::GeoShort => "geoshort",
            ColumnTypeTag::GeoInt => "geoint",
            ColumnTypeTag::GeoLong => "geolong",
            ColumnTypeTag::Binary => "binary",
            ColumnTypeTag::Uuid => "uuid",
            ColumnTypeTag::Long128 => "long128",
            ColumnTypeTag::IPv4 => "ipv4",
            ColumnTypeTag::Varchar => "varchar",
            ColumnTypeTag::Array => "array",
        }
    }

    // Don't expose this in the general API, as it heightens the risk
    // of constructing an invalid `ColumnType`, e.g. one without the appropriate
    // extra type info for Geo types.
    #[cfg(test)]
    pub(crate) fn into_type(self) -> ColumnType {
        ColumnType::new(self, 0)
    }
}

impl TryFrom<u8> for ColumnTypeTag {
    type Error = CoreError;

    fn try_from(col_tag_num: u8) -> Result<Self, Self::Error> {
        match col_tag_num {
            0 => Ok(ColumnTypeTag::Undefined),
            1 => Ok(ColumnTypeTag::Boolean),
            2 => Ok(ColumnTypeTag::Byte),
            3 => Ok(ColumnTypeTag::Short),
            4 => Ok(ColumnTypeTag::Char),
            5 => Ok(ColumnTypeTag::Int),
            6 => Ok(ColumnTypeTag::Long),
            7 => Ok(ColumnTypeTag::Date),
            8 => Ok(ColumnTypeTag::Timestamp),
            9 => Ok(ColumnTypeTag::Float),
            10 => Ok(ColumnTypeTag::Double),
            11 => Ok(ColumnTypeTag::String),
            12 => Ok(ColumnTypeTag::Symbol),
            13 => Ok(ColumnTypeTag::Long256),
            14 => Ok(ColumnTypeTag::GeoByte),
            15 => Ok(ColumnTypeTag::GeoShort),
            16 => Ok(ColumnTypeTag::GeoInt),
            17 => Ok(ColumnTypeTag::GeoLong),
            18 => Ok(ColumnTypeTag::Binary),
            19 => Ok(ColumnTypeTag::Uuid),
            24 => Ok(ColumnTypeTag::Long128),
            25 => Ok(ColumnTypeTag::IPv4),
            26 => Ok(ColumnTypeTag::Varchar),
            27 => Ok(ColumnTypeTag::Array),
            _ => Err(fmt_err!(
                InvalidType,
                "unknown QuestDB column tag code: {}",
                col_tag_num
            )),
        }
    }
}

fn tag_of(col_type: i32) -> u8 {
    (col_type & 0xFF) as u8
}

const TYPE_FLAG_DESIGNATED_TIMESTAMP: i32 = 1i32 << 17;
const ARRAY_ELEMTYPE_FIELD_MASK: i32 = 0x3F;
const ARRAY_ELEMTYPE_FIELD_POS: i32 = 8;
const ARRAY_NDIMS_LIMIT: i32 = 32; // inclusive
const ARRAY_NDIMS_FIELD_MASK: i32 = ARRAY_NDIMS_LIMIT - 1;
const ARRAY_NDIMS_FIELD_POS: i32 = 14;

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Serialize, Ord, PartialOrd, Eq)]
#[serde(transparent)]
pub struct ColumnType {
    // Optimization so `Option<ColumnType>` is the same size as `ColumnType`.
    code: i32,
}

impl ColumnType {
    pub fn new(tag: ColumnTypeTag, extra_type_info: i32) -> Self {
        let shifted_extra_type_info = extra_type_info << 8;
        let code = tag as i32 | shifted_extra_type_info;
        Self { code }
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn is_designated(&self) -> bool {
        (self.tag() == ColumnTypeTag::Timestamp)
            && ((self.code & TYPE_FLAG_DESIGNATED_TIMESTAMP) > 0)
    }

    pub fn into_designated(self) -> CoreResult<ColumnType> {
        if self.tag() != ColumnTypeTag::Timestamp {
            return Err(fmt_err!(
                InvalidType,
                "invalid column type {}, only timestamp columns can be marked as designated",
                self
            ));
        }
        let code = self.code() | TYPE_FLAG_DESIGNATED_TIMESTAMP;
        Ok(Self { code })
    }

    pub fn tag(&self) -> ColumnTypeTag {
        let col_tag_num: u8 = tag_of(self.code());
        // Constructing from int should already have validated the tag.
        col_tag_num
            .try_into()
            .expect("invalid column type tag, should already be validated")
    }

    pub fn array_dimensionality(&self) -> CoreResult<i32> {
        if self.tag() != ColumnTypeTag::Array {
            return Err(fmt_err!(
                InvalidType,
                "invalid column type {}, only array columns have dimensionality",
                self
            ));
        }
        let dim = ((self.code() >> ARRAY_NDIMS_FIELD_POS) & ARRAY_NDIMS_FIELD_MASK) + 1;
        Ok(dim)
    }

    pub fn array_element_type(&self) -> CoreResult<ColumnTypeTag> {
        if self.tag() != ColumnTypeTag::Array {
            return Err(fmt_err!(
                InvalidType,
                "invalid column type {}, only array columns have element type",
                self
            ));
        }
        let tag = (self.code() >> ARRAY_ELEMTYPE_FIELD_POS) & ARRAY_ELEMTYPE_FIELD_MASK;
        let tag = ColumnTypeTag::try_from(tag as u8)?;
        Ok(tag)
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.code, self.tag().name())
    }
}

impl Debug for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ColumnType({}/{:?})", self.code, self.tag())
    }
}

impl TryFrom<i32> for ColumnType {
    type Error = CoreError;

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        if v <= 0 {
            return Err(fmt_err!(
                InvalidType,
                "invalid column type code <= 0: {}",
                v
            ));
        }
        // Start with removing geohash size bits. See ColumnType#tagOf().
        let col_tag_num = tag_of(v);
        let _tag: ColumnTypeTag = col_tag_num
            .try_into()
            .with_context(|_| format!("could not parse {v} to a valid ColumnType"))?;
        Ok(Self { code: v })
    }
}

impl<'de> Deserialize<'de> for ColumnType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let code = i32::deserialize(deserializer)?;
        ColumnType::try_from(code).map_err(serde::de::Error::custom)
    }
}

pub fn encode_array_type(elem_type: ColumnTypeTag, dim: i32) -> CoreResult<ColumnType> {
    if !(1..=ARRAY_NDIMS_LIMIT).contains(&dim) {
        return Err(fmt_err!(InvalidType, "invalid array dimensionality {dim}",));
    }
    if elem_type != ColumnTypeTag::Double {
        return Err(fmt_err!(
            InvalidType,
            "unsupported array element type {}",
            elem_type.name()
        ));
    }
    let extra = ((dim - 1) & ARRAY_NDIMS_FIELD_MASK)
        << (ARRAY_NDIMS_FIELD_POS - ARRAY_ELEMTYPE_FIELD_POS)
        | ((elem_type as i32) & ARRAY_ELEMTYPE_FIELD_MASK);
    Ok(ColumnType::new(ColumnTypeTag::Array, extra))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_value_deserialization() {
        let scenarios = [
            (0i32, "invalid column type code <= 0: 0"),
            (-20, "invalid column type code <= 0: -20"),
            (
                244,
                "could not parse 244 to a valid ColumnType: unknown QuestDB column tag code: 244",
            ),
            (
                100073,
                "could not parse 100073 to a valid ColumnType: unknown QuestDB column tag code: 233",
            ),
        ];
        for &(code, exp_err_msg) in &scenarios {
            let encoded = serde_json::json!(code);
            let deserialized: Result<ColumnType, _> = serde_json::from_value(encoded);
            assert!(deserialized.is_err());

            // Stringify error without backtrace.
            let msg = deserialized.unwrap_err().to_string();
            assert_eq!(msg, exp_err_msg);
        }
    }

    #[test]
    fn test_is_var_size() {
        assert!(!ColumnTypeTag::Byte.is_var_size());
        assert!(!ColumnTypeTag::Boolean.is_var_size());
        assert!(!ColumnTypeTag::Long.is_var_size());
        assert!(!ColumnTypeTag::Double.is_var_size());
        assert!(ColumnTypeTag::String.is_var_size());

        // Yes, symbols too.
        assert!(!ColumnTypeTag::Symbol.is_var_size());

        assert!(ColumnTypeTag::Binary.is_var_size());
        assert!(ColumnTypeTag::String.is_var_size());
        assert!(ColumnTypeTag::Varchar.is_var_size());
        assert!(ColumnTypeTag::Array.is_var_size());
    }

    #[test]
    fn test_fixed_size() {
        assert_eq!(ColumnTypeTag::Boolean.fixed_size(), Some(1));
        assert_eq!(ColumnTypeTag::Short.fixed_size(), Some(2));
        assert_eq!(ColumnTypeTag::Int.fixed_size(), Some(4));
        assert_eq!(ColumnTypeTag::Long.fixed_size(), Some(8));
        assert_eq!(ColumnTypeTag::Uuid.fixed_size(), Some(16));
        assert_eq!(ColumnTypeTag::Long256.fixed_size(), Some(32));
        assert_eq!(ColumnTypeTag::Binary.fixed_size(), None);
        assert_eq!(ColumnTypeTag::String.fixed_size(), None);
        assert_eq!(ColumnTypeTag::Varchar.fixed_size(), None);
        assert_eq!(ColumnTypeTag::Array.fixed_size(), None);
    }

    #[test]
    fn test_array_dimensionality() {
        for tag in ColumnTypeTag::VALUES {
            if tag != ColumnTypeTag::Array {
                assert!(ColumnType::new(tag, 0).array_dimensionality().is_err());
            }
        }

        let typ = encode_array_type(ColumnTypeTag::Double, 3);
        assert!(typ.is_ok());
        let dim = typ.unwrap().array_dimensionality();
        assert!(dim.is_ok());
        assert_eq!(dim.unwrap(), 3);
    }

    #[test]
    fn test_array_element_type() {
        for tag in ColumnTypeTag::VALUES {
            if tag != ColumnTypeTag::Array {
                assert!(ColumnType::new(tag, 0).array_element_type().is_err());
            }
        }

        let typ = encode_array_type(ColumnTypeTag::Double, 3);
        assert!(typ.is_ok());
        let dim = typ.unwrap().array_element_type();
        assert!(dim.is_ok());
        assert_eq!(dim.unwrap(), ColumnTypeTag::Double);
    }

    #[test]
    fn test_encode_array_type() {
        let typ = encode_array_type(ColumnTypeTag::Double, 11);
        assert!(typ.is_ok());
        let typ = typ.unwrap();

        let elem_typ = typ.array_element_type();
        assert!(elem_typ.is_ok());
        assert_eq!(elem_typ.unwrap(), ColumnTypeTag::Double);

        let dim = typ.array_dimensionality();
        assert!(dim.is_ok());
        assert_eq!(dim.unwrap(), 11);
    }
}
