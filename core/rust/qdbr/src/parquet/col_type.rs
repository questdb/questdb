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
use crate::parquet::error::{fmt_err, ParquetError};
use serde::{Deserialize, Deserializer, Serialize};
use std::num::NonZeroI32;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ColumnTypeTag {
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
}

impl ColumnTypeTag {
    // Don't expose this in the general API, as it heightens the risk
    // of constructing an invalid `ColumnType`, e.g. one without the appropriate
    // extra type info for Geo types.
    #[cfg(test)]
    pub fn into_type(self) -> ColumnType {
        ColumnType::new(self, 0)
    }
}

impl TryFrom<u8> for ColumnTypeTag {
    type Error = ParquetError;

    fn try_from(col_tag_num: u8) -> Result<Self, Self::Error> {
        match col_tag_num {
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
            21 => Ok(ColumnTypeTag::IPv4),
            24 => Ok(ColumnTypeTag::Long128),
            25 => Ok(ColumnTypeTag::IPv4),
            26 => Ok(ColumnTypeTag::Varchar),
            _ => Err(fmt_err!(
                Invalid,
                "unknown QuestDB column tag code: {}",
                col_tag_num
            )),
        }
    }
}

fn tag_of(col_type: i32) -> u8 {
    (col_type & 0xFF) as u8
}

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ColumnType {
    // Optimization so `Option<ColumnType>` is the same size as `ColumnType`.
    code: NonZeroI32,
}

impl ColumnType {
    pub fn new(tag: ColumnTypeTag, extra_type_info: i32) -> Self {
        let shifted_extra_type_info = extra_type_info << 8;
        let code = NonZeroI32::new(tag as i32 | shifted_extra_type_info)
            .expect("column type code should never be zero");
        Self { code }
    }

    pub fn code(&self) -> i32 {
        self.code.get()
    }

    pub fn tag(&self) -> ColumnTypeTag {
        let col_tag_num: u8 = tag_of(self.code());
        // Constructing from int should already have validated the tag.
        col_tag_num
            .try_into()
            .expect("invalid column type tag, should already be validated")
    }
}

impl TryFrom<i32> for ColumnType {
    type Error = ParquetError;

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        // Start with removing geohash size bits. See ColumnType#tagOf().
        let col_tag_num = tag_of(v);
        let _tag: ColumnTypeTag = col_tag_num.try_into()?; // just validate
        let code = NonZeroI32::new(v).expect("column type code should never be zero");
        Ok(Self { code })
    }
}

impl <'de> Deserialize<'de> for ColumnType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let code = i32::deserialize(deserializer)?;
        ColumnType::try_from(code).map_err(serde::de::Error::custom)
    }
}
