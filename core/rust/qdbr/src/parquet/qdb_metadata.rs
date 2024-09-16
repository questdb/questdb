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
#![allow(dead_code)]

use crate::parquet::error::{fmt_err, ParquetError, ParquetErrorCause, ParquetResult};
use serde::{Deserialize, Serialize};

/// A constant field that serializes always as the same value in JSON.
/// On deserialization, it checks that the value is the same as the constant.
struct U32Const<const N: u32>;

impl<const N: u32> Serialize for U32Const<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        N.serialize(serializer)
    }
}

impl<'de, const N: u32> Deserialize<'de> for U32Const<N> {
    fn deserialize<D>(deserializer: D) -> Result<U32Const<N>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let n = u32::deserialize(deserializer)?;
        if n != N {
            return Err(serde::de::Error::custom(format!(
                "expected {}, got {}",
                N, n
            )));
        }
        Ok(U32Const)
    }
}

/// A basic "version"-only metadata struct that's exclusively used to select
/// which version of the metadata struct to deserialize.
/// This contains just the `version` field that all the other versions
/// of the metadata struct must also contain as a `
#[derive(Deserialize)]
struct VersionMetadata {
    pub version: u32,
}

/// Special instructions on how to handle the column data,
/// beyond the basic column type.
#[derive(Serialize, Deserialize)]
pub enum Handling {
    /// A symbol column where the local dictionary keys,
    /// i.e. the numeric keys in the parquet data match
    /// the QuestDB keys in the global symbol table.
    SymbolLocalIsGlobal,
}

#[derive(Serialize, Deserialize)]
pub struct Column {
    /// The numeric code for the internal QuestDB column type.
    /// To convert, use `let column_type: ColumnType = col.qdb_type_code.try_into()?`.
    pub qdb_type_code: i32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub handling: Option<Handling>,
}

#[derive(Serialize, Deserialize)]
pub struct Schema {
    columns: Vec<Column>,
}

#[derive(Serialize, Deserialize)]
pub struct QdbMetadataV1 {
    version: U32Const<1>,
    schema: Schema,
}

/// Alias to the latest version of struct.
/// This is the only one we use in the code base.
/// Older versions upgraded to this version when read.
pub type QdbMetadata = QdbMetadataV1;
const CURRENT_VERSION: u32 = 1;

impl QdbMetadata {
    pub fn deserialize(metadata: &[u8]) -> ParquetResult<Self> {
        let json_str = std::str::from_utf8(metadata)
            .map_err(|e| ParquetErrorCause::Utf8Decode(e).into_err())?;
        let version: VersionMetadata = serde_json::from_str(json_str)
            .map_err(|e| ParquetErrorCause::QdbMetadata(e.into()).into_err())?;
        match version.version {
            1 => serde_json::from_str(json_str)
                .map_err(|e| ParquetErrorCause::QdbMetadata(e.into()).into_err()),
            _ => Err(fmt_err!(
                Unsupported,
                "unsupported questdb metadata version: {}",
                version.version
            )),
        }
    }

    pub fn serialize(&self) -> ParquetResult<String> {
        serde_json::to_string(self)
            .map_err(|e| ParquetErrorCause::QdbMetadata(e.into()).into_err())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use super::*;
    use crate::parquet_write::schema::ColumnType;

    #[test]
    fn test_serialize() -> ParquetResult<()> {
        let metadata = QdbMetadata {
            version: U32Const,
            schema: Schema {
                columns: vec![
                    Column {
                        qdb_type_code: ColumnType::Symbol.code(),
                        handling: Some(Handling::SymbolLocalIsGlobal),
                    },
                    Column {
                        qdb_type_code: ColumnType::Int.code(),
                        handling: None,
                    },
                ],
            },
        };

        let expected = json!({
            "version": 1,
            "schema": {
                "columns": [
                    {
                        "qdb_type_code": 12,
                        "handling": "SymbolLocalIsGlobal"
                    },
                    {
                        "qdb_type_code": 5
                    }
                ]
            }
        });
        let serialized: Value = serde_json::from_str(metadata.serialize()?.as_str())
            .map_err(|e| ParquetErrorCause::QdbMetadata(e.into()).into_err())?;
        assert_eq!(serialized, expected);

        Ok(())
    }
}
