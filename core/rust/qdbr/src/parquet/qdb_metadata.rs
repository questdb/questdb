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
use std::collections::HashMap;
use std::fmt::Debug;

pub const QDB_META_KEY: &str = "questdb";

/// A constant field that serializes always as the same value in JSON.
/// On deserialization, it checks that the value is the same as the constant.
#[derive(PartialEq)]
struct U32Const<const N: u32>;

impl<const N: u32> Serialize for U32Const<N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        N.serialize(serializer)
    }
}

impl<const N: u32> Debug for U32Const<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{N}")
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
struct VersionMeta {
    pub version: u32,
}

/// Special instructions on how to handle the column data,
/// beyond the basic column type.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum QdbMetaColHandling {
    /// A symbol column where the local dictionary keys,
    /// i.e. the numeric keys in the parquet data match
    /// the QuestDB keys in the global symbol table.
    SymbolLocalIsGlobal,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct QdbMetaCol {
    /// The numeric code for the internal QuestDB column type.
    /// To convert, use `let column_type: ColumnType = col.qdb_type_code.try_into()?`.
    pub qdb_type_code: i32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub handling: Option<QdbMetaColHandling>,
}

pub type ColId = i32;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct QdbMetaSchema {
    pub(crate) columns: HashMap<ColId, QdbMetaCol>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct QdbMetaV1 {
    version: U32Const<1>,
    pub(crate) schema: QdbMetaSchema,
}

impl QdbMetaV1 {
    pub fn new() -> Self {
        Self {
            version: U32Const,
            schema: QdbMetaSchema { columns: HashMap::new() },
        }
    }
}

/// Alias to the latest version of the QuestDB-specific parquet metadata.
/// This is the only one we use in the code base.
/// Older versions upgraded to this version when read.
pub type QdbMeta = QdbMetaV1;

impl QdbMeta {
    pub fn deserialize(metadata: &str) -> ParquetResult<Self> {
        let version: VersionMeta = serde_json::from_str(metadata)
            .map_err(|e| ParquetErrorCause::QdbMeta(e.into()).into_err())?;
        match version.version {
            1 => serde_json::from_str(metadata)
                .map_err(|e| ParquetErrorCause::QdbMeta(e.into()).into_err()),
            _ => Err(fmt_err!(
                Unsupported,
                "unsupported questdb metadata version: {}",
                version.version
            )),
        }
    }

    pub fn serialize(&self) -> ParquetResult<String> {
        serde_json::to_string(self).map_err(|e| ParquetErrorCause::QdbMeta(e.into()).into_err())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_write::schema::ColumnType;
    use serde_json::{json, Value};

    #[test]
    fn test_serialize() -> ParquetResult<()> {
        let metadata = QdbMeta {
            version: U32Const,
            schema: QdbMetaSchema {
                columns: HashMap::from([
                    (
                        0,
                        QdbMetaCol {
                            qdb_type_code: ColumnType::Symbol.code(),
                            handling: Some(QdbMetaColHandling::SymbolLocalIsGlobal),
                        },
                    ),
                    (
                        1,
                        QdbMetaCol {
                            qdb_type_code: ColumnType::Int.code(),
                            handling: None,
                        },
                    ),
                ]),
            },
        };

        let expected = json!({
            "version": 1,
            "schema": {
                "columns": {
                    "0": {
                        "qdb_type_code": 12,
                        "handling": "SymbolLocalIsGlobal"
                    },
                    "1": {
                        "qdb_type_code": 5
                    }
                }
            }
        });

        let serialized_str = metadata.serialize()?;
        let serialized: Value = serde_json::from_str(serialized_str.as_str())
            .map_err(|e| ParquetErrorCause::QdbMeta(e.into()).into_err())?;

        // Check that it serializes to the expected JSON.
        assert_eq!(serialized, expected);

        // Check that it round-trips back to the original struct.
        let deserialized = QdbMeta::deserialize(&serialized_str)?;
        assert_eq!(metadata, deserialized);

        Ok(())
    }

    #[test]
    fn test_bad_version() -> ParquetResult<()> {
        let metadata = json!({
            "version": 2,
            "other_fields": ["are", "ignored"]
        });

        let serialized_str = serde_json::to_string(&metadata).unwrap();

        let err = QdbMeta::deserialize(&serialized_str).unwrap_err();
        assert!(matches!(err.get_cause(), ParquetErrorCause::Unsupported));

        let msg = err.to_string();
        assert_eq!(msg, "unsupported questdb metadata version: 2");

        Ok(())
    }
}
