/*+*****************************************************************************
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
#![allow(dead_code)]

//! QuestDB-specific parquet footer key-value metadata.
//!
//! Lives in this shared crate so any crate that needs to serialize or
//! deserialize the on-disk representation can do so without dragging in the
//! `qdbr` error stack. `qdbr` re-exports these types via
//! `parquet::qdb_metadata`.

use crate::error::{ParquetMetaErrorKind, ParquetMetaResult};
use crate::parquet_meta_err;
use parquet2::metadata::FileMetaData;
use qdb_core::col_type::ColumnType;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

pub const QDB_META_KEY: &str = "questdb";

/// The id stored in the parquet schema (`SchemaElement::field_id`).
pub type ParquetFieldId = i32;

pub type QdbMetaSchema = Vec<QdbMetaCol>;

/// Alias to the latest version of the QuestDB-specific parquet metadata.
/// Older versions are upgraded to this version on read.
pub type QdbMeta = QdbMetaV1;

/// A constant field that serializes always as the same value in JSON. On
/// deserialization, checks that the value equals the constant.
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
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let n = u32::deserialize(deserializer)?;
        if n != N {
            return Err(serde::de::Error::custom(format!("expected {N}, got {n}")));
        }
        Ok(U32Const)
    }
}

/// Version-only header used to pick the right deserialization branch.
#[derive(Deserialize)]
struct VersionMeta {
    pub version: u32,
}

/// Special instructions on how to handle the column data, beyond the basic
/// column type.
#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum QdbMetaColFormat {
    /// For dict-encoded columns, the row-range local dict key is the same as
    /// QuestDB's global dict key. Used for symbol columns.
    LocalKeyIsGlobal = 1,
}

impl Serialize for QdbMetaColFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        (*self as u8).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for QdbMetaColFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let format = u8::deserialize(deserializer)?;
        match format {
            1 => Ok(QdbMetaColFormat::LocalKeyIsGlobal),
            _ => Err(serde::de::Error::custom(format!(
                "unsupported format: {format}"
            ))),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Copy, Clone)]
pub struct QdbMetaCol {
    // designated timestamp has TYPE_FLAG_DESIGNATED_TIMESTAMP bit set
    pub column_type: ColumnType,
    #[serde(default)]
    pub column_top: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<QdbMetaColFormat>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub ascii: Option<bool>,

    /// QuestDB's authoritative column id: the table writer index. It carries the
    /// same value QuestDB stamps into the Parquet `field_id`, but living here
    /// makes column identity independent of `field_id`. `None` for files written
    /// before this field existed; readers then fall back to the `field_id`. See
    /// [`ParquetFieldId`].
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<ParquetFieldId>,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

fn default_neg_one_i64() -> i64 {
    -1
}

fn is_neg_one_i64(v: &i64) -> bool {
    *v == -1
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct QdbMetaV1 {
    version: U32Const<1>,
    pub schema: QdbMetaSchema,

    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub unused_bytes: u64,

    #[serde(default = "default_neg_one_i64")]
    #[serde(skip_serializing_if = "is_neg_one_i64")]
    pub squash_tracker: i64,

    #[serde(default = "default_neg_one_i64")]
    #[serde(skip_serializing_if = "is_neg_one_i64")]
    pub seq_txn: i64,
}

impl QdbMetaV1 {
    pub fn new(column_count: usize) -> Self {
        Self {
            version: U32Const,
            schema: QdbMetaSchema::with_capacity(column_count),
            unused_bytes: 0,
            squash_tracker: -1,
            seq_txn: -1,
        }
    }
}

impl QdbMeta {
    pub fn deserialize(metadata: &str) -> ParquetMetaResult<Self> {
        let version: VersionMeta = serde_json::from_str(metadata).map_err(|e| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "could not parse questdb metadata: {e}"
            )
        })?;
        match version.version {
            1 => serde_json::from_str(metadata).map_err(|e| {
                parquet_meta_err!(
                    ParquetMetaErrorKind::Conversion,
                    "could not parse questdb metadata: {e}"
                )
            }),
            v => Err(parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "unsupported questdb metadata version: {v}"
            )),
        }
    }

    pub fn serialize(&self) -> ParquetMetaResult<String> {
        serde_json::to_string(self).map_err(|e| {
            parquet_meta_err!(
                ParquetMetaErrorKind::Conversion,
                "could not serialize questdb metadata: {e}"
            )
        })
    }
}

/// Reads the `"questdb"` key-value entry from a parquet footer and decodes it
/// as `QdbMeta`. Returns `Ok(None)` when the entry is missing.
pub fn extract_qdb_meta(file_metadata: &FileMetaData) -> ParquetMetaResult<Option<QdbMeta>> {
    let Some(key_value_meta) = file_metadata.key_value_metadata.as_ref() else {
        return Ok(None);
    };
    let Some(kv) = key_value_meta.iter().find(|kv| kv.key == QDB_META_KEY) else {
        return Ok(None);
    };
    let Some(json) = kv.value.as_deref() else {
        return Ok(None);
    };
    Ok(Some(QdbMeta::deserialize(json)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};
    use serde_json::{json, Value};

    pub trait ColumnTypeTagExt {
        fn into_type(self) -> ColumnType;
    }

    impl ColumnTypeTagExt for ColumnTypeTag {
        fn into_type(self) -> ColumnType {
            ColumnType::new(self, 0)
        }
    }

    #[test]
    fn test_serialize() {
        let metadata = QdbMeta {
            version: U32Const,
            schema: vec![
                QdbMetaCol {
                    column_type: ColumnType::new(ColumnTypeTag::Symbol, 0),
                    column_top: 0,
                    format: Some(QdbMetaColFormat::LocalKeyIsGlobal),
                    ascii: None,
                    id: None,
                },
                QdbMetaCol {
                    column_type: ColumnType::new(ColumnTypeTag::Int, 0),
                    column_top: 256,
                    format: None,
                    ascii: None,
                    id: None,
                },
                QdbMetaCol {
                    column_type: ColumnType::new(ColumnTypeTag::Varchar, 0),
                    column_top: 0,
                    format: None,
                    ascii: Some(true),
                    id: None,
                },
            ],
            unused_bytes: 0,
            squash_tracker: -1,
            seq_txn: -1,
        };

        let expected = json!({
            "version": 1,
            "schema": [
                {
                    "column_type": ColumnType::new(ColumnTypeTag::Symbol, 0).code(),
                    "column_top": 0,
                    "format": 1
                },
                {
                    "column_type": ColumnType::new(ColumnTypeTag::Int, 0).code(),
                    "column_top": 256
                },
                {
                    "column_type": ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
                    "column_top": 0,
                    "ascii": true
                }
            ]
        });

        let serialized_str = metadata.serialize().unwrap();
        let serialized: Value = serde_json::from_str(serialized_str.as_str()).unwrap();
        assert_eq!(serialized, expected);

        let deserialized = QdbMeta::deserialize(&serialized_str).unwrap();
        assert_eq!(metadata, deserialized);
    }

    #[test]
    fn test_deserialize_defaults() {
        let json_str = r#"{"version":1,"schema":[{"column_type":5,"column_top":0}]}"#;
        let deserialized = QdbMeta::deserialize(json_str).unwrap();
        assert_eq!(deserialized.unused_bytes, 0);
        assert_eq!(deserialized.squash_tracker, -1);
        assert_eq!(deserialized.seq_txn, -1);
    }

    #[test]
    fn test_bad_version() {
        let metadata = json!({
            "version": 2,
            "other_fields": ["are", "ignored"]
        });
        let serialized_str = serde_json::to_string(&metadata).unwrap();
        let err = QdbMeta::deserialize(&serialized_str).unwrap_err();
        assert_eq!(err.kind, ParquetMetaErrorKind::Conversion);
        assert_eq!(err.to_string(), "unsupported questdb metadata version: 2");
    }

    #[test]
    fn test_serialize_with_id() -> ParquetMetaResult<()> {
        let metadata = QdbMeta {
            version: U32Const,
            schema: vec![
                QdbMetaCol {
                    column_type: ColumnTypeTag::Int.into_type(),
                    column_top: 0,
                    format: None,
                    ascii: None,
                    id: Some(7),
                },
                QdbMetaCol {
                    column_type: ColumnTypeTag::Int.into_type(),
                    column_top: 0,
                    format: None,
                    ascii: None,
                    id: None,
                },
            ],
            unused_bytes: 0,
            squash_tracker: -1,
            seq_txn: -1,
        };

        let serialized_str = metadata.serialize()?;
        let serialized: Value = serde_json::from_str(serialized_str.as_str()).unwrap();

        // A non-negative id is serialized; None is omitted.
        assert_eq!(serialized["schema"][0]["id"], json!(7));
        assert!(serialized["schema"][1].get("id").is_none());

        // Round-trips back to the original struct.
        let deserialized = QdbMeta::deserialize(&serialized_str)?;
        assert_eq!(metadata, deserialized);

        // Backward compatibility: a blob written before the id field existed
        // has no id (None), so the reader falls back to the parquet field_id.
        let legacy = r#"{"version":1,"schema":[{"column_type":5,"column_top":0}]}"#;
        let parsed = QdbMeta::deserialize(legacy)?;
        assert_eq!(parsed.schema[0].id, None);

        Ok(())
    }

    #[test]
    fn test_serialize_with_unused_bytes() -> ParquetMetaResult<()> {
        let metadata = QdbMeta {
            version: U32Const,
            schema: vec![QdbMetaCol {
                column_type: ColumnTypeTag::Int.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            }],
            unused_bytes: 4096,
            squash_tracker: -1,
            seq_txn: -1,
        };

        let expected = json!({
            "version": 1,
            "schema": [
                {
                    "column_type": 5,
                    "column_top": 0
                }
            ],
            "unused_bytes": 4096
        });

        let serialized_str = metadata.serialize()?;
        let serialized: Value = serde_json::from_str(serialized_str.as_str()).unwrap();
        assert_eq!(serialized, expected);

        let deserialized = QdbMeta::deserialize(&serialized_str)?;
        assert_eq!(metadata, deserialized);

        Ok(())
    }

    #[test]
    fn test_deserialize_without_unused_bytes() -> ParquetMetaResult<()> {
        // Backward compatibility: old JSON without unused_bytes and squash_tracker
        // should default to 0 and -1 respectively
        let json_str = r#"{"version":1,"schema":[{"column_type":5,"column_top":0}]}"#;
        let deserialized = QdbMeta::deserialize(json_str)?;
        assert_eq!(deserialized.unused_bytes, 0);
        assert_eq!(deserialized.squash_tracker, -1);
        Ok(())
    }

    #[test]
    fn test_unused_bytes_zero_omitted() -> ParquetMetaResult<()> {
        let metadata = QdbMeta {
            version: U32Const,
            schema: vec![QdbMetaCol {
                column_type: ColumnTypeTag::Int.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            }],
            unused_bytes: 0,
            squash_tracker: -1,
            seq_txn: -1,
        };

        let serialized_str = metadata.serialize()?;
        let serialized: Value = serde_json::from_str(serialized_str.as_str()).unwrap();

        // When unused_bytes is 0, it should be omitted from JSON
        assert!(serialized.get("unused_bytes").is_none());
        // When squash_tracker is -1, it should be omitted from JSON
        assert!(serialized.get("squash_tracker").is_none());

        Ok(())
    }

    #[test]
    fn test_serialize_with_squash_tracker() -> ParquetMetaResult<()> {
        let metadata = QdbMeta {
            version: U32Const,
            schema: vec![QdbMetaCol {
                column_type: ColumnTypeTag::Int.into_type(),
                column_top: 0,
                format: None,
                ascii: None,
                id: None,
            }],
            unused_bytes: 0,
            squash_tracker: 42,
            seq_txn: -1,
        };

        let expected = json!({
            "version": 1,
            "schema": [
                {
                    "column_type": 5,
                    "column_top": 0
                }
            ],
            "squash_tracker": 42
        });

        let serialized_str = metadata.serialize()?;
        let serialized: Value = serde_json::from_str(serialized_str.as_str()).unwrap();
        assert_eq!(serialized, expected);

        let deserialized = QdbMeta::deserialize(&serialized_str)?;
        assert_eq!(metadata, deserialized);

        Ok(())
    }

    #[test]
    fn test_deserialize_ignores_unknown_fields() -> ParquetMetaResult<()> {
        // Older builds wrote a `column_structure_version` key that has since been
        // removed. Make sure deserialization still succeeds when such a key is present.
        let json_str = r#"{"version":1,"schema":[{"column_type":5,"column_top":0}],"column_structure_version":3}"#;
        let deserialized = QdbMeta::deserialize(json_str)?;
        assert_eq!(deserialized.unused_bytes, 0);
        Ok(())
    }
}
