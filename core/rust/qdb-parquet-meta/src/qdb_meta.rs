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
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
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

/// What the chunk null counts of a parquet file say about the column top that its QuestDB
/// metadata gives a column, see [`column_top_evidence`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnTopEvidence {
    /// Every chunk below the top counts its rows below the top as NULLs.
    Verified,
    /// A chunk below the top counts fewer NULLs than it has rows below the top, so it holds a
    /// value below the top: the top is stale.
    Contradicted,
    /// The null counts cannot tell.
    Unverifiable,
}

/// Checks the column top that the QuestDB metadata gives column `column_index`, of type
/// `column_type`, against the chunk null counts of the file.
///
/// QuestDB 8.3.0 to 9.3.3 merged O3 rows into a parquet row group in place and kept the footer
/// metadata of the file, so its top can cover rows that a merge gave values. A column without a
/// NULL sentinel (BOOLEAN, BYTE, SHORT, CHAR) is NULL only in its column top, and the chunk null
/// count is the column top of the chunk, the count the `_pm` decoder reads as such
/// (`prepare_chunk`): an Optional chunk counts its NULL definition levels, and a Required BYTE,
/// SHORT or CHAR chunk counts the column top it was encoded with, 0 for a row group that a merge
/// re-encoded from its first row. A Required BOOLEAN chunk counts no NULL even below its top, and
/// a type with a NULL sentinel also counts its NULLs past the top, so the counts cannot tell for
/// these, nor for a chunk without a null count. A negative count is unknown, as in the `_pm`.
pub fn column_top_evidence(
    file_metadata: &FileMetaData,
    column_index: usize,
    column_type: ColumnType,
    column_top: usize,
) -> ColumnTopEvidence {
    if column_top == 0 {
        return ColumnTopEvidence::Verified;
    }
    let tag = column_type.tag();
    if !matches!(
        tag,
        ColumnTypeTag::Boolean | ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Char
    ) {
        return ColumnTopEvidence::Unverifiable;
    }
    let is_required = file_metadata
        .schema_descr
        .columns()
        .get(column_index)
        .is_none_or(|column| column.descriptor.max_def_level == 0);
    if tag == ColumnTypeTag::Boolean && is_required {
        return ColumnTopEvidence::Unverifiable;
    }
    let mut evidence = ColumnTopEvidence::Verified;
    let mut row_group_lo = 0usize;
    for row_group in &file_metadata.row_groups {
        if row_group_lo >= column_top {
            break;
        }
        let row_group_hi = row_group_lo + row_group.num_rows();
        let rows_below_top = (column_top.min(row_group_hi) - row_group_lo) as u64;
        let null_count = row_group
            .columns()
            .get(column_index)
            .and_then(|chunk| chunk.metadata().statistics.as_ref())
            .and_then(|statistics| statistics.null_count)
            .and_then(|null_count| u64::try_from(null_count).ok());
        match null_count {
            Some(null_count) if null_count < rows_below_top => {
                return ColumnTopEvidence::Contradicted;
            }
            Some(_) => {}
            None => evidence = ColumnTopEvidence::Unverifiable,
        }
        row_group_lo = row_group_hi;
    }
    evidence
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

    /// The footer metadata of a file with one column of physical type `physical_type` and the
    /// given row groups, each its row count and the chunk null count.
    fn single_column_file(
        physical_type: parquet2::thrift_format::Type,
        repetition: parquet2::thrift_format::FieldRepetitionType,
        row_groups: &[(i64, Option<i64>)],
    ) -> FileMetaData {
        use parquet2::thrift_format as thrift;
        let schema = vec![
            thrift::SchemaElement::new(
                None,
                None,
                None,
                "t".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
            ),
            thrift::SchemaElement::new(
                physical_type,
                None,
                repetition,
                "c".to_string(),
                None,
                None,
                None,
                None,
                0,
                None,
            ),
        ];
        let row_groups = row_groups
            .iter()
            .map(|&(num_rows, null_count)| {
                let statistics = thrift::Statistics {
                    max: None,
                    min: None,
                    null_count,
                    distinct_count: None,
                    max_value: None,
                    min_value: None,
                    is_max_value_exact: None,
                    is_min_value_exact: None,
                };
                let meta_data = thrift::ColumnMetaData::new(
                    physical_type,
                    vec![thrift::Encoding::PLAIN],
                    vec!["c".to_string()],
                    thrift::CompressionCodec::UNCOMPRESSED,
                    num_rows,
                    0,
                    0,
                    None,
                    4,
                    None,
                    None,
                    statistics,
                    None,
                    None,
                    None,
                );
                let chunk = thrift::ColumnChunk::new(
                    None, 4, meta_data, None, None, None, None, None, None,
                );
                thrift::RowGroup::new(vec![chunk], 0, num_rows, None, None, None, None)
            })
            .collect::<Vec<_>>();
        let num_rows = row_groups.iter().map(|rg| rg.num_rows).sum();
        FileMetaData::try_from_thrift(thrift::FileMetaData::new(
            1, schema, num_rows, row_groups, None, None, None, None, None,
        ))
        .expect("valid footer")
    }

    fn evidence(
        tag: ColumnTypeTag,
        repetition: parquet2::thrift_format::FieldRepetitionType,
        row_groups: &[(i64, Option<i64>)],
        column_top: usize,
    ) -> ColumnTopEvidence {
        use parquet2::thrift_format::Type;
        let physical_type = if tag == ColumnTypeTag::Boolean {
            Type::BOOLEAN
        } else {
            Type::INT32
        };
        let file = single_column_file(physical_type, repetition, row_groups);
        column_top_evidence(&file, 0, tag.into_type(), column_top)
    }

    #[test]
    fn column_top_evidence_verifies_required_chunks_counting_their_top() {
        use parquet2::thrift_format::FieldRepetitionType;
        // 9.3.3 and 9.4.x encode a Required BYTE, SHORT or CHAR top with its rows as the null
        // count: top 6 over row groups of 4 rows
        for tag in [
            ColumnTypeTag::Byte,
            ColumnTypeTag::Short,
            ColumnTypeTag::Char,
        ] {
            assert_eq!(
                evidence(
                    tag,
                    FieldRepetitionType::REQUIRED,
                    &[(4, Some(4)), (4, Some(2)), (4, Some(0))],
                    6
                ),
                ColumnTopEvidence::Verified,
                "{tag:?}"
            );
        }
        // the top ends at a row group boundary, and past the last row
        assert_eq!(
            evidence(
                ColumnTypeTag::Byte,
                FieldRepetitionType::REQUIRED,
                &[(4, Some(4)), (4, Some(0))],
                4
            ),
            ColumnTopEvidence::Verified
        );
        assert_eq!(
            evidence(
                ColumnTypeTag::Byte,
                FieldRepetitionType::REQUIRED,
                &[(4, Some(4)), (4, Some(4))],
                100
            ),
            ColumnTopEvidence::Verified
        );
        assert_eq!(
            evidence(
                ColumnTypeTag::Byte,
                FieldRepetitionType::REQUIRED,
                &[(4, None)],
                0
            ),
            ColumnTopEvidence::Verified
        );
    }

    #[test]
    fn column_top_evidence_contradicts_top_of_chunk_a_merge_rewrote() {
        use parquet2::thrift_format::FieldRepetitionType;
        // a 9.3.3 merge re-encodes the row group from its first row: null count 0
        for tag in [
            ColumnTypeTag::Byte,
            ColumnTypeTag::Short,
            ColumnTypeTag::Char,
        ] {
            assert_eq!(
                evidence(tag, FieldRepetitionType::REQUIRED, &[(7, Some(0))], 4),
                ColumnTopEvidence::Contradicted,
                "{tag:?}"
            );
        }
        // row group 1 straddles the top and lost its 2 NULLs
        assert_eq!(
            evidence(
                ColumnTypeTag::Byte,
                FieldRepetitionType::REQUIRED,
                &[(4, Some(4)), (5, Some(0)), (4, Some(0))],
                6
            ),
            ColumnTopEvidence::Contradicted
        );
        // one NULL short of the overlap
        assert_eq!(
            evidence(
                ColumnTypeTag::Short,
                FieldRepetitionType::OPTIONAL,
                &[(4, Some(4)), (4, Some(1))],
                6
            ),
            ColumnTopEvidence::Contradicted
        );
        // an Optional BOOLEAN counts its NULL definition levels
        assert_eq!(
            evidence(
                ColumnTypeTag::Boolean,
                FieldRepetitionType::OPTIONAL,
                &[(7, Some(0))],
                4
            ),
            ColumnTopEvidence::Contradicted
        );
        assert_eq!(
            evidence(
                ColumnTypeTag::Boolean,
                FieldRepetitionType::OPTIONAL,
                &[(7, Some(4))],
                4
            ),
            ColumnTopEvidence::Verified
        );
        // a contradicting chunk outweighs one without a null count
        assert_eq!(
            evidence(
                ColumnTypeTag::Byte,
                FieldRepetitionType::REQUIRED,
                &[(4, None), (4, Some(0))],
                6
            ),
            ColumnTopEvidence::Contradicted
        );
    }

    #[test]
    fn column_top_evidence_cannot_verify_without_counted_nulls() {
        use parquet2::thrift_format::FieldRepetitionType;
        // a Required BOOLEAN chunk counts no NULL below its top, legit or stale
        assert_eq!(
            evidence(
                ColumnTypeTag::Boolean,
                FieldRepetitionType::REQUIRED,
                &[(7, Some(0))],
                4
            ),
            ColumnTopEvidence::Unverifiable
        );
        // a type with a NULL sentinel counts its other NULLs too
        assert_eq!(
            evidence(
                ColumnTypeTag::Int,
                FieldRepetitionType::OPTIONAL,
                &[(7, Some(0))],
                4
            ),
            ColumnTopEvidence::Unverifiable
        );
        // no null count, or a negative one, which the `_pm` reads as unknown
        for null_count in [None, Some(-1), Some(i64::MIN)] {
            assert_eq!(
                evidence(
                    ColumnTypeTag::Byte,
                    FieldRepetitionType::REQUIRED,
                    &[(4, Some(4)), (4, null_count)],
                    6
                ),
                ColumnTopEvidence::Unverifiable,
                "{null_count:?}"
            );
        }
    }
}
