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
//! The dedup sort claim of a written row group: its dedup version, with the dedup keys of its
//! first and last rows, see [`RowGroupDedupSort`].

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::convert::RowGroupDedupSort;
use crate::parquet_metadata::types::DEDUP_SORT_VERSION_UNSET;
use crate::parquet_write::schema::{Column, Partition};
use qdb_core::dedup_key::{append_key_from, append_null_key};

/// The claim of the row group that holds rows `[row_lo, row_hi)` of `partition`, in the key order
/// of dedup `version`. `key_ids` are the ids of the partition's columns that are the version's
/// non-timestamp keys, in column id order. The claim has no keys when there are none, or when a
/// key column is not in the partition.
pub fn row_group_dedup_sort(
    version: i64,
    partition: &Partition,
    key_ids: &[i32],
    row_lo: usize,
    row_hi: usize,
) -> ParquetResult<RowGroupDedupSort> {
    if version == DEDUP_SORT_VERSION_UNSET || key_ids.is_empty() || row_lo >= row_hi {
        return Ok(RowGroupDedupSort { version, keys: None });
    }
    let first = partition_row_key(partition, key_ids, row_lo)?;
    let last = partition_row_key(partition, key_ids, row_hi - 1)?;
    Ok(RowGroupDedupSort { version, keys: first.zip(last) })
}

/// The dedup key of row `row` of `partition`, from its columns with ids `key_ids`, in that order.
/// `None` when a key column is not in the partition.
pub fn partition_row_key(
    partition: &Partition,
    key_ids: &[i32],
    row: usize,
) -> ParquetResult<Option<Vec<u8>>> {
    let mut key = Vec::new();
    for &id in key_ids {
        let Some(column) = partition.columns.iter().find(|column| column.id == id) else {
            return Ok(None);
        };
        append_column_key(&mut key, column, row)?;
    }
    Ok(Some(key))
}

fn append_column_key(key: &mut Vec<u8>, column: &Column, row: usize) -> ParquetResult<()> {
    if row < column.column_top {
        append_null_key(key, column.data_type)?;
    } else {
        let aux = column
            .data_type
            .tag()
            .fixed_size()
            .is_none()
            .then_some(column.secondary_data);
        append_key_from(
            key,
            column.data_type,
            column.primary_data,
            aux,
            row - column.column_top,
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_write::schema::ParquetEncodingConfig;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};

    fn column(id: i32, tag: ColumnTypeTag, top: usize, data: &'static [u8]) -> Column {
        Column {
            id,
            name: "c",
            data_type: ColumnType::new(tag, 0),
            row_count: 3,
            column_top: top,
            primary_data: data,
            secondary_data: &[],
            symbol_offsets: &[],
            designated_timestamp: false,
            not_null_hint: false,
            strided_timestamp_16: false,
            designated_timestamp_ascending: false,
            dense_symbol_dictionary: false,
            parquet_encoding_config: ParquetEncodingConfig::default(),
        }
    }

    fn key(values: &[(ColumnTypeTag, Option<i32>)]) -> Vec<u8> {
        let mut key = Vec::new();
        for &(tag, value) in values {
            match value {
                Some(value) => append_key_from(
                    &mut key,
                    ColumnType::new(tag, 0),
                    &value.to_le_bytes(),
                    None,
                    0,
                )
                .unwrap(),
                None => append_null_key(&mut key, ColumnType::new(tag, 0)).unwrap(),
            }
        }
        key
    }

    #[test]
    fn keys_of_first_and_last_rows_in_key_id_order() {
        static SYMBOLS: [u8; 12] = [1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0];
        // One row of column top.
        static INTS: [u8; 8] = [7, 0, 0, 0, 9, 0, 0, 0];
        let partition = Partition {
            table: "t".to_owned(),
            columns: vec![
                column(4, ColumnTypeTag::Int, 1, &INTS),
                column(2, ColumnTypeTag::Symbol, 0, &SYMBOLS),
            ],
        };
        let sort = row_group_dedup_sort(5, &partition, &[2, 4], 0, 3).unwrap();
        assert_eq!(sort.version, 5);
        assert_eq!(
            sort.keys,
            Some((
                key(&[(ColumnTypeTag::Symbol, Some(1)), (ColumnTypeTag::Int, None)]),
                key(&[
                    (ColumnTypeTag::Symbol, Some(3)),
                    (ColumnTypeTag::Int, Some(9))
                ]),
            ))
        );
        // A key column the partition does not have: the version without keys.
        assert_eq!(
            row_group_dedup_sort(5, &partition, &[2, 8], 0, 3).unwrap(),
            RowGroupDedupSort { version: 5, keys: None }
        );
        // No claim, no keys.
        assert_eq!(
            row_group_dedup_sort(DEDUP_SORT_VERSION_UNSET, &partition, &[2, 4], 0, 3).unwrap(),
            RowGroupDedupSort::UNSET
        );
    }
}
