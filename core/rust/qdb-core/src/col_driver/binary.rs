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
use crate::byte_util::cast_slice;
use crate::col_driver::err;
use crate::col_driver::{ColumnDriver, MappedColumn};
use crate::col_type::ColumnTypeTag;
use crate::error::{CoreErrorExt, CoreResult};

/// The `binary` column type is implemented using two files:
///
/// # Format overview
/// * **Aux file**: Indicates the location of the string in the data file.
/// * **Data file**: byte and binary buffer.
///
/// ```text
/// Aux File (N+1 entries)           Data File (N entries)
/// +--------------+                  +------------+------------+
/// |  Offset[0]   |  --->            |  Length_0  |  String_0  |
/// |  Offset[1]   |  --->            |  Length_1  |  String_1  |
/// |  Offset[2]   |  --->            |  Length_2  |  String_2  |
/// |  ...         |  --->            |  ...       |  ...       |
/// |  Offset[N]   |  --->            |  Length_N  |  String_N  |
/// |  Offset[N+1] |  (Total size)    +------------+------------+
/// +--------------+
/// ```
///
/// # The N+1 aux format
/// Each entry in the aux file is a fixed sized unsigned 64-bit integer.
/// If refers the offset in the datafile where the binary length and buffer can be read.
///
/// **IMPORTANT**: For any _N_ rows there are _N + 1_ records in the aux file.
/// The aux file contains a last additional entry for the total size of the data file.
/// This makes it easier to calculate the size of a given binary buffer in the data file for
/// a given row without reading the data file.
///
/// # The data format
/// Each entry in the data file is dynamically sized.
///
/// ```text
/// +-----------+----------------+
/// | Length    | Payload        |
/// | (8 bytes) | (Length bytes) |
/// +-----------+----------------+
/// ```
///
/// * **Length**: The first 8 bytes are a signed 64-bit integer length. This length represents
///   the number of bytes.
/// * **Payload**: The next `Length` bytes hold the binary buffer.
///
/// ## NULL value
/// If a binary is `NULL`, we _still_ have an entry in the aux file as usual.
/// In the data file, the entry's length is `-1` and there's no payload.
/// This is the only time the length can be negative.
///
/// ```text
/// +--------------+
/// | Length == -1 |
/// | (8 bytes)    |
/// +--------------+
/// ```
pub struct BinaryDriver;

impl ColumnDriver for BinaryDriver {
    fn col_sizes_for_row_count(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        let (data_size, aux_size) = data_and_aux_size_at(col, row_count)?;
        Ok((data_size, Some(aux_size)))
    }

    fn descr(&self) -> &'static str {
        ColumnTypeTag::Binary.name()
    }
}

/// Return (data_size, aux_size).
fn data_and_aux_size_at(col: &MappedColumn, row_count: u64) -> CoreResult<(u64, u64)> {
    // Main logic
    let aux_mmap = col
        .aux
        .as_ref()
        .ok_or_else(|| err::missing_aux(&BinaryDriver, col))?;
    let aux: &[u64] =
        cast_slice(&aux_mmap[..]).with_context(|_| err::bad_aux_layout(&BinaryDriver, col))?;

    let required_aux_entry_count = row_count + 1; // N + 1 logic
    if aux.len() < required_aux_entry_count as usize {
        return Err(err::not_found(&BinaryDriver, col, row_count));
    }

    let data_size = aux[row_count as usize];
    if (col.data.len() as u64) < data_size {
        return Err(err::bad_data_size(&BinaryDriver, col, data_size));
    }

    let aux_size = required_aux_entry_count * (size_of::<u64>() as u64);
    Ok((data_size, aux_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::col_type::ColumnTypeTag;
    use crate::error::CoreErrorReason;
    use std::path::PathBuf;

    fn map_col(name: &str) -> MappedColumn {
        /*

        It should be noted that the various test columns have been generated as so:

                final String nullBin = "NULL";
                final String emptyBin = "from_base64('')";

                // b'abc'
                final String shortBin = "from_base64('YWJj')";

                // All bits set for 50 bytes.
                final String longBin = "from_base64('//////////////////////////////////////////////////////////////////8=')";

                qdb.execute("create table x (b1 binary, timestamp_c timestamp) timestamp(timestamp_c) partition by day wal");
                qdb.execute("insert into x (b1, timestamp_c) values " +
                        "(" + nullBin + ", '2022-02-24T01:01:00')");
                qdb.execute("insert into x (b1, timestamp_c) values " +
                        "(" + emptyBin + ", '2022-02-24T01:01:01')");
                qdb.execute("insert into x (b1, timestamp_c) values " +
                        "(" + shortBin + ", '2022-02-24T01:01:02')");
                qdb.execute("insert into x (b1, timestamp_c) values " +
                        "(" + longBin + ", '2022-02-24T01:01:03')");
                qdb.execute("insert into x (b1, timestamp_c) values " +
                        "(" + nullBin + ", '2022-02-24T01:01:04')");

        This gives the various columns different starting and ending patterns.

        IMPORTANT! ALL THE COLUMNS HAVE BEEN TRUNCATED!
         */
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/binary");
        MappedColumn::open(parent_path, name, ColumnTypeTag::Binary.into_type()).unwrap()
    }

    #[test]
    fn test_b1() {
        let col = map_col("b1");

        let (data_size, aux_size) = BinaryDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(8));

        // index 0 is null binary
        let (data_size, aux_size) = BinaryDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 8);
        assert_eq!(aux_size, Some(16));

        // index 1 is empty binary
        let (data_size, aux_size) = BinaryDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 16);
        assert_eq!(aux_size, Some(24));

        // index 2 is a 3-byte binary
        let (data_size, aux_size) = BinaryDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 27);
        assert_eq!(aux_size, Some(32));

        // index 3 is a 50-byte binary
        let (data_size, aux_size) = BinaryDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 85);
        assert_eq!(aux_size, Some(40));

        // index 4 is a null binary
        let (data_size, aux_size) = BinaryDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 93);
        assert_eq!(aux_size, Some(48));

        // out of range
        let err = BinaryDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        // eprintln!("{}", &msg);
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("binary entry index 6 not found in aux for column b1 in"));
    }

    #[test]
    fn test_bempty() {
        let col = map_col("bempty");

        let (data_size, aux_size) = BinaryDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(8));

        // out of range
        let err = BinaryDriver.col_sizes_for_row_count(&col, 1).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        // eprintln!("{msg}");
        assert!(msg.contains("binary entry index 1 not found in aux for column bempty in"));
    }
}
