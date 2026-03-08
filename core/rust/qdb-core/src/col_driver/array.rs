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

/// Type driver for the Varchar column type.
///
/// Refer to `ArrayTypeDriver.java` for a description of the layout.
pub struct ArrayDriver;

impl ColumnDriver for ArrayDriver {
    fn col_sizes_for_row_count(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        let (data_size, aux_size) = data_and_aux_size_at(col, row_count)?;
        Ok((data_size, Some(aux_size)))
    }

    fn descr(&self) -> &'static str {
        ColumnTypeTag::Array.name()
    }
}

/// Maximum offset that can be represented in an aux entry.
/// This is also used as a mask to ignore other bits in the aux entry.
const OFFSET_MAX: u64 = (1u64 << 48) - 1;

#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct ArrayAuxEntry {
    packed: u128,
}

impl ArrayAuxEntry {
    pub fn size(&self) -> u32 {
        (self.packed >> 64) as u32
    }

    pub fn offset(&self) -> u64 {
        (self.packed as u64) & OFFSET_MAX
    }
}

/// Return (data_size, aux_size).
fn data_and_aux_size_at(col: &MappedColumn, row_count: u64) -> CoreResult<(u64, u64)> {
    if row_count == 0 {
        return Ok((0, 0));
    }

    // To know the size required for a given row count we need to poke at size needed to
    // store the row index just before.
    let row_index = row_count - 1;

    let aux_mmap = col
        .aux
        .as_ref()
        .ok_or_else(|| err::missing_aux(&ArrayDriver, col))?;

    let data_mmap = &col.data;
    let aux: &[ArrayAuxEntry] =
        cast_slice(&aux_mmap[..]).with_context(|_| err::bad_aux_layout(&ArrayDriver, col))?;
    let Some(aux_entry) = aux.get((row_index) as usize) else {
        return Err(err::not_found(&ArrayDriver, col, row_index));
    };

    let aux_size = (row_index + 1) * size_of::<ArrayAuxEntry>() as u64;
    let data_size = aux_entry.offset() + aux_entry.size() as u64;
    if (data_mmap.len() as u64) < data_size {
        return Err(err::bad_data_size(&ArrayDriver, col, data_size));
    }
    Ok((data_size, aux_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::col_driver::ArrayDriver;
    use crate::col_type::ColumnTypeTag;
    use crate::error::CoreErrorReason;
    use std::path::PathBuf;

    fn map_col(name: &str) -> MappedColumn {
        /*

        It should be noted that the various test columns have been generated as so:

                final String nullBin = "NULL";

                final String nullArr = "NULL";
                final String emptyArr = "ARRAY[]";
                final String shortArr = "ARRAY[1.0, 2.0]";
                final String longArr = "ARRAY[" +
                        " 1.0,  2.0,  3.0,  4.0,  5.0,  6.0,  7.0,  8.0, " +
                        " 9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, " +
                        "17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0, " +
                        "25.0, 26.0, 27.0, 28.0, 29.0, 30.0, 31.0, 32.0]";

                execute("CREATE TABLE x (a1 DOUBLE[], a2 DOUBLE[], a3 DOUBLE[], a4 DOUBLE[], timestamp_c timestamp) " +
                        "timestamp(timestamp_c) partition by day wal");
                execute("insert into x (a1, a2, a3, a4, timestamp_c) values " +
                        "(" + nullArr + ", " + emptyArr + ", " + shortArr + ", " + longArr + ", '2022-02-24T01:01:00')");
                execute("insert into x (a1, a2, a3, a4, timestamp_c) values " +
                        "(" + emptyArr + ", " + shortArr + ", " + longArr + ", " + nullArr + ", '2022-02-24T01:01:01')");
                execute("insert into x (a1, a2, a3, a4, timestamp_c) values " +
                        "(" + shortArr + ", " + longArr + ", " + nullArr + ", " + emptyArr + ", '2022-02-24T01:01:02')");
                execute("insert into x (a1, a2, a3, a4, timestamp_c) values " +
                        "(" + longArr + ", " + nullArr + ", " + emptyArr + ", " + longArr + ", '2022-02-24T01:01:03')");
                execute("insert into x (a1, a2, a3, a4, timestamp_c) values " +
                        "(" + nullArr + ", " + emptyArr + ", " + shortArr + ", " + longArr + ", '2022-02-24T01:01:04')");

        This gives the various columns different starting and ending patterns.

        IMPORTANT! ALL THE COLUMNS HAVE BEEN TRUNCATED!
         */
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/array");
        MappedColumn::open(parent_path, name, ColumnTypeTag::Array.into_type()).unwrap()
    }

    #[test]
    fn test_a1() {
        let col = map_col("a1");

        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is a null array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(16));

        // index 1 is an empty array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 8); // for the array len!
        assert_eq!(aux_size, Some(32));

        // index 2 is a short 16-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 32); // 8 from before, 8 for array len, 16 bytes data.
        assert_eq!(aux_size, Some(48));

        // index 3 is a larger 256-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 296);
        assert_eq!(aux_size, Some(64));

        // index 4 is a null array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 296);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = ArrayDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("array entry index 5 not found in aux for column a1 in"));
    }

    #[test]
    fn test_a2() {
        let col = map_col("a2");

        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is an empty array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 8);
        assert_eq!(aux_size, Some(16));

        // index 1 is a short 16-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 32);
        assert_eq!(aux_size, Some(32));

        // index 2 is a larger 256-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 296);
        assert_eq!(aux_size, Some(48));

        // index 3 is a null array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 296);
        assert_eq!(aux_size, Some(64));

        // index 4 is an empty array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 304);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = ArrayDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("array entry index 5 not found in aux for column a2 in"));
    }

    #[test]
    fn test_a3() {
        let col = map_col("a3");

        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is a short 16-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 24);
        assert_eq!(aux_size, Some(16));

        // index 1 is a larger 256-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 288);
        assert_eq!(aux_size, Some(32));

        // index 2 is a null array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 288);
        assert_eq!(aux_size, Some(48));

        // index 3 is an empty array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 296);
        assert_eq!(aux_size, Some(64));

        // index 4 is a short 16-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 320);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = ArrayDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("array entry index 5 not found in aux for column a3 in"));
    }

    #[test]
    fn test_a4() {
        let col = map_col("a4");

        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is a larger 256-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 264);
        assert_eq!(aux_size, Some(16));

        // index 1 is a null array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 264);
        assert_eq!(aux_size, Some(32));

        // index 2 is an empty array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 272);
        assert_eq!(aux_size, Some(48));

        // index 3 is a larger 256-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 536);
        assert_eq!(aux_size, Some(64));

        // index 4 is a larger 256-byte array
        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 800);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = ArrayDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("array entry index 5 not found in aux for column a4 in"));
    }

    #[test]
    fn test_aempty() {
        let col = map_col("aempty");

        let (data_size, aux_size) = ArrayDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // out of range
        let err = ArrayDriver.col_sizes_for_row_count(&col, 1).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        eprintln!("{msg}");
        assert!(msg.contains("array entry index 0 not found in aux for column aempty in"));
    }
}
