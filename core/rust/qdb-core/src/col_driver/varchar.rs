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
/// # Format Overview
///
/// The `varchar` column type is implemented using two files:
/// - **Aux file:** Contains fixed-size records—one per row—that store metadata about each UTF-8 string.
///   Strings up to 9 bytes are also inlined.
/// - **Data file:** Stores the actual UTF-8 string bytes for values that are too long to be stored directly in the aux record
///   for any strings larger than 9 bytes.
///
/// The aux record has two different layouts depending on whether the string is NULL, inlined or stored in the data file.
///
/// The lowest 4 bits of the aux record contains the flags. The flags are:
/// * `INLINED`: 1
/// * `ASCII`: 2
/// * `NULL`: 4
///
/// ## NULL varchar
///
/// ```text
/// +----------+------------------+------------------+
/// | Flags    | 0-padding        | 48-bit Offset    |
/// | (4 bits) | (9 & half bytes) | (last data size) |
/// +----------+------------------+------------------+
/// ```
///
/// _Flags: `NULL` flag set_
///
/// ## Small inlined varchar (<= 9 bytes)
///
/// ```text
/// +----------+----------+---------------------+-------------+-------------------+
/// | Flags    | Size     | Inlined UTF-8 Bytes | 0-padding   | 48-bit Offset     |
/// | (4 bits) | (4 bits) | (up to 9 bytes)     | (if needed) | (last data size)  |
/// +----------+----------+---------------------+-------------+-------------------+
/// ```
///
/// _Flags: `INLINED` flag set; `ASCII` flag if applicable_
///
/// ## Long varchar (> 9 bytes)
///
/// ```text
/// +----------+------------+----------------+------------------+
/// | Flags    | UTF-8 size | Inlined prefix | 48-bit Offset    |
/// | (4 bits) | (28 bits)  | (6 bytes)      | (last data size) |
/// +----------+------------+----------------+------------------+
/// ```
///
/// _Flags: `ASCII` flag if applicable_
///
/// ## 48-bit Offset
///
/// The 48-bit offset is stored in 6 bytes in little-endian order.
/// It points to the start of the UTF-8 string in the data file for large strings,
/// or the end of the previous record for small strings and null values.
pub struct VarcharDriver;

impl ColumnDriver for VarcharDriver {
    fn col_sizes_for_row_count(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        let (data_size, aux_size) = data_and_aux_size_at(col, row_count)?;
        Ok((data_size, Some(aux_size)))
    }

    fn descr(&self) -> &'static str {
        ColumnTypeTag::Varchar.name()
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
struct VarcharAuxEntry {
    packed: u128,
}

const VARCHAR_HEADER_FLAG_INLINED: u8 = 1;
const VARCHAR_HEADER_FLAG_NULL: u8 = 4;

impl VarcharAuxEntry {
    fn flags(&self) -> u8 {
        // The flags are stored in the lowest 4 bits.
        (self.packed & 0xf) as u8
    }

    fn is_inlined(&self) -> bool {
        self.flags() & VARCHAR_HEADER_FLAG_INLINED != 0
    }

    fn size(&self) -> u32 {
        if self.is_inlined() {
            // An inlined varchar has at most 4 bits for the size.
            (self.packed as u32 >> 4) & 0xf
        } else {
            // The size at the offset in the data file.
            (self.packed as u32) >> 4
        }
    }

    fn is_null(&self) -> bool {
        self.flags() & VARCHAR_HEADER_FLAG_NULL != 0
    }

    fn offset(&self) -> u64 {
        (self.packed >> 80) as u64
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
        .ok_or_else(|| err::missing_aux(&VarcharDriver, col))?;
    let data_mmap = &col.data;
    let aux: &[VarcharAuxEntry] =
        cast_slice(&aux_mmap[..]).with_context(|_| err::bad_aux_layout(&VarcharDriver, col))?;
    let Some(aux_entry) = aux.get((row_index) as usize) else {
        return Err(err::not_found(&VarcharDriver, col, row_index));
    };

    let aux_size = (row_index + 1) * size_of::<VarcharAuxEntry>() as u64;
    let offset = aux_entry.offset();
    let data_size = if aux_entry.is_inlined() || aux_entry.is_null() {
        offset
    } else {
        offset + aux_entry.size() as u64
    };
    if (data_mmap.len() as u64) < data_size {
        return Err(err::bad_data_size(&VarcharDriver, col, data_size));
    }
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

            final String nullString = "NULL";
            final String emptyString = "''";
            final String shortStr = "'abc'";
            final String longStr = "'Lorem ipsum dolor sit amet, consectetur tincidunt.'"; // 50 bytes

            qdb.execute("create table x (v1 varchar, v2 varchar, v3 varchar, v4 varchar, timestamp_c timestamp) timestamp(timestamp_c) partition by day wal");
            qdb.execute("insert into x (v1, v2, v3, v4, timestamp_c) values " +
                    "(" + nullString + ", " + emptyString + ", " + shortStr + ", " + longStr + ", '2022-02-24T01:01:00')");
            qdb.execute("insert into x (v1, v2, v3, v4, timestamp_c) values " +
                    "(" + emptyString + ", " + shortStr + ", " + longStr + ", " + nullString + ", '2022-02-24T01:01:01')");
            qdb.execute("insert into x (v1, v2, v3, v4, timestamp_c) values " +
                    "(" + shortStr + ", " + longStr + ", " + nullString + ", " + emptyString + ", '2022-02-24T01:01:02')");
            qdb.execute("insert into x (v1, v2, v3, v4, timestamp_c) values " +
                    "(" + longStr + ", " + nullString + ", " + emptyString + ", " + longStr + ", '2022-02-24T01:01:03')");
            qdb.execute("insert into x (v1, v2, v3, v4, timestamp_c) values " +
                    "(" + nullString + ", " + emptyString + ", " + shortStr + ", " + longStr + ", '2022-02-24T01:01:04')");

        This gives the various columns different starting and ending patterns.

        IMPORTANT! ALL THE COLUMNS HAVE BEEN TRUNCATED!
        */
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/varchar");
        MappedColumn::open(parent_path, name, ColumnTypeTag::Varchar.into_type()).unwrap()
    }

    #[test]
    fn test_v1() {
        let col = map_col("v1");

        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is null string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(16));

        // index 1 is empty string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(32));

        // index 2 is a short inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(48));

        // index 3 is a 50-byte non-inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(64));

        // index 4 is a null string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = VarcharDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("varchar entry index 5 not found in aux for column v1 in"));
    }

    #[test]
    fn test_v2() {
        let col = map_col("v2");

        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is empty string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(16));

        // index 1 is a short inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(32));

        // index 2 is a 50-byte non-inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(48));

        // index 3 is a null string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(64));

        // index 4 is empty string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = VarcharDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("varchar entry index 5 not found in aux for column v2 in"));
    }

    #[test]
    fn test_v3() {
        let col = map_col("v3");

        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is a short inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(16));

        // index 1 is a 50-byte non-inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(32));

        // index 2 is a null string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(48));

        // index 3 is empty string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(64));

        // index 4 is a short inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = VarcharDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("varchar entry index 5 not found in aux for column v3 in"));
    }

    #[test]
    fn test_v4() {
        let col = map_col("v4");

        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is a 50-byte non-inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(16));

        // index 1 is a null string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 2).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(32));

        // index 2 is empty string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 3).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(48));

        // index 3 is a 50-byte non-inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 4).unwrap();
        assert_eq!(data_size, 100);
        assert_eq!(aux_size, Some(64));

        // index 4 is a 50-byte non-inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 5).unwrap();
        assert_eq!(data_size, 150);
        assert_eq!(aux_size, Some(80));

        // out of range
        let err = VarcharDriver.col_sizes_for_row_count(&col, 6).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("varchar entry index 5 not found in aux for column v4 in"));
    }

    #[test]
    fn test_vempty() {
        let col = map_col("vempty");

        let (data_size, aux_size) = VarcharDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // out of range
        let err = VarcharDriver.col_sizes_for_row_count(&col, 1).unwrap_err();
        let msg = format!("{err:#}");
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        assert!(msg.contains("varchar entry index 0 not found in aux for column vempty in"));
    }
}
