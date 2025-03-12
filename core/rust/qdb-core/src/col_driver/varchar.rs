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
use crate::col_driver::{ColumnDriver, MappedColumn};
use crate::error::{CoreResult, fmt_err};
use std::intrinsics::transmute;

pub struct VarcharDriver;

impl ColumnDriver for VarcharDriver {
    fn col_sizes_for_size(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        let (data_size, aux_size) = data_and_aux_size_at(col, row_count)?;
        Ok((data_size, Some(aux_size)))
    }
}

#[repr(transparent)]
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

    let aux_mmap = col.aux.as_ref().expect("varchar has aux");
    let data_mmap = &col.data;
    if aux_mmap.len() % size_of::<u128>() != 0 {
        return Err(fmt_err!(
            InvalidColumnData,
            "varchar aux len {} not a multiple of 16 bytes for column {} in {}",
            aux_mmap.len(),
            col.col_name,
            col.parent_path.display()
        ));
    }
    let aux: &[VarcharAuxEntry] = unsafe { transmute(&aux_mmap[..]) };
    let Some(aux_entry) = aux.get((row_index) as usize) else {
        return Err(fmt_err!(
            InvalidColumnData,
            "varchar row index {} not found in aux for column {} in {}",
            row_index,
            col.col_name,
            col.parent_path.display()
        ));
    };

    let aux_size = (row_index + 1) * size_of::<VarcharAuxEntry>() as u64;
    let offset = aux_entry.offset();
    eprintln!("data_and_aux_size_at :: (A)");
    let data_size = if aux_entry.is_inlined() || aux_entry.is_null() {
        eprintln!("data_and_aux_size_at :: (B)");
        offset
    } else {
        eprintln!(
            "data_and_aux_size_at :: (C): aux_entry.packed: {:032X}, aux_entry.size(): {}",
            aux_entry.packed,
            aux_entry.size()
        );
        offset + aux_entry.size() as u64
    };
    if (data_mmap.len() as u64) < data_size {
        return Err(fmt_err!(
            InvalidColumnData,
            "varchar data size {} exceeds data mmap len {} for column {} in {}",
            data_size,
            data_mmap.len(),
            col.col_name,
            col.parent_path.display()
        ));
    }
    Ok((data_size, aux_size))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::col_type::ColumnTypeTag;
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
        */
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/varchar");
        MappedColumn::open(parent_path, name, ColumnTypeTag::Varchar.into_type()).unwrap()
    }

    #[test]
    fn test_v1() {
        let col = map_col("v1");
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_size(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(0));

        // index 0 is null string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_size(&col, 1).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(16));

        // index 1 is empty string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_size(&col, 2).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(32));

        // index 2 is a short inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_size(&col, 3).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, Some(48));

        // index 3 is a 50-byte non-inlined string
        let (data_size, aux_size) = VarcharDriver.col_sizes_for_size(&col, 4).unwrap();
        assert_eq!(data_size, 50);
        assert_eq!(aux_size, Some(64));
    }
}
