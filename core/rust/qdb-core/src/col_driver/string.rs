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
use crate::col_driver::util::cast_slice;
use crate::col_driver::{ColumnDriver, MappedColumn};
use crate::error::{CoreErrorExt, CoreResult};

/// The `string` column type is implemented using two files:
///
/// # Format overview
/// * **Aux file**: Indicates the location of the string in the data file.
/// * **Data file**: length and UTF-16LE string buffer.
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
/// If refers the offset in the datafile where the string length and buffer can be read.
///
/// **IMPORTANT**: For any _N_ rows there are _N + 1_ records in the aux file.
/// The aux file contains a last additional entry for the total size of the data file.
/// This makes it easier to calculate the size of a given string buffer in the data file for
/// a given row without reading the data file.
///
/// # The data format
/// Each entry in the data file is dynamically sized.
///
/// ```text
/// +-----------+--------------------+
/// | Length    | Payload            |
/// | (4 bytes) | (Length * 2 bytes) |
/// +-----------+--------------------+
/// ```
///
/// * **Length**: The first 4 bytes are a signed 32-bit integer length. This length represents the number of UTF-16LE
///   code units.
/// * **Payload**: The next `Length * 2` bytes hold the UTF-16LE buffer.
///
/// ## NULL value
/// If a string is `NULL`, we _still_ have an entry in the aux file as usual.
/// In the data file, the entry's length is `-1` and there's no payload.
/// This is the only time the length can be negative.
///
/// ```text
/// +--------------+
/// | Length == -1 |
/// | (4 bytes)    |
/// +--------------+
/// ```
pub struct StringDriver;

impl ColumnDriver for StringDriver {
    fn col_sizes_for_size(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        let (data_size, aux_size) = data_and_aux_size_at(col, row_count)?;
        Ok((data_size, Some(aux_size)))
    }
}

pub(super) mod err {
    use crate::col_driver::MappedColumn;
    use crate::error::{CoreError, fmt_err};

    pub(super) fn missing_aux(col: &MappedColumn) -> CoreError {
        fmt_err!(
            InvalidColumnData,
            "string driver expects aux mapping, but missing for {} column {} in {}",
            col.col_type,
            col.col_name,
            col.parent_path.display()
        )
    }

    pub(super) fn bad_aux_layout(col: &MappedColumn) -> String {
        format!(
            "bad layout of string aux column {} in {}",
            col.col_name,
            col.parent_path.display()
        )
    }

    pub(super) fn not_found(col: &MappedColumn, index: u64) -> CoreError {
        fmt_err!(
            InvalidColumnData,
            "string entry index {} not found in aux for column {} in {}",
            index,
            col.col_name,
            col.parent_path.display()
        )
    }

    pub(super) fn bad_data_size(col: &MappedColumn, data_size: u64) -> CoreError {
        fmt_err!(
            InvalidColumnData,
            "string required data size {} exceeds data mmap len {} for column {} in {}",
            data_size,
            col.data.len(),
            col.col_name,
            col.parent_path.display()
        )
    }
}

/// Return (data_size, aux_size).
fn data_and_aux_size_at(col: &MappedColumn, row_count: u64) -> CoreResult<(u64, u64)> {
    // Main logic
    let aux_mmap = col.aux.as_ref().ok_or_else(|| err::missing_aux(col))?;
    let aux: &[u64] = cast_slice(&aux_mmap[..]).with_context(|_| err::bad_aux_layout(col))?;

    let required_aux_entry_count = row_count + 1; // N + 1 logic
    if aux.len() < required_aux_entry_count as usize {
        return Err(err::not_found(col, row_count));
    }

    let data_size = aux[row_count as usize - 1];
    if (col.data.len() as u64) < data_size {
        return Err(err::bad_data_size(col, data_size));
    }

    let aux_size = required_aux_entry_count * (size_of::<u64>() as u64);
    Ok((data_size, aux_size))
}
