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
use crate::error::CoreResult;

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
        _col: &MappedColumn,
        _row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        todo!()
    }
}
