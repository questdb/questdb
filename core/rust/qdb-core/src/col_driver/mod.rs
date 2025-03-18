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
mod binary;
mod designated_timestamp;
mod err;
mod mapped;
mod primitives;
mod string;
mod varchar;

use crate::col_type::{ColumnType, ColumnTypeTag};
use crate::error::CoreResult;

pub use binary::*;
pub use designated_timestamp::*;
pub use mapped::*;
pub use primitives::*;
pub use string::*;
pub use varchar::*;

pub const DATA_FILE_EXTENSION: &str = "d";
pub const AUX_FILE_EXTENSION: &str = "i";

pub trait ColumnDriver {
    /// Returns the data and aux file sizes for the given row count.
    /// If a column is a simple type such as `INT` or `DOUBLE`, the aux size will be `None`.
    fn col_sizes_for_size(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)>;

    fn descr(&self) -> &'static str;
}

pub fn lookup_driver(col_type: ColumnType) -> &'static dyn ColumnDriver {
    match (col_type.tag(), col_type.is_designated()) {
        (ColumnTypeTag::Boolean, _) => &BooleanDriver,
        (ColumnTypeTag::Byte, _) => &ByteDriver,
        (ColumnTypeTag::Short, _) => &ShortDriver,
        (ColumnTypeTag::Char, _) => &CharDriver,
        (ColumnTypeTag::Int, _) => &IntDriver,
        (ColumnTypeTag::Long, _) => &LongDriver,
        (ColumnTypeTag::Date, _) => &DateDriver,
        (ColumnTypeTag::Timestamp, false) => &TimestampDriver,
        (ColumnTypeTag::Timestamp, true) => &DesignatedTimestampDriver,
        (ColumnTypeTag::Float, _) => &FloatDriver,
        (ColumnTypeTag::Double, _) => &DoubleDriver,
        (ColumnTypeTag::String, _) => &StringDriver,
        (ColumnTypeTag::Symbol, _) => &SymbolDriver,
        (ColumnTypeTag::Long256, _) => &Long256Driver,
        (ColumnTypeTag::GeoByte, _) => &GeoByteDriver,
        (ColumnTypeTag::GeoShort, _) => &GeoShortDriver,
        (ColumnTypeTag::GeoInt, _) => &GeoIntDriver,
        (ColumnTypeTag::GeoLong, _) => &GeoLongDriver,
        (ColumnTypeTag::Binary, _) => &BinaryDriver,
        (ColumnTypeTag::Uuid, _) => &UuidDriver,
        (ColumnTypeTag::Long128, _) => &Long128Driver,
        (ColumnTypeTag::IPv4, _) => &IPv4Driver,
        (ColumnTypeTag::Varchar, _) => &VarcharDriver,
    }
}
