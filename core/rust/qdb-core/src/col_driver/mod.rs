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
mod mapped;
mod primitives;
mod string;
mod symbol;
mod varchar;

use crate::col_type::ColumnTypeTag;
use crate::error::CoreResult;

pub use binary::*;
pub use mapped::*;
pub use primitives::*;
pub use string::*;
pub use symbol::*;
pub use varchar::*;

pub trait ColumnDriver {
    /// Returns the data and aux file sizes at the given row.
    /// If a column is a simple type such as `INT` or `DOUBLE`, the aux size will be `None`.
    fn col_sizes_for_row(
        &self,
        col: &MappedColumn,
        row_index: u64,
    ) -> CoreResult<(u64, Option<u64>)>;
}

pub const fn lookup_driver(tag: ColumnTypeTag) -> &'static dyn ColumnDriver {
    match tag {
        ColumnTypeTag::Boolean => &BooleanDriver,
        ColumnTypeTag::Byte => &ByteDriver,
        ColumnTypeTag::Short => &ShortDriver,
        ColumnTypeTag::Char => &CharDriver,
        ColumnTypeTag::Int => &IntDriver,
        ColumnTypeTag::Long => &LongDriver,
        ColumnTypeTag::Date => &DateDriver,
        ColumnTypeTag::Timestamp => &TimestampDriver,
        ColumnTypeTag::Float => &FloatDriver,
        ColumnTypeTag::Double => &DoubleDriver,
        ColumnTypeTag::String => &StringDriver,
        ColumnTypeTag::Symbol => &SymbolDriver,
        ColumnTypeTag::Long256 => &Long256Driver,
        ColumnTypeTag::GeoByte => &GeoByteDriver,
        ColumnTypeTag::GeoShort => &GeoShortDriver,
        ColumnTypeTag::GeoInt => &GeoIntDriver,
        ColumnTypeTag::GeoLong => &GeoLongDriver,
        ColumnTypeTag::Binary => &BinaryDriver,
        ColumnTypeTag::Uuid => &UuidDriver,
        ColumnTypeTag::Long128 => &Long128Driver,
        ColumnTypeTag::IPv4 => &IPv4Driver,
        ColumnTypeTag::Varchar => &VarcharDriver,
    }
}
