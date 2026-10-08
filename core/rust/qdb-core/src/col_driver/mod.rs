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
mod array;
mod binary;
mod designated_timestamp;
mod err;
mod mapped;
mod primitives;
mod string;
mod varchar;

use crate::col_type::{ColumnType, ColumnTypeTag};
use crate::error::{CoreResult, fmt_err};

pub use array::*;
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
    fn col_sizes_for_row_count(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)>;

    fn descr(&self) -> &'static str;
}

/// Obtain a type driver from the provided column type.
pub fn try_lookup_driver(col_type: ColumnType) -> CoreResult<&'static dyn ColumnDriver> {
    match (col_type.tag(), col_type.is_designated_timestamp_ascending()) {
        (ColumnTypeTag::Boolean, _) => Ok(&BooleanDriver),
        (ColumnTypeTag::Byte, _) => Ok(&ByteDriver),
        (ColumnTypeTag::Short, _) => Ok(&ShortDriver),
        (ColumnTypeTag::Char, _) => Ok(&CharDriver),
        (ColumnTypeTag::Int, _) => Ok(&IntDriver),
        (ColumnTypeTag::Long, _) => Ok(&LongDriver),
        (ColumnTypeTag::Date, _) => Ok(&DateDriver),
        (ColumnTypeTag::Timestamp, false) => Ok(&TimestampDriver),
        (ColumnTypeTag::Timestamp, true) => Ok(&DesignatedTimestampDriver),
        (ColumnTypeTag::Float, _) => Ok(&FloatDriver),
        (ColumnTypeTag::Double, _) => Ok(&DoubleDriver),
        (ColumnTypeTag::String, _) => Ok(&StringDriver),
        (ColumnTypeTag::Symbol, _) => Ok(&SymbolDriver),
        (ColumnTypeTag::Long256, _) => Ok(&Long256Driver),
        (ColumnTypeTag::GeoByte, _) => Ok(&GeoByteDriver),
        (ColumnTypeTag::GeoShort, _) => Ok(&GeoShortDriver),
        (ColumnTypeTag::GeoInt, _) => Ok(&GeoIntDriver),
        (ColumnTypeTag::GeoLong, _) => Ok(&GeoLongDriver),
        (ColumnTypeTag::Binary, _) => Ok(&BinaryDriver),
        (ColumnTypeTag::Uuid, _) => Ok(&UuidDriver),
        (ColumnTypeTag::Long128, _) => Ok(&Long128Driver),
        (ColumnTypeTag::IPv4, _) => Ok(&IPv4Driver),
        (ColumnTypeTag::Varchar, _) => Ok(&VarcharDriver),
        (ColumnTypeTag::VarcharSlice, _) => Err(fmt_err!(
            InvalidType,
            "VarcharSlice is a transient in-memory type with no on-disk column driver"
        )),
        (ColumnTypeTag::Array, _) => Ok(&ArrayDriver),
        (ColumnTypeTag::Decimal8, _) => Ok(&Decimal8Driver),
        (ColumnTypeTag::Decimal16, _) => Ok(&Decimal16Driver),
        (ColumnTypeTag::Decimal32, _) => Ok(&Decimal32Driver),
        (ColumnTypeTag::Decimal64, _) => Ok(&Decimal64Driver),
        (ColumnTypeTag::Decimal128, _) => Ok(&Decimal128Driver),
        (ColumnTypeTag::Decimal256, _) => Ok(&Decimal256Driver),
    }
}

/// On-disk data-file row width of a fixed-size column, or `None` for a variable-size column.
///
/// This is the authoritative width for sizing a column's `.d` file from a row count WITHOUT
/// mapping the file: `data_size = fixed_data_row_width(col_type) * row_count`. It mirrors the
/// driver dispatch in [`try_lookup_driver`] and, crucially, encodes the one place a driver's
/// on-disk row width diverges from [`ColumnTypeTag::fixed_size`]: an ascending designated
/// timestamp stores a 16-byte (timestamp, row-index) record per row in its segment `.d` file
/// (see [`DesignatedTimestampDriver`]), not the tag's 8. Keeping that special case here, next to
/// the dispatch it must agree with, is what lets callers size fixed columns arithmetically
/// without re-deriving per-type widths and drifting from the drivers.
///
/// Returns `None` for variable-size types (varchar/string/binary/array): their data size depends
/// on the aux file and cannot be computed from the row count alone.
pub fn fixed_data_row_width(col_type: ColumnType) -> Option<u64> {
    if col_type.tag() == ColumnTypeTag::Timestamp && col_type.is_designated_timestamp_ascending() {
        return Some(16);
    }
    col_type.tag().fixed_size().map(|width| width as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lookup_driver() {
        let cases = vec![
            (ColumnTypeTag::Boolean.into_type(), "boolean"),
            (ColumnTypeTag::Byte.into_type(), "byte"),
            (ColumnTypeTag::Short.into_type(), "short"),
            (ColumnTypeTag::Char.into_type(), "char"),
            (ColumnTypeTag::Int.into_type(), "int"),
            (ColumnTypeTag::Long.into_type(), "long"),
            (ColumnTypeTag::Date.into_type(), "date"),
            (ColumnTypeTag::Timestamp.into_type(), "timestamp"),
            (
                ColumnTypeTag::Timestamp
                    .into_type()
                    .into_designated()
                    .unwrap(),
                "designated-timestamp",
            ),
            (ColumnTypeTag::Float.into_type(), "float"),
            (ColumnTypeTag::Double.into_type(), "double"),
            (ColumnTypeTag::String.into_type(), "string"),
            (ColumnTypeTag::Symbol.into_type(), "symbol"),
            (ColumnTypeTag::Long256.into_type(), "long256"),
            (ColumnTypeTag::GeoByte.into_type(), "geobyte"),
            (ColumnTypeTag::GeoShort.into_type(), "geoshort"),
            (ColumnTypeTag::GeoInt.into_type(), "geoint"),
            (ColumnTypeTag::GeoLong.into_type(), "geolong"),
            (ColumnTypeTag::Binary.into_type(), "binary"),
            (ColumnTypeTag::Uuid.into_type(), "uuid"),
            (ColumnTypeTag::Long128.into_type(), "long128"),
            (ColumnTypeTag::IPv4.into_type(), "ipv4"),
            (ColumnTypeTag::Varchar.into_type(), "varchar"),
            (ColumnTypeTag::Array.into_type(), "array"),
            (ColumnTypeTag::Decimal8.into_type(), "decimal8"),
            (ColumnTypeTag::Decimal16.into_type(), "decimal16"),
            (ColumnTypeTag::Decimal32.into_type(), "decimal32"),
            (ColumnTypeTag::Decimal64.into_type(), "decimal64"),
            (ColumnTypeTag::Decimal128.into_type(), "decimal128"),
            (ColumnTypeTag::Decimal256.into_type(), "decimal256"),
        ];
        for (col_type, exp_descr) in cases.iter().copied() {
            let driver = try_lookup_driver(col_type).unwrap();
            let actual_descr = driver.descr();
            assert_eq!(actual_descr, exp_descr);
        }
    }

    #[test]
    fn test_fixed_data_row_width() {
        // Plain fixed-size types match the tag width.
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::Boolean.into_type()),
            Some(1)
        );
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::Int.into_type()),
            Some(4)
        );
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::Long.into_type()),
            Some(8)
        );
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::Long256.into_type()),
            Some(32)
        );
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::Timestamp.into_type()),
            Some(8)
        );

        // The one divergence: an ascending designated timestamp stores 16 bytes/row, not 8 -
        // this is the value that must agree with DesignatedTimestampDriver.
        let designated = ColumnTypeTag::Timestamp
            .into_type()
            .into_designated()
            .unwrap();
        assert_eq!(fixed_data_row_width(designated), Some(16));

        // Variable-size types have no row-count-derivable data width.
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::Varchar.into_type()),
            None
        );
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::String.into_type()),
            None
        );
        assert_eq!(
            fixed_data_row_width(ColumnTypeTag::Binary.into_type()),
            None
        );
        assert_eq!(fixed_data_row_width(ColumnTypeTag::Array.into_type()), None);
    }
}
