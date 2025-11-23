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
use crate::col_type::ColumnTypeTag;
use crate::error::{CoreResult, fmt_err};
use paste::paste;

macro_rules! impl_primitive_type_driver {
    ($tag:ident) => {
        paste! {
            pub struct [<$tag Driver>];

            impl ColumnDriver for [<$tag Driver>] {
                fn col_sizes_for_row_count(&self, col: &MappedColumn, row_count: u64) -> CoreResult<(u64, Option<u64>)> {
                    assert!(!ColumnTypeTag::$tag.is_var_size());
                    let row_size = ColumnTypeTag::$tag.fixed_size().expect("fixed size column") as u64;
                    let data_size = row_size * row_count;
                    if data_size > col.data.len() as u64 {
                        return Err(fmt_err!(
                            InvalidLayout,
                            "data file for {} column {} shorter than {} rows, expected at least {} bytes but is {} at {}",
                            self.descr(),
                            col.col_name,
                            row_count,
                            data_size,
                            col.data.len(),
                            col.parent_path.display()
                        ));
                    }
                    Ok((data_size, None))
                }

                fn descr(&self) -> &'static str {
                    ColumnTypeTag::$tag.name()
                }
            }
        }
    };
}

impl_primitive_type_driver!(Boolean);
impl_primitive_type_driver!(Byte);
impl_primitive_type_driver!(Short);
impl_primitive_type_driver!(Char);
impl_primitive_type_driver!(Int);
impl_primitive_type_driver!(Long);
impl_primitive_type_driver!(Date);
impl_primitive_type_driver!(Timestamp);
impl_primitive_type_driver!(Float);
impl_primitive_type_driver!(Double);
impl_primitive_type_driver!(Symbol); // Treating as primitive is enough for the current needs
impl_primitive_type_driver!(Long256);
impl_primitive_type_driver!(GeoByte);
impl_primitive_type_driver!(GeoShort);
impl_primitive_type_driver!(GeoInt);
impl_primitive_type_driver!(GeoLong);
impl_primitive_type_driver!(Uuid);
impl_primitive_type_driver!(Long128);
impl_primitive_type_driver!(IPv4);
impl_primitive_type_driver!(Decimal8);
impl_primitive_type_driver!(Decimal16);
impl_primitive_type_driver!(Decimal32);
impl_primitive_type_driver!(Decimal64);
impl_primitive_type_driver!(Decimal128);
impl_primitive_type_driver!(Decimal256);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::col_type::ColumnType;
    use crate::error::CoreErrorReason;
    use std::path::PathBuf;

    fn map_col(name: &str, col_type: ColumnType) -> MappedColumn {
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/primitives");
        MappedColumn::open(parent_path, name, col_type).unwrap()
    }

    #[test]
    fn test_int_col0() {
        // Empty column
        let col = map_col("int_col0", ColumnTypeTag::Int.into_type());

        let (data_size, aux_size) = IntDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, None);

        let err = IntDriver.col_sizes_for_row_count(&col, 1).unwrap_err();
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        let msg = format!("{err:#}");
        eprintln!("{msg}");
        assert!(msg.contains(
            "data file for int column int_col0 shorter than 1 rows, expected at least 4 bytes but is 0"
        ));
    }

    #[test]
    fn test_int_col1() {
        let col = map_col("int_col1", ColumnTypeTag::Int.into_type());
        let (data_size, aux_size) = IntDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, None);

        let (data_size, aux_size) = IntDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 4);
        assert_eq!(aux_size, None);

        let err = IntDriver.col_sizes_for_row_count(&col, 2).unwrap_err();
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        let msg = format!("{err:#}");
        // eprintln!("{msg}");
        assert!(msg.contains(
            "data file for int column int_col1 shorter than 2 rows, expected at least 8 bytes but is 4"
        ));
    }

    #[test]
    fn test_timestamp_col1() {
        let col = map_col("timestamp_col1", ColumnTypeTag::Timestamp.into_type());
        let (data_size, aux_size) = TimestampDriver.col_sizes_for_row_count(&col, 0).unwrap();
        assert_eq!(data_size, 0);
        assert_eq!(aux_size, None);

        let (data_size, aux_size) = TimestampDriver.col_sizes_for_row_count(&col, 1).unwrap();
        assert_eq!(data_size, 8);
        assert_eq!(aux_size, None);

        let err = TimestampDriver
            .col_sizes_for_row_count(&col, 2)
            .unwrap_err();
        assert!(matches!(err.reason(), CoreErrorReason::InvalidLayout));
        let msg = format!("{err:#}");
        // eprintln!("{msg}");
        assert!(msg.contains(
            "data file for timestamp column timestamp_col1 shorter than 2 rows, expected at least 16 bytes but is 8"
        ));
    }
}
