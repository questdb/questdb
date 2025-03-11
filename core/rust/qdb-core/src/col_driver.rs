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
use crate::col_type::ColumnTypeTag;
use crate::error::CoreResult;
use paste::paste;
use std::path::Path;

pub trait ColumnDriver {
    /// Returns the data and aux file sizes at the given row.
    /// If a column is a simple type such as `INT` or `DOUBLE`, the aux size will be `None`.
    fn col_sizes_for_row(
        &self,
        row_index: usize,
        parent_path: &Path,
    ) -> CoreResult<(u64, Option<u64>)>;
}

macro_rules! impl_primitive_type_driver {
    ($tag:ident) => {
        paste! {
            pub struct [<$tag Driver>];

            impl ColumnDriver for [<$tag Driver>] {
                fn col_sizes_for_row(&self, row_index: usize, _parent_path: &Path) -> CoreResult<(u64, Option<u64>)> {
                    assert!(!ColumnTypeTag::$tag.is_var_size());
                    let row_size = ColumnTypeTag::$tag.fixed_size().expect("primitive");
                    // +1 because row_index is 0-based
                    let data_size = (row_size * (row_index + 1)) as u64;
                    Ok((data_size, None))
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
impl_primitive_type_driver!(Long256);
impl_primitive_type_driver!(GeoByte);
impl_primitive_type_driver!(GeoShort);
impl_primitive_type_driver!(GeoInt);
impl_primitive_type_driver!(GeoLong);
impl_primitive_type_driver!(Uuid);
impl_primitive_type_driver!(Long128);
impl_primitive_type_driver!(IPv4);

pub struct StringDriver;

impl ColumnDriver for StringDriver {
    fn col_sizes_for_row(&self, _row_index: usize, _parent_path: &Path) -> CoreResult<(u64, Option<u64>)> {
        todo!()
    }
}

pub struct SymbolDriver;

impl ColumnDriver for SymbolDriver {
    fn col_sizes_for_row(&self, _row_index: usize, _parent_path: &Path) -> CoreResult<(u64, Option<u64>)> {
        todo!()
    }
}

pub struct BinaryDriver;

impl ColumnDriver for BinaryDriver {
    fn col_sizes_for_row(&self, _row_index: usize, _parent_path: &Path) -> CoreResult<(u64, Option<u64>)> {
        todo!()
    }
}

pub struct VarcharDriver;

impl ColumnDriver for VarcharDriver {
    fn col_sizes_for_row(&self, _row_index: usize, _parent_path: &Path) -> CoreResult<(u64, Option<u64>)> {
        todo!()
    }
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
