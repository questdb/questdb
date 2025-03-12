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
use crate::error::CoreResult;
use paste::paste;

macro_rules! impl_primitive_type_driver {
    ($tag:ident) => {
        paste! {
            pub struct [<$tag Driver>];

            impl ColumnDriver for [<$tag Driver>] {
                fn col_sizes_for_row(&self, _col: &MappedColumn, row_index: u64) -> CoreResult<(u64, Option<u64>)> {
                    assert!(!ColumnTypeTag::$tag.is_var_size());
                    let row_size = ColumnTypeTag::$tag.fixed_size().expect("fixed size column") as u64;
                    // +1 because row_index is 0-based
                    let data_size = (row_size * (row_index + 1));
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
