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

//! Inference of QuestDB column types from parquet schema descriptors.
//!
//! Used when QuestDB-specific metadata (`QdbMeta`) is absent — i.e. for
//! external parquet files not written by QuestDB.

use parquet2::metadata::ColumnDescriptor;
use parquet2::schema::types::PrimitiveLogicalType::{Timestamp, Uuid};
use parquet2::schema::types::{
    GroupConvertedType, GroupLogicalType, IntegerType, ParquetType, PhysicalType,
    PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use parquet2::schema::Repetition;
use qdb_core::col_type::{
    encode_array_type, ColumnType, ColumnTypeTag, QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG,
};

/// Infers a QuestDB `ColumnType` from a parquet `ColumnDescriptor`'s physical
/// type, logical type, and converted type.
pub fn infer_column_type(column: &ColumnDescriptor) -> Option<ColumnType> {
    match (
        column.descriptor.primitive_type.physical_type,
        column.descriptor.primitive_type.logical_type,
        column.descriptor.primitive_type.converted_type,
    ) {
        (
            PhysicalType::Int64,
            Some(Timestamp {
                unit: TimeUnit::Microseconds,
                is_adjusted_to_utc: _,
            }),
            _,
        ) => Some(ColumnType::new(ColumnTypeTag::Timestamp, 0)),
        (
            PhysicalType::Int64,
            Some(Timestamp {
                unit: TimeUnit::Nanoseconds,
                is_adjusted_to_utc: _,
            }),
            _,
        ) => Some(ColumnType::new(
            ColumnTypeTag::Timestamp,
            QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG,
        )),
        (
            PhysicalType::Int64,
            Some(Timestamp {
                unit: TimeUnit::Milliseconds,
                is_adjusted_to_utc: _,
            }),
            _,
        ) => Some(ColumnType::new(ColumnTypeTag::Date, 0)),
        (PhysicalType::Int64, Some(PrimitiveLogicalType::Decimal(precision, scale)), _)
        | (PhysicalType::Int64, _, Some(PrimitiveConvertedType::Decimal(precision, scale))) => {
            ColumnType::new_decimal(precision as u8, scale as u8)
        }
        (PhysicalType::Int64, _, _) => Some(ColumnType::new(ColumnTypeTag::Long, 0)),
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int32)), _) => {
            Some(ColumnType::new(ColumnTypeTag::Int, 0))
        }
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Decimal(precision, scale)), _)
        | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Decimal(precision, scale))) => {
            ColumnType::new_decimal(precision as u8, scale as u8)
        }
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int16)), _)
        | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int16)) => {
            Some(ColumnType::new(ColumnTypeTag::Short, 0))
        }
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int8)), _)
        | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int8)) => {
            Some(ColumnType::new(ColumnTypeTag::Byte, 0))
        }
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Date), _)
        | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Date)) => {
            Some(ColumnType::new(ColumnTypeTag::Date, 0))
        }
        (PhysicalType::Int32, _, _) => Some(ColumnType::new(ColumnTypeTag::Int, 0)),
        (PhysicalType::Boolean, _, _) => Some(ColumnType::new(ColumnTypeTag::Boolean, 0)),
        (PhysicalType::Double, _, _) => match array_column_type(&column.base_type) {
            Some(array_type) => Some(array_type),
            None => Some(ColumnType::new(ColumnTypeTag::Double, 0)),
        },
        (PhysicalType::Float, _, _) => Some(ColumnType::new(ColumnTypeTag::Float, 0)),
        (
            PhysicalType::FixedLenByteArray(_),
            Some(PrimitiveLogicalType::Decimal(precision, scale)),
            _,
        )
        | (
            PhysicalType::FixedLenByteArray(_),
            _,
            Some(PrimitiveConvertedType::Decimal(precision, scale)),
        ) => ColumnType::new_decimal(precision as u8, scale as u8),
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::Decimal(precision, scale)), _)
        | (PhysicalType::ByteArray, _, Some(PrimitiveConvertedType::Decimal(precision, scale))) => {
            ColumnType::new_decimal(precision as u8, scale as u8)
        }
        (PhysicalType::FixedLenByteArray(16), Some(Uuid), _) => {
            Some(ColumnType::new(ColumnTypeTag::Uuid, 0))
        }
        (PhysicalType::FixedLenByteArray(16), _, _) => {
            Some(ColumnType::new(ColumnTypeTag::Long128, 0))
        }
        (PhysicalType::FixedLenByteArray(32), _, _) => {
            Some(ColumnType::new(ColumnTypeTag::Long256, 0))
        }
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String), _)
        | (PhysicalType::ByteArray, _, Some(PrimitiveConvertedType::Utf8)) => {
            Some(ColumnType::new(ColumnTypeTag::Varchar, 0))
        }
        (PhysicalType::ByteArray, _, _) => Some(ColumnType::new(ColumnTypeTag::Binary, 0)),
        (PhysicalType::Int96, _, None) => Some(ColumnType::new(
            ColumnTypeTag::Timestamp,
            QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG,
        )),
        (_, _, _) => None,
    }
}

fn array_column_type(base_type: &ParquetType) -> Option<ColumnType> {
    let mut cur_type;
    match base_type {
        ParquetType::GroupType {
            field_info: _,
            logical_type,
            converted_type,
            fields,
        } => {
            let is_list = *converted_type == Some(GroupConvertedType::List)
                || *logical_type == Some(GroupLogicalType::List);
            if !is_list || fields.len() != 1 {
                return None;
            }
            cur_type = &fields[0];
        }
        ParquetType::PrimitiveType(_) => {
            return None;
        }
    };

    let mut dim = 0;
    loop {
        match cur_type {
            ParquetType::PrimitiveType(_) => {
                break;
            }
            ParquetType::GroupType {
                field_info,
                logical_type: _,
                converted_type: _,
                fields,
            } => {
                if fields.len() != 1 {
                    return None;
                }
                if field_info.repetition == Repetition::Repeated {
                    dim += 1;
                }
                cur_type = &fields[0];
            }
        }
    }

    encode_array_type(ColumnTypeTag::Double, dim).ok()
}
