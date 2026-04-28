#![allow(clippy::not_unsafe_ptr_arg_deref)]

mod buffers;
mod partition_decoder;
mod file_decoder;

use crate::parquet::error::{ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::ParquetFieldId;
use qdb_core::col_type::ColumnType;

pub(crate) fn validate_jni_column_types(
    columns: &[(ParquetFieldId, ColumnType)],
) -> ParquetResult<()> {
    for &(_, java_column_type) in columns {
        let code = java_column_type.code();
        let _col_type: ColumnType = code
            .try_into()
            .context("invalid column type passed across JNI layer")?;
    }
    Ok(())
}

#[repr(u8)]
pub(crate) enum DecodeMode {
    NoFilter = 0,
    FilterSkip = 1,
    FilterFillNulls = 2,
}
