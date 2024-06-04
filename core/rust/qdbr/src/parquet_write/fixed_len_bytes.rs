use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter};
use crate::parquet_write::ParquetResult;

fn encode_plain<const N: usize>(data: &[[u8; N]], buffer: &mut Vec<u8>) {
    // append the non-null values
    data.iter().for_each(|x| {
        //TODO: if not a null
        buffer.extend_from_slice(x);
    })
}

pub fn bytes_to_page<const N: usize>(
    data: &[[u8; N]],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let num_rows = column_top + data.len();
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        // TODO: detect null value
        } else if data[i - column_top] != data[i - column_top] {
            null_count += 1;
            false
        } else {
            true
        }
    });

    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;
    let definition_levels_byte_length = buffer.len();
    encode_plain(data, &mut buffer);
    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        None, // TODO: add statistics
        primitive_type,
        options,
        Encoding::Plain,
    )
    .map(Page::Data)
}
