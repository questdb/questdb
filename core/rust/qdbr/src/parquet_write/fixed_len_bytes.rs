use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{build_plain_page, encode_bool_iter};
use crate::parquet_write::ParquetResult;

use super::util::BinaryMaxMin;

fn encode_plain_be<const N: usize>(data: &[[u8; N]], buffer: &mut Vec<u8>, null_value: [u8; N]) {
    for x in data.iter().filter(|&&x| x != null_value) {
        buffer.extend(x.iter().rev());
    }
}

fn encode_plain<const N: usize>(
    data: &[[u8; N]],
    buffer: &mut Vec<u8>,
    null_value: [u8; N],
    stats: &mut BinaryMaxMin,
) {
    for x in data.iter().filter(|&&x| x != null_value) {
        buffer.extend_from_slice(x);
        stats.update(x);
    }
}

pub fn bytes_to_page<const N: usize>(
    data: &[[u8; N]],
    reverse: bool,
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let num_rows = column_top + data.len();
    let null_value = {
        let mut null_value = [0u8; N];
        let long_as_bytes = i64::MIN.to_le_bytes();
        for i in 0..N {
            null_value[i] = long_as_bytes[i % long_as_bytes.len()];
        }
        null_value
    };
    let mut buffer = vec![];
    let mut null_count = 0;

    let deflevels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else if data[i - column_top] == null_value {
            null_count += 1;
            false
        } else {
            true
        }
    });

    encode_bool_iter(&mut buffer, deflevels_iter, options.version)?;
    let definition_levels_byte_length = buffer.len();
    let mut stats = BinaryMaxMin::new(&primitive_type);
    if reverse {
        encode_plain_be(data, &mut buffer, null_value);
    } else {
        encode_plain(data, &mut buffer, null_value, &mut stats);
    }

    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats(null_count))
        } else {
            None
        },
        primitive_type,
        options,
        Encoding::Plain,
    )
    .map(Page::Data)
}
