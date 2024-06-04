use parquet2::encoding::hybrid_rle::bitpacked_encode;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{
    serialize_statistics, BooleanStatistics, ParquetStatistics, Statistics,
};

use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::MaxMin;
use crate::parquet_write::{util, ParquetResult};

fn encode_plain(iterator: impl Iterator<Item = bool>, buffer: &mut Vec<u8>) -> ParquetResult<()> {
    // encode values using bitpacking
    let len = buffer.len();
    let mut buffer = std::io::Cursor::new(buffer);
    buffer.set_position(len as u64);
    Ok(bitpacked_encode(&mut buffer, iterator)?)
}

pub fn slice_to_page(
    slice: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<Page> {
    let num_rows = column_top + slice.len();
    let mut buffer = vec![];
    let mut stats = MaxMin::new();

    encode_plain(
        (0..num_rows).map(|i| {
            let x = if i < column_top {
                0
            } else {
                slice[i - column_top]
            };
            stats.update(x as i32);
            x != 0
        }),
        &mut buffer,
    )?;

    let statistics = if options.write_statistics {
        Some(build_statistics(stats))
    } else {
        None
    };

    util::build_plain_page(
        buffer,
        num_rows,
        0,
        0,
        statistics,
        primitive_type,
        options,
        Encoding::Plain,
    )
    .map(Page::Data)
}

fn build_statistics(bool_statistics: MaxMin<i32>) -> ParquetStatistics {
    let (max, min) = bool_statistics.get_current_values();
    let statistics = &BooleanStatistics {
        null_count: Some(0),
        distinct_count: None,
        max_value: max.map(|x| x != 0),
        min_value: min.map(|x| x != 0),
    } as &dyn Statistics;
    serialize_statistics(statistics)
}
