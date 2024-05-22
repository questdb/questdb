use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util;
use crate::parquet_write::util::MaxMin;
use parquet2::encoding::hybrid_rle::bitpacked_encode;
use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{
    serialize_statistics, BooleanStatistics, ParquetStatistics, Statistics,
};

fn encode_plain(
    iterator: impl Iterator<Item = bool>,
    buffer: &mut Vec<u8>,
) -> parquet2::error::Result<()> {
    // encode values using bitpacking
    let len = buffer.len();
    let mut buffer = std::io::Cursor::new(buffer);
    buffer.set_position(len as u64);
    Ok(bitpacked_encode(&mut buffer, iterator)?)
}

pub fn slice_to_page(
    slice: &[u8],
    options: WriteOptions,
    type_: PrimitiveType,
) -> parquet2::error::Result<Page> {
    let mut buffer = vec![];
    let mut stats = MaxMin::new();
    encode_plain(
        slice.iter().map(|x| {
            stats.update(*x as i32);
            *x != 0
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
        slice.len(),
        slice.len(),
        0,
        0,
        0,
        statistics,
        type_,
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
