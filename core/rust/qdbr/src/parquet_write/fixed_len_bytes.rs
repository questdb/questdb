use parquet2::encoding::Encoding;
use parquet2::page::DataPage;
use parquet2::schema::types::PrimitiveType;
use crate::parquet_write::file::WriteOptions;
use crate::util::{build_plain_page, encode_bool_iter};

fn encode_plain(data: &[u8], chunk_size: usize, buffer: &mut Vec<u8>) {
    // append the non-null values
    data.chunks(chunk_size).for_each(|x| {
        //TODO: if not a null
        buffer.extend_from_slice(x);
    })
}

pub fn bytes_to_page(
    data: &[u8],
    chunk_size: usize,
    options: WriteOptions,
    type_: PrimitiveType,
) -> parquet2::error::Result<DataPage> {
    let mut buffer = vec![];
    let mut null_count = 0;

    let nulls_iterator = data.chunks(chunk_size).map(|bytes| {
        // TODO: null
        if false {
            null_count += 1;
            false
        } else {
            true
        }
    }) ;

    let length = nulls_iterator.len();
    encode_bool_iter(&mut buffer, nulls_iterator, options.version)?;
    let definition_levels_byte_length = buffer.len();
    encode_plain(data, chunk_size, &mut buffer);
    build_plain_page(
        buffer,
        length,
        length,
        null_count,
        0,
        definition_levels_byte_length,
        None, // do we really want a binary statistics?
        type_,
        options,
        Encoding::Plain,
    )
}

