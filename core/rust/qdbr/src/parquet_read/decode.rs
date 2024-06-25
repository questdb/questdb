use crate::parquet_read::{ColumnChunkBuffers, ParquetDecoder};
use anyhow::{anyhow, Context};
use parquet2::encoding::hybrid_rle::HybridEncoded;
use parquet2::encoding::{bitpacked, delta_bitpacked, hybrid_rle, Encoding};
use parquet2::page::{split_buffer, DataPage, DictPage, Page};
use parquet2::read::{decompress, get_page_iterator};
use parquet2::schema::types::{PhysicalType, PrimitiveLogicalType};
use parquet2::write::Version;
use std::mem::size_of;
use std::ptr;

impl ColumnChunkBuffers {
    pub fn new() -> Self {
        Self {
            row_count: 0,
            data_vec: Vec::new(),
            data_ptr: ptr::null_mut(),
            data_size: 0,
            aux_vec: Vec::new(),
            aux_ptr: ptr::null_mut(),
            aux_size: 0,
        }
    }
}

impl ParquetDecoder {
    pub fn decode_column_chunk(
        &mut self,
        row_group: usize,
        column: usize,
        _column_type: i32,
    ) -> anyhow::Result<()> {
        println!(
            "decode_column_chunk: row_group={}, column={}, groups: {}",
            row_group,
            column,
            self.metadata.row_groups.len()
        );
        let columns = self.metadata.row_groups[row_group].columns();
        let column_metadata = &columns[column];
        let buffers = &mut self.column_buffers[column];

        let chunk_size = column_metadata.compressed_size().try_into()?;
        let page_reader =
            get_page_iterator(column_metadata, &mut self.file, None, vec![], chunk_size)?;

        let version = match self.metadata.version {
            1 => Ok(Version::V1),
            2 => Ok(Version::V2),
            ver => Err(anyhow!(format!(
                "unsupported parquet file version: {}",
                ver
            ))),
        }?;

        let mut dict = None;
        let mut row_count = 0usize;
        buffers.aux_vec.clear();
        buffers.data_vec.clear();
        for maybe_page in page_reader {
            let page = maybe_page?;
            let page = decompress(page, &mut self.decompress_buffer)?;

            match page {
                Page::Dict(page) => {
                    dict = Some(page);
                }
                Page::Data(page) => {
                    row_count += decoder_page(version, &page, dict.as_ref(), buffers)?;
                }
            };
        }
        buffers.row_count = row_count;

        Ok(())
    }
}

pub fn decoder_page(
    version: Version,
    page: &DataPage,
    dict: Option<&DictPage>,
    buffers: &mut ColumnChunkBuffers,
) -> anyhow::Result<usize> {
    let (_rep_levels, def_levels, values_buffer) = split_buffer(page)?;
    let row_count = page.header().num_values();

    match (
        page.descriptor.primitive_type.physical_type,
        page.descriptor.primitive_type.logical_type,
    ) {
        (typ, None) => match page.encoding() {
            Encoding::Plain => {
                buffers.aux_size = 0;
                buffers.aux_ptr = ptr::null_mut();

                if def_levels.is_empty() {
                    buffers.data_vec.resize(values_buffer.len(), 0);
                    buffers.data_vec.clone_from_slice(values_buffer);
                    buffers.data_size = values_buffer.len();
                    buffers.data_ptr = buffers.data_vec.as_mut_ptr();
                    Ok(row_count)
                } else {
                    return match typ {
                        PhysicalType::Int32 => {
                            decode_fixed_plain(
                                version,
                                buffers,
                                def_levels,
                                &values_buffer,
                                size_of::<i32>(),
                                row_count,
                                &i32::MIN.to_le_bytes(),
                            )?;
                            Ok(row_count)
                        }
                        PhysicalType::Int64 => {
                            decode_fixed_plain(
                                version,
                                buffers,
                                def_levels,
                                &values_buffer,
                                size_of::<i64>(),
                                row_count,
                                &i64::MIN.to_le_bytes(),
                            )?;

                            Ok(row_count)
                        }
                        _ => Err(anyhow!("type not supported")),
                    };
                }
            }
            _ => Err(anyhow!("encoding not supported")),
        },
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String)) => {
            let encoding = page.encoding();
            match (encoding, dict) {
                (Encoding::DeltaLengthByteArray, None) => {
                    decode_delta_len_string(
                        version,
                        buffers,
                        def_levels,
                        values_buffer,
                        row_count,
                    )?;
                    Ok(row_count)
                }
                _ => Err(anyhow!("encoding not supported")),
            }
        }
        _ => Err(anyhow!("deserialization not supported")),
    }
}

fn decode_delta_len_string(
    version: Version,
    buffers: &mut ColumnChunkBuffers,
    def_levels: &[u8],
    values_buffer: &[u8],
    row_count: usize,
) -> anyhow::Result<()> {
    let iter = decode_null_bitmap(version, def_levels, row_count)?;
    let mut decoder = delta_bitpacked::Decoder::try_new(values_buffer)?;
    let mut data_offset = decoder.consumed_bytes();
    println!("data_offset: {}", data_offset);
    let mut count = 0;

    let data_vec = &mut buffers.data_vec;
    let aux_vec = &mut buffers.aux_vec;
    if aux_vec.is_empty() {
        aux_vec.extend_from_slice(&0u64.to_le_bytes());
    }
    aux_vec.reserve(row_count * size_of::<i64>());
    let null_value = (-1i32).to_le_bytes();

    for value in iter {
        if count < row_count {
            if value == 1u8 {
                if let Some(len) = decoder.next() {
                    let len = len? as usize;
                    let slice = &values_buffer[data_offset..data_offset + len];
                    data_offset += len;

                    let str = String::from_utf8(slice.to_vec()).with_context(|| {
                        format!(
                            "reading value {} at offset {}: {:?}",
                            count,
                            data_offset - len,
                            slice
                        )
                    })?;
                    let utf16 = str.encode_utf16();

                    let pos = data_vec.len();
                    data_vec.resize(pos + size_of::<i32>(), 0);
                    let mut utf16_len: i32 = 0;
                    utf16.for_each(|c| {
                        data_vec.extend_from_slice(&c.to_le_bytes());
                        utf16_len += 1;
                    });

                    data_vec[pos..pos + size_of::<i32>()].copy_from_slice(&utf16_len.to_le_bytes());
                    aux_vec.extend_from_slice(&(data_vec.len() as u64).to_le_bytes());
                } else {
                    return Err(anyhow!("values buffer is too small"));
                }
            } else {
                data_vec.extend_from_slice(&null_value);
                aux_vec.extend_from_slice(&(data_vec.len() as u64).to_le_bytes());
            }
            count += 1;
        } else {
            break;
        }
    }
    buffers.data_size = data_vec.len();
    buffers.data_ptr = data_vec.as_mut_ptr();

    if count == 0 {
        aux_vec.clear();
    }
    buffers.aux_size = aux_vec.len();
    buffers.aux_ptr = buffers.aux_vec.as_mut_ptr();

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn decode_fixed_plain(
    version: Version,
    buffers: &mut ColumnChunkBuffers,
    def_levels: &[u8],
    values_buffer: &&[u8],
    value_size: usize,
    row_count: usize,
    null_value: &[u8],
) -> anyhow::Result<()> {
    let data_vec = &mut buffers.data_vec;
    let offset = data_vec.len();
    data_vec.resize(offset + row_count * value_size, 0);
    let iter = decode_null_bitmap(version, def_levels, row_count)?;
    let mut non_null_count = 0;
    let mut count = 0;
    for value in iter {
        if count < row_count {
            if value == 1u8 {
                if non_null_count >= row_count {
                    return Err(anyhow!("values buffer is too small"));
                }
                data_vec[offset + count * value_size..offset + (count + 1) * value_size]
                    .copy_from_slice(
                        &values_buffer
                            [non_null_count * value_size..(non_null_count + 1) * value_size],
                    );
                non_null_count += 1;
            } else {
                data_vec[offset + count * value_size..offset + (count + 1) * value_size]
                    .copy_from_slice(null_value);
            }
            count += 1;
        } else {
            break;
        }
    }
    buffers.data_size = data_vec.len();
    buffers.data_ptr = data_vec.as_mut_ptr();
    Ok(())
}

pub fn decode_null_bitmap(
    _version: Version,
    buffer: &[u8],
    count: usize,
) -> anyhow::Result<bitpacked::Decoder<u8>> {
    decode_bitmap_v2(buffer, count)
}

fn decode_bitmap_v2(buffer: &[u8], count: usize) -> anyhow::Result<bitpacked::Decoder<u8>> {
    let decoder = hybrid_rle::Decoder::new(buffer, 1usize);
    for run in decoder {
        if let HybridEncoded::Bitpacked(values) = run? {
            let inner_decoder = bitpacked::Decoder::<u8>::try_new(values, 1usize, count)?;
            return Ok(inner_decoder);
        }
    }
    Err(anyhow::anyhow!("No data found"))
}

fn _decode_bitmap_v1(buffer: &[u8], count: usize) -> anyhow::Result<bitpacked::Decoder<u8>> {
    if buffer.len() < 4 {
        return Err(anyhow::anyhow!("definition level buffer is too short"));
    }

    let decoder = hybrid_rle::Decoder::new(&buffer[4..], 1usize);
    for run in decoder {
        if let HybridEncoded::Bitpacked(values) = run? {
            let inner_decoder = bitpacked::Decoder::<u8>::try_new(values, 1usize, count)?;
            return Ok(inner_decoder);
        }
    }
    Err(anyhow::anyhow!("No data found"))
}

#[cfg(test)]
mod tests {
    use crate::parquet_read::ParquetDecoder;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, ColumnType, Partition};
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use parquet2::write::Version;
    use rand::Rng;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::mem::size_of;
    use std::path::Path;
    use std::ptr::null;
    use tempfile::NamedTempFile;

    #[test]
    fn test_decode_int_column_v2_nulls() {
        let row_count = 10;
        let row_group_size = 50;
        let data_page_size = 50;
        let version = Version::V2;
        let expected_buff = create_col_data_buff_int(row_count);
        let column_count = 1;
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            &expected_buff,
        );

        let mut decoder = ParquetDecoder::read(file).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;

        for column_index in 0..column_count {
            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(row_group_index, column_index, 0)
                    .unwrap();

                let ccb = &decoder.column_buffers[column_index];
                assert_eq!(ccb.data_size, expected_buff.len());
                assert_eq!(ccb.aux_size, 0);
                assert_eq!(ccb.data_vec, expected_buff);
            }
        }
    }

    // #[test]
    fn test_decode_file() {
        let file = File::open("/Users/alpel/temp/db/x.parquet").unwrap();
        let mut decoder = ParquetDecoder::read(file).unwrap();
        let row_group_count = decoder.row_group_count as usize;
        let column_count = decoder.columns.len();

        for column_index in 0..column_count {
            let mut col_row_count = 0usize;

            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(row_group_index, column_index, 0)
                    .unwrap();

                let ccb = &decoder.column_buffers[column_index];
                assert_eq!(ccb.data_vec.len(), ccb.data_size);

                col_row_count += ccb.row_count;
            }

            assert_eq!(col_row_count, decoder.row_count);
        }
    }

    #[test]
    fn test_decode_int_long_column_v2_nulls_multi_groups() {
        let row_count = 100000;
        let row_group_size = 1000;
        let data_page_size = 1000;
        let version = Version::V1;
        let mut columns = Vec::new();
        let expected_int_buff = create_col_data_buff_int(row_count);
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "int_col",
            &expected_int_buff,
            ColumnType::Int,
        ));
        let expected_long_buff = create_col_data_buff_long(row_count);
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "long_col",
            &expected_long_buff,
            ColumnType::Long,
        ));
        let (expected_string_primary_buff, expected_string_aux_buff) =
            create_col_data_buff_string(row_count, 3);
        columns.push(create_var_column(
            columns.len() as i32,
            row_count,
            "string_col",
            &expected_string_primary_buff,
            &expected_string_aux_buff,
            ColumnType::String,
        ));

        let expected_buff = [
            &expected_int_buff,
            &expected_long_buff,
            &expected_string_primary_buff,
        ];
        let expected_buff_aux = [None, None, Some(&expected_string_aux_buff)];
        // let expected_buff = [&expected_string_primary_buff];
        // let expected_buff_aux = [Some(&expected_string_aux_buff)];
        let column_count = columns.len();

        let file = write_cols_to_parquet_file(row_group_size, data_page_size, version, columns);

        let mut decoder = ParquetDecoder::read(file).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;

        for (column_index, expected) in expected_buff.iter().enumerate() {
            let expected_aux = expected_buff_aux[column_index];
            let mut data_offset = 0usize;
            let mut col_row_count = 0usize;

            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(row_group_index, column_index, 0)
                    .unwrap();

                let ccb = &decoder.column_buffers[column_index];
                assert_eq!(ccb.data_vec.len(), ccb.data_size);

                assert!(
                    data_offset + ccb.data_size <= expected.len(),
                    "Assertion failed: {} + {} < {}, where read_row_offset = {}, ccb.data_size = {}, expected.len() = {}",
                    data_offset, ccb.data_size, expected.len(), data_offset, ccb.data_size, expected.len()
                );

                assert_eq!(
                    expected[data_offset..data_offset + ccb.data_size],
                    ccb.data_vec
                );

                if let Some(expected_aux_data) = expected_aux {
                    let mut expected_aux_data_slice = vec![];
                    if col_row_count == 0 {
                        expected_aux_data_slice
                            .extend_from_slice(&expected_aux_data[0..ccb.aux_size])
                    } else {
                        let vec_i64_ref = unsafe {
                            std::slice::from_raw_parts(
                                expected_aux_data.as_ptr() as *const i64,
                                expected_aux_data.len() / size_of::<i64>(),
                            )
                        };
                        expected_aux_data_slice.extend_from_slice(&0u64.to_le_bytes());
                        for i in 0..ccb.row_count {
                            let row_data_offset = vec_i64_ref[col_row_count + 1 + i];
                            expected_aux_data_slice.extend_from_slice(
                                &(row_data_offset - data_offset as i64).to_le_bytes(),
                            );
                        }
                    }

                    assert_eq!(expected_aux_data_slice, ccb.aux_vec);
                } else {
                    assert_eq!(ccb.aux_size, 0);
                }
                col_row_count += ccb.row_count;
                data_offset += ccb.data_vec.len();
            }

            assert_eq!(expected.len(), data_offset);
            assert_eq!(row_count, col_row_count);
        }
    }

    fn write_parquet_file(
        row_count: usize,
        row_group_size: usize,
        data_page_size: usize,
        version: Version,
        expected_buff: &[u8],
    ) -> File {
        let columns = vec![create_fix_column(
            0,
            row_count,
            "int_col",
            expected_buff,
            ColumnType::Int,
        )];

        write_cols_to_parquet_file(row_group_size, data_page_size, version, columns)
    }

    fn write_cols_to_parquet_file(
        row_group_size: usize,
        data_page_size: usize,
        version: Version,
        columns: Vec<Column>,
    ) -> File {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let partition = Partition { table: "test_table".to_string(), columns };
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_row_group_size(Some(row_group_size))
            .with_data_page_size(Some(data_page_size))
            .with_version(version)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file
            .write_all(bytes.to_byte_slice())
            .expect("Failed to write to temp file");

        let path = temp_file.path().to_str().unwrap();
        let file = File::open(Path::new(path)).unwrap();
        file
    }

    fn create_col_data_buff_int(row_count: usize) -> Vec<u8> {
        let value_size = size_of::<i32>();
        let null_value = i32::MIN.to_le_bytes();
        let mut buff = vec![0u8; row_count * value_size];
        for i in 0..((row_count + 1) / 2) {
            let value = i as i32;
            let offset = 2 * i * value_size;
            buff[offset..offset + value_size].copy_from_slice(&value.to_le_bytes());

            if offset + 2 * value_size <= buff.len() {
                // buff[offset + value_size..offset + 2 * value_size].copy_from_slice(&value.to_le_bytes());
                buff[offset + value_size..offset + 2 * value_size].copy_from_slice(&null_value);
            }
        }
        buff
    }

    fn create_col_data_buff_long(row_count: usize) -> Vec<u8> {
        let value_size = size_of::<i64>();
        let null_value = i64::MIN.to_le_bytes();
        let mut buff = vec![0u8; row_count * value_size];
        for i in 0..((row_count + 1) / 2) {
            let value = i as i64;
            let offset = 2 * i * value_size;
            buff[offset..offset + value_size].copy_from_slice(&value.to_le_bytes());

            if offset + 2 * value_size <= buff.len() {
                buff[offset + value_size..offset + 2 * value_size].copy_from_slice(&null_value);
            }
        }
        buff
    }

    fn generate_random_unicode_string(len: usize) -> String {
        let mut rng = rand::thread_rng();

        let len = 1 + rng.gen_range(0..len - 1);
        let random_string: String = (0..len)
            .map(|_| {
                // Generate a random Unicode scalar value in a range that includes non-ASCII characters
                let c = rng.gen_range(0x00A0..0xD7FF); // Example range excluding surrogate pairs
                char::from_u32(c).unwrap_or('�') // Use a replacement character for invalid values
            })
            .collect();

        random_string
    }

    fn create_col_data_buff_string(row_count: usize, distinct_values: usize) -> (Vec<u8>, Vec<u8>) {
        let value_size = size_of::<i64>();
        let mut aux_buff = vec![0u8; value_size];
        let mut data_buff = Vec::new();

        let str_values: Vec<Vec<u16>> = (0..distinct_values)
            .map(|_| generate_random_unicode_string(10).encode_utf16().collect())
            .collect();

        let mut i = 0;
        while i < row_count {
            let str_value = &str_values[i % distinct_values];
            data_buff.extend_from_slice(&(str_value.len() as i32).to_le_bytes());
            data_buff.extend_from_slice(str_value.to_byte_slice());
            aux_buff.extend_from_slice(&data_buff.len().to_le_bytes());
            i += 1;

            if i < row_count {
                data_buff.extend_from_slice(&(-1i32).to_le_bytes());
                aux_buff.extend_from_slice(&data_buff.len().to_le_bytes());
                i += 1;
            }
        }
        (data_buff, aux_buff)
    }

    fn create_fix_column(
        id: i32,
        row_count: usize,
        name: &'static str,
        primary_data: &[u8],
        col_type: ColumnType,
    ) -> Column {
        Column::from_raw_data(
            id,
            name,
            col_type as i32,
            0,
            row_count,
            primary_data.as_ptr(),
            primary_data.len(),
            null(),
            0,
            null(),
            0,
        )
        .unwrap()
    }

    fn create_var_column(
        id: i32,
        row_count: usize,
        name: &'static str,
        primary_data: &Vec<u8>,
        aux_data: &Vec<u8>,
        col_type: ColumnType,
    ) -> Column {
        Column::from_raw_data(
            id,
            name,
            col_type as i32,
            0,
            row_count,
            primary_data.as_ptr(),
            primary_data.len(),
            aux_data.as_ptr(),
            aux_data.len(),
            null(),
            0,
        )
        .unwrap()
    }
}
