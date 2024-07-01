use std::collections::VecDeque;
use crate::parquet_read::{ColumnChunkBuffers, ParquetDecoder};
use crate::parquet_write::schema::ColumnType;
use anyhow::{anyhow};
use parquet2::encoding::hybrid_rle::{BitmapIter, HybridEncoded};
use parquet2::encoding::{bitpacked, hybrid_rle, Encoding};
use parquet2::page::{split_buffer, DataPage, DictPage, Page};
use parquet2::read::{decompress, get_page_iterator};
use parquet2::schema::types::{IntegerType, PhysicalType, PrimitiveLogicalType};
use parquet2::write::Version;
use std::ptr;
use parquet2::deserialize::{FilteredHybridEncoded, FilteredHybridRleDecoderIter, HybridDecoderBitmapIter};
use parquet2::error::Error;
use parquet2::indexes::Interval;
use crate::parquet_read::column_sink::{FixedColumnSink, PhysicalIntegerColumnSink, Pushable, StringColumnSink, VarcharColumnSink};
use crate::parquet_read::slicer::{DataPageFixedSlicer, decode_rle, DeltaLengthArraySlicer, DictionarySlicer};
use crate::parquet_write::ParquetResult;

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
        column_type: ColumnType,
    ) -> anyhow::Result<()> {
        println!("Decoding column: {}, row_group {}", column, row_group);
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
                    row_count += decoder_page(version, &page, dict.as_ref(), buffers, column_type)?;
                }
            };
        }

        buffers.data_size = buffers.data_vec.len();
        buffers.data_ptr = buffers.data_vec.as_mut_ptr();

        buffers.aux_size = buffers.aux_vec.len();
        buffers.aux_ptr = buffers.aux_vec.as_mut_ptr();
        buffers.row_count = row_count;

        Ok(())
    }
}

pub fn decoder_page(
    version: Version,
    page: &DataPage,
    dict: Option<&DictPage>,
    buffers: &mut ColumnChunkBuffers,
    column_type: ColumnType,
) -> anyhow::Result<usize> {
    let (_rep_levels, _, values_buffer) = split_buffer(page)?;
    let row_count = page.header().num_values();

    match (
        page.descriptor.primitive_type.physical_type,
        page.descriptor.primitive_type.logical_type,
    ) {
        (typ, None) => match page.encoding() {
            Encoding::Plain => {
                buffers.aux_vec.clear();
                buffers.aux_ptr = ptr::null_mut();

                match typ {
                    PhysicalType::Int32 => {
                        decode_page(
                            version,
                            page,
                            row_count,
                            &mut FixedColumnSink::new(&mut DataPageFixedSlicer::<4>::new(values_buffer, row_count), buffers, i32::MIN.to_le_bytes()),
                        )?;

                        Ok(row_count)
                    }
                    PhysicalType::Int64 => {
                        decode_page(
                            version,
                            page,
                            row_count,
                            &mut FixedColumnSink::new(&mut DataPageFixedSlicer::<8>::new(values_buffer, row_count), buffers, i64::MIN.to_le_bytes()),
                        )?;

                        Ok(row_count)
                    }
                    PhysicalType::Double => {
                        decode_page(
                            version,
                            page,
                            row_count,
                            &mut FixedColumnSink::new(&mut DataPageFixedSlicer::<8>::new(values_buffer, row_count), buffers, f64::NAN.to_le_bytes()),
                        )?;

                        Ok(row_count)
                    }
                    PhysicalType::Boolean => {
                        decode_page_bool(
                            version,
                            page,
                            values_buffer,
                            row_count,
                            buffers,
                        )?;

                        Ok(row_count)
                    }

                    _ => Err(anyhow!(format!("encoding {:?} not supported for type {:?}", page.encoding(), typ))),
                }
            }
            _ => Err(anyhow!(format!("encoding {:?} not supported for type {:?}", page.encoding(), typ))),
        },
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(logical_type))) => match page.encoding() {
            Encoding::Plain => {
                match logical_type {
                    IntegerType::Int16 | IntegerType::UInt16 => {
                        decode_page(
                            version,
                            page,
                            row_count,
                            &mut PhysicalIntegerColumnSink::new(&mut DataPageFixedSlicer::<4>::new(values_buffer, row_count), buffers, i16::MIN.to_le_bytes()),
                        )?;
                        Ok(row_count)
                    },
                    IntegerType::Int8 | IntegerType::UInt8 => {
                        decode_page(
                            version,
                            page,
                            row_count,
                            &mut PhysicalIntegerColumnSink::new(&mut DataPageFixedSlicer::<4>::new(values_buffer, row_count), buffers, [0u8]),
                        )?;
                        Ok(row_count)
                    },
                    _ => Err(anyhow!(format!("encoding not supported for type {:?}, {:?}", page.encoding(), logical_type))),
                }
            },
            _ => Err(anyhow!(format!("encoding {:?} not supported for type {:?}", page.encoding(), logical_type))),
        },
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String)) => {
            let encoding = page.encoding();

            match (encoding, dict, column_type) {
                (Encoding::DeltaLengthByteArray, None, ColumnType::String) => {
                    let mut slicer = DeltaLengthArraySlicer::try_new(values_buffer, row_count)?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut StringColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }

                (Encoding::DeltaLengthByteArray, None, ColumnType::Varchar) => {
                    let mut slicer = DeltaLengthArraySlicer::try_new(values_buffer, row_count)?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut VarcharColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }

                (Encoding::RleDictionary, Some(dict_page), ColumnType::Varchar) => {
                    let slicer = decode_rle(values_buffer, dict_page, row_count)?;
                    match slicer {
                        DictionarySlicer::Bitpacked(mut slicer) => {
                            decode_page(
                                version,
                                page,
                                row_count,
                                &mut VarcharColumnSink::new(&mut slicer, buffers),
                            )?;
                        }
                        DictionarySlicer::Rle(mut slicer) => {
                            decode_page(
                                version,
                                page,
                                row_count,
                                &mut VarcharColumnSink::new(&mut slicer, buffers),
                            )?;
                        }
                    }
                    Ok(row_count)
                }
                _ => Err(anyhow!("encoding not supported")),
            }
        }
        _ => Err(anyhow!(format!("deserialization not supported; physical_type: {:?}, logical_type: {:?}", page.descriptor.primitive_type.physical_type, page.descriptor.primitive_type.logical_type))),
    }
}

// fn decode_rle_dict_to_varchar<DIter: Iterator<Item=u8>>(
//     values_buffer: &[u8],
//     dict_page: &DictPage,
//     def_levels_iter: DIter,
//     buffers: &mut ColumnChunkBuffers,
//     row_count: usize,
// ) -> anyhow::Result<()> {
//     let mut dict_values: Vec<&[u8]> = Vec::with_capacity(dict_page.num_values);
//     let mut offset = 0usize;
//     let dict_data = &dict_page.buffer;
//
//     for _i in 0..dict_page.num_values {
//         if offset + size_of::<u32>() > dict_data.len() {
//             return Err(anyhow!("dictionary data is too short to read length"));
//         }
//
//         let str_len =
//             unsafe { ptr::read_unaligned(dict_data.as_ptr().add(offset) as *const u32) } as usize;
//         offset += size_of::<u32>();
//
//         if offset + str_len > dict_data.len() {
//             return Err(anyhow!("dictionary data is too short to read value"));
//         }
//
//         let str_slice = &dict_data[offset..offset + str_len];
//         let _str = String::from_utf8(str_slice.to_vec())?;
//         dict_values.push(str_slice);
//         offset += str_len;
//     }
//
//     let bits_per_entry = values_buffer[0] as usize;
//     let mut rle_decoder = decode_rle_v2::<u32>(&values_buffer[1..], None, bits_per_entry)?;
//     let mut count = 0;
//
//     for value in def_levels_iter {
//         if count < row_count {
//             if value == 1u8 {
//                 let next = rle_decoder.next().expect("expected value of rle entry") as usize;
//                 if next < dict_page.num_values {
//                     let ut8_slice = dict_values[next];
//                     append_varchar(&mut buffers.aux_vec, &mut buffers.data_vec, ut8_slice);
//                 } else {
//                     return Err(anyhow!("dictionary no entry in the dictionary"));
//                 }
//             } else {
//                 append_varchar_null(&mut buffers.aux_vec, &mut buffers.data_vec);
//             }
//             count += 1;
//         } else {
//             break;
//         }
//     }
//     buffers.data_size = buffers.data_vec.len();
//     buffers.data_ptr = buffers.data_vec.as_mut_ptr();
//
//     buffers.aux_size = buffers.aux_vec.len();
//     buffers.aux_ptr = buffers.aux_vec.as_mut_ptr();
//
//     Ok(())
// }

// fn decode_delta_len_string_to_varchar<DIter: Iterator<Item=u8>>(
//     values_buffer: &[u8],
//     def_levels_iter: DIter,
//     buffers: &mut ColumnChunkBuffers,
//     row_count: usize,
// ) -> anyhow::Result<()> {
//     let mut decoder = delta_bitpacked::Decoder::try_new(values_buffer)?;
//     let mut count = 0;
//
//     let data_vec = &mut buffers.data_vec;
//     let aux_vec = &mut buffers.aux_vec;
//     aux_vec.reserve(row_count * size_of::<i64>());
//
//     let lengths: Vec<i64> = (decoder
//         .by_ref()
//         .collect::<Result<Vec<i64>, parquet2::error::Error>>())?;
//     let mut data_offset = decoder.consumed_bytes();
//     let mut non_null_count = 0usize;
//
//     for value in def_levels_iter {
//         if count < row_count {
//             if value == 1u8 {
//                 let len = lengths[non_null_count] as usize;
//                 non_null_count += 1;
//                 let slice = &values_buffer[data_offset..data_offset + len];
//                 append_varchar(aux_vec, data_vec, slice);
//                 data_offset += len;
//             } else {
//                 append_varchar_null(aux_vec, data_vec);
//             }
//             count += 1;
//         } else {
//             break;
//         }
//     }
//     buffers.data_size = data_vec.len();
//     buffers.data_ptr = data_vec.as_mut_ptr();
//
//     buffers.aux_size = aux_vec.len();
//     buffers.aux_ptr = buffers.aux_vec.as_mut_ptr();
//
//     Ok(())
// }

// fn decode_delta_len_string_to_string<DIter: Iterator<Item=u8>>(
//     values_buffer: &[u8],
//     def_levels_iter: DIter,
//     buffers: &mut ColumnChunkBuffers,
//     row_count: usize,
// ) -> anyhow::Result<()> {
//     let mut decoder = delta_bitpacked::Decoder::try_new(values_buffer)?;
//     let data_vec = &mut buffers.data_vec;
//     let aux_vec = &mut buffers.aux_vec;
//     if aux_vec.is_empty() {
//         aux_vec.extend_from_slice(&0u64.to_le_bytes());
//     }
//     aux_vec.reserve(row_count * size_of::<i64>());
//     let null_value = (-1i32).to_le_bytes();
//
//     let lengths: Vec<i64> = (decoder
//         .by_ref()
//         .collect::<Result<Vec<i64>, parquet2::error::Error>>())?;
//     let mut data_offset = decoder.consumed_bytes();
//     let mut non_null_count = 0usize;
//     let count = def_levels_iter.size_hint().0;
//
//     for value in def_levels_iter {
//         if value == 1u8 {
//             let len = lengths[non_null_count] as usize;
//             non_null_count += 1;
//             let slice = &values_buffer[data_offset..data_offset + len];
//             data_offset += len;
//
//             let str = String::from_utf8(slice.to_vec())?;
//             let utf16 = str.encode_utf16();
//
//             let pos = data_vec.len();
//             data_vec.resize(pos + size_of::<i32>(), 0);
//             let mut utf16_len: i32 = 0;
//             utf16.for_each(|c| {
//                 data_vec.extend_from_slice(&c.to_le_bytes());
//                 utf16_len += 1;
//             });
//
//             data_vec[pos..pos + size_of::<i32>()].copy_from_slice(&utf16_len.to_le_bytes());
//             aux_vec.extend_from_slice(&(data_vec.len() as u64).to_le_bytes());
//         } else {
//             data_vec.extend_from_slice(&null_value);
//             aux_vec.extend_from_slice(&(data_vec.len() as u64).to_le_bytes());
//         }
//     }
//     buffers.data_size = data_vec.len();
//     buffers.data_ptr = data_vec.as_mut_ptr();
//
//     if count == 0 {
//         aux_vec.clear();
//     }
//     buffers.aux_size = aux_vec.len();
//     buffers.aux_ptr = buffers.aux_vec.as_mut_ptr();
//
//     Ok(())
// }

fn decode_page<T: Pushable>(
    version: Version,
    page: &DataPage,
    row_count: usize,
    sink: &mut T,
) -> ParquetResult<()> {
    sink.reserve();
    let iter = decode_null_bitmap(version, page, row_count)?;

    if let Some(iter) = iter {
        for run in iter {
            let run = run?;
            match run {
                FilteredHybridEncoded::Bitmap {
                    values,
                    offset,
                    length,
                } => {
                    // consume `length` items
                    let iter = BitmapIter::new(values, offset, length);
                    for item in iter {
                        if item {
                            sink.push();
                        } else {
                            sink.push_null();
                        }
                    }
                }
                FilteredHybridEncoded::Repeated { is_set, length } => {
                    if is_set {
                        sink.push_slice(length);
                    } else {
                        sink.push_nulls(length);
                    }
                }
                FilteredHybridEncoded::Skipped(valids) => { sink.skip(valids); }
            };
        }
    } else {
        sink.push_slice(row_count);
    }
    sink.result()
}

fn decode_page_bool(
    version: Version,
    page: &DataPage,
    values_buffer: &[u8],
    row_count: usize,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    let iter = decode_null_bitmap(version, page, row_count)?;

    if let Some(_iter) = iter {
        return Err(Error::FeatureNotSupported("nullable boolean values are not supported yet".to_string()));
    } else {
        buffers.data_vec.reserve(row_count);
        let iter = BitmapIter::new(values_buffer, 0, row_count);
        for item in iter {
            if item {
                buffers.data_vec.push(1u8);
            } else {
                buffers.data_vec.push(0u8);
            }
        }
        Ok(())
    }
}


pub fn decode_null_bitmap(
    _version: Version,
    page: &DataPage,
    count: usize,
) -> ParquetResult<Option<FilteredHybridRleDecoderIter>> {
    let def_levels = split_buffer(page)?.1;
    if def_levels.is_empty() {
        return Ok(None);
    }

    let iter = hybrid_rle::Decoder::new(def_levels, 1);
    let iter = HybridDecoderBitmapIter::new(iter, count);
    let selected_rows = get_selected_rows(page);
    let iter = FilteredHybridRleDecoderIter::new(iter, selected_rows);
    Ok(Some(iter))
}

pub fn get_selected_rows(page: &DataPage) -> VecDeque<Interval> {
    page.selected_rows()
        .unwrap_or(&[Interval::new(0, page.num_values())])
        .iter()
        .copied()
        .collect()
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
    use crate::parquet_write::varchar::{append_varchar, append_varchar_null};
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
            let column_typ = decoder.columns[column_index].typ;
            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(row_group_index, column_index, column_typ)
                    .unwrap();

                let ccb = &decoder.column_buffers[column_index];
                assert_eq!(ccb.data_size, expected_buff.len());
                assert_eq!(ccb.aux_size, 0);
                assert_eq!(ccb.data_vec, expected_buff);
            }
        }
    }

    // #[test]
    // fn test_decode_file() {
    //     let file = File::open("/Users/alpel/temp/db/requests_log.parquet").unwrap();
    //     let mut decoder = ParquetDecoder::read(file).unwrap();
    //     let row_group_count = decoder.row_group_count as usize;
    //     let column_count = decoder.columns.len();
    //
    //     for column_index in 0..column_count {
    //         let mut col_row_count = 0usize;
    //
    //         let column_type = decoder.columns[column_index].typ;
    //         for row_group_index in 0..row_group_count {
    //             decoder
    //                 .decode_column_chunk(row_group_index, column_index, column_type)
    //                 .unwrap();
    //
    //             let ccb = &decoder.column_buffers[column_index];
    //             assert_eq!(ccb.data_vec.len(), ccb.data_size);
    //
    //             col_row_count += ccb.row_count;
    //         }
    //
    //         assert_eq!(col_row_count, decoder.row_count);
    //     }
    // }

    #[test]
    fn test_decode_int_long_column_v2_nulls_multi_groups() {
        let row_count = 10000;
        let row_group_size = 1000;
        let data_page_size = 1000;
        let version = Version::V1;
        let mut columns = Vec::new();

        let mut expected_buffs: Vec<(&[u8], Option<&[u8]>, ColumnType)> = Vec::new();
        let expected_int_buff = create_col_data_buff_int(row_count);
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "int_col",
            &expected_int_buff,
            ColumnType::Int,
        ));
        expected_buffs.push((&expected_int_buff, None, ColumnType::Int));

        let expected_long_buff = create_col_data_buff_long(row_count);
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "long_col",
            &expected_long_buff,
            ColumnType::Long,
        ));
        expected_buffs.push((&expected_long_buff, None, ColumnType::Long));

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

        expected_buffs.push((
            &expected_string_primary_buff,
            Some(&expected_string_aux_buff),
            ColumnType::String,
        ));

        let (expected_varchar_primary_buff, expected_varchar_aux_buff) =
            create_col_data_buff_varchar(row_count, 3);
        columns.push(create_var_column(
            columns.len() as i32,
            row_count,
            "string_col",
            &expected_varchar_primary_buff,
            &expected_varchar_aux_buff,
            ColumnType::Varchar,
        ));
        expected_buffs.push((
            &expected_varchar_primary_buff,
            Some(&expected_varchar_aux_buff),
            ColumnType::Varchar,
        ));

        let (
            symbol_col_primary_buff,
            symbol_col_chars_buff,
            symbol_col_offsets_buff,
            expected_symbol_col_varchar_data_buff,
            expected_symbol_col_varchar_aux_buff,
        ) = create_col_data_buff_symbol(row_count, 10);

        columns.push(create_symbol_column(
            columns.len() as i32,
            row_count,
            "string_col",
            &symbol_col_primary_buff,
            &symbol_col_chars_buff,
            &symbol_col_offsets_buff,
            ColumnType::Symbol,
        ));
        expected_buffs.push((
            &expected_symbol_col_varchar_data_buff,
            Some(&expected_symbol_col_varchar_aux_buff),
            ColumnType::Varchar,
        ));

        assert_columns(row_count, row_group_size, data_page_size, version, columns, &expected_buffs);
    }

    #[test]
    fn test_decode_column_type2() {
        let row_count = 10000;
        let row_group_size = 1000;
        let data_page_size = 1000;
        let version = Version::V2;
        let mut columns = Vec::new();
        let mut expected_buffs: Vec<(&[u8], Option<&[u8]>, ColumnType)> = Vec::new();

        // let expected_bool_buff = create_col_data_buff_bool(row_count);
        // columns.push(create_fix_column(
        //     columns.len() as i32,
        //     row_count,
        //     "bool_col",
        //     &expected_bool_buff,
        //     ColumnType::Boolean,
        // ));
        // expected_buffs.push((&expected_bool_buff, None, ColumnType::Boolean));
        //
        // let expected_bool_buff = create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| short.to_le_bytes());
        // columns.push(create_fix_column(
        //     columns.len() as i32,
        //     row_count,
        //     "bool_short",
        //     &expected_bool_buff,
        //     ColumnType::Short,
        // ));
        // expected_buffs.push((&expected_bool_buff, None, ColumnType::Short));

        let expected_bool_buff = create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| short.to_le_bytes());
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_char",
            &expected_bool_buff,
            ColumnType::Char,
        ));
        expected_buffs.push((&expected_bool_buff, None, ColumnType::Char));

        assert_columns(row_count, row_group_size, data_page_size, version, columns, &expected_buffs);
    }

    fn assert_columns(row_count: usize, row_group_size: usize, data_page_size: usize, version: Version, columns: Vec<Column>, expected_buffs: &Vec<(&[u8], Option<&[u8]>, ColumnType)>) {
        let column_count = columns.len();
        let file = write_cols_to_parquet_file(row_group_size, data_page_size, version, columns);

        let mut decoder = ParquetDecoder::read(file).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;

        for (column_index, (expected, expected_aux, column_type)) in
        expected_buffs.into_iter().enumerate()
        {
            let column_type = *column_type;
            let mut data_offset = 0usize;
            let mut col_row_count = 0usize;

            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(row_group_index, column_index, column_type)
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
                    if col_row_count == 0 {
                        assert_eq!(&expected_aux_data[0..ccb.aux_size], ccb.aux_vec);
                    } else if column_type == ColumnType::String {
                        let mut expected_aux_data_slice = vec![];
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
                        assert_eq!(expected_aux_data_slice, ccb.aux_vec);
                    }
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

    fn create_col_data_buff_bool(row_count: usize) -> Vec<u8>
    {
        let value_size = 1;
        let mut buff = vec![0u8; row_count * value_size];
        for i in 0..row_count {
            let value = i % 3 == 0;
            let offset = i * value_size;
            let bval = if value { 1u8 } else { 0u8 };
            buff[offset] = bval;
        }
        buff
    }

    fn create_col_data_buff<T, const N: usize, F>(row_count: usize, null_value: [u8; N], to_le_bytes: F)
                                                  -> Vec<u8>
        where
            T: From<i16> + Copy,
            F: Fn(T) -> [u8; N],
    {
        let value_size = N;
        let mut buff = vec![0u8; row_count * value_size];
        for i in 0..((row_count + 1) / 2) {
            let value = T::from(i as i16);
            let offset = 2 * i * value_size;
            buff[offset..offset + value_size].copy_from_slice(&to_le_bytes(value));

            if offset + 2 * value_size <= buff.len() {
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

        // 0x00A0..0xD7FF generates a random Unicode scalar value in a range that includes non-ASCII characters
        let range = if rng.gen_bool(0.5) {
            0x00A0..0xD7FF
        } else {
            33..126
        };

        let random_string: String = (0..len)
            .map(|_| {
                let c = rng.gen_range(range.clone());
                char::from_u32(c).unwrap_or('�') // Use a replacement character for invalid values
            })
            .collect();

        random_string
    }

    #[allow(clippy::type_complexity)]
    fn create_col_data_buff_symbol(
        row_count: usize,
        distinct_values: usize,
    ) -> (Vec<u8>, Vec<u8>, Vec<u64>, Vec<u8>, Vec<u8>) {
        let mut symbol_data_buff = Vec::new();
        let mut expected_aux_buff = Vec::new();
        let mut expected_data_buff = Vec::new();

        let str_values: Vec<String> = (0..distinct_values)
            .map(|_| generate_random_unicode_string(10))
            .collect();

        let (symbol_chars_buff, symbol_offsets_buff) = serialize_as_symbols(&str_values);

        let mut i = 0;
        let null_sym_value = (i32::MIN).to_le_bytes();
        while i < row_count {
            let symbol_value = i % distinct_values;
            symbol_data_buff.extend_from_slice(&(symbol_value as i32).to_le_bytes());

            let str_value = &str_values[i % distinct_values];
            append_varchar(
                &mut expected_aux_buff,
                &mut expected_data_buff,
                str_value.as_bytes(),
            );
            i += 1;

            if i < row_count {
                symbol_data_buff.extend_from_slice(&null_sym_value);
                append_varchar_null(&mut expected_aux_buff, &mut expected_data_buff);
                i += 1;
            }
        }

        (
            symbol_data_buff,
            symbol_chars_buff,
            symbol_offsets_buff,
            expected_data_buff,
            expected_aux_buff,
        )
    }

    fn serialize_as_symbols(symbol_chars: &Vec<String>) -> (Vec<u8>, Vec<u64>) {
        let mut chars = vec![];
        let mut offsets = vec![];

        for s in symbol_chars {
            let sym_chars: Vec<_> = s.encode_utf16().collect();
            let len = sym_chars.len();
            offsets.push(chars.len() as u64);
            chars.extend_from_slice(&(len as u32).to_le_bytes());
            let encoded: &[u8] = unsafe {
                std::slice::from_raw_parts(
                    sym_chars.as_ptr() as *const u8,
                    sym_chars.len() * size_of::<u16>(),
                )
            };
            chars.extend_from_slice(encoded);
        }

        (chars, offsets)
    }

    fn create_col_data_buff_varchar(
        row_count: usize,
        distinct_values: usize,
    ) -> (Vec<u8>, Vec<u8>) {
        let mut aux_buff = Vec::new();
        let mut data_buff = Vec::new();

        let str_values: Vec<String> = (0..distinct_values)
            .map(|_| generate_random_unicode_string(10))
            .collect();

        let mut i = 0;
        while i < row_count {
            let str_value = &str_values[i % distinct_values];
            append_varchar(&mut aux_buff, &mut data_buff, str_value.as_bytes());
            i += 1;

            if i < row_count {
                append_varchar_null(&mut aux_buff, &mut data_buff);
                i += 1;
            }
        }
        (data_buff, aux_buff)
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
        primary_data: &[u8],
        aux_data: &[u8],
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

    fn create_symbol_column(
        id: i32,
        row_count: usize,
        name: &'static str,
        primary_data: &[u8],
        chars_data: &[u8],
        offsets: &[u64],
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
            chars_data.as_ptr(),
            chars_data.len(),
            offsets.as_ptr(),
            offsets.len(),
        )
            .unwrap()
    }
}
