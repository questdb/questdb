use crate::parquet_read::column_sink::fixed::{
    FixedBooleanColumnSink, FixedDoubleColumnSink, FixedFloatColumnSink, FixedInt2ByteColumnSink,
    FixedInt2ShortColumnSink, FixedIntColumnSink, FixedLong256ColumnSink, FixedLongColumnSink,
    IntDecimalColumnSink, NanoTimestampColumnSink, ReverseFixedColumnSink,
};
use crate::parquet_read::column_sink::var::{
    BinaryColumnSink, StringColumnSink, VarcharColumnSink,
};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::dict_decoder::{FixedDictDecoder, VarDictDecoder};
use crate::parquet_read::slicer::rle::RleDictionarySlicer;
use crate::parquet_read::slicer::{
    BooleanBitmapSlicer, DataPageFixedSlicer, DeltaBinaryPackedSlicer, DeltaBytesArraySlicer,
    DeltaLengthArraySlicer, PlainVarSlicer, ValueConvertSlicer,
};
use crate::parquet_read::{ColumnChunkBuffers, ParquetDecoder};
use crate::parquet_write::schema::ColumnType;
use crate::parquet_write::ParquetResult;
use anyhow::{anyhow, Context};
use parquet2::deserialize::{
    FilteredHybridEncoded, FilteredHybridRleDecoderIter, HybridDecoderBitmapIter,
};
use parquet2::encoding::hybrid_rle::{BitmapIter, HybridEncoded};
use parquet2::encoding::{bitpacked, hybrid_rle, Encoding};
use parquet2::indexes::Interval;
use parquet2::page::{split_buffer, DataPage, DictPage, Page};
use parquet2::read::{decompress, get_page_iterator};
use parquet2::schema::types::{
    PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use parquet2::write::Version;
use std::collections::VecDeque;
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

const UUID_NULL: [u8; 16] = unsafe { std::mem::transmute([i64::MIN; 2]) };
const LONG256_NULL: [u8; 32] = unsafe { std::mem::transmute([i64::MIN; 4]) };
const BYTE_NULL: [u8; 1] = [0u8];
const INT_NULL: [u8; 4] = i32::MIN.to_le_bytes();
const SHORT_NULL: [u8; 2] = 0i16.to_le_bytes();
const LONG_NULL: [u8; 8] = i64::MIN.to_le_bytes();
const DOUBLE_NULL: [u8; 8] = unsafe { std::mem::transmute([f64::NAN]) };
const FLOAT_NULL: [u8; 4] = unsafe { std::mem::transmute([f32::NAN]) };
const TIMESTAMP_96_EMPTY: [u8; 12] = [0; 12];

impl ParquetDecoder {
    pub fn decode_column_chunk(
        &mut self,
        row_group: usize,
        file_column_index: usize,
        column: usize,
        column_type: ColumnType,
    ) -> anyhow::Result<()> {
        let columns = self.metadata.row_groups[row_group].columns();
        let column_metadata = &columns[file_column_index];
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
                    row_count += decoder_page(version, &page, dict.as_ref(), buffers, column_type)
                        .with_context(|| {
                        format!(
                            "failed to decode row group [column={}, group={}]",
                            self.metadata.schema_descr.columns()[file_column_index]
                                .descriptor
                                .primitive_type
                                .field_info
                                .name,
                            row_group
                        )
                    })?
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

    let encoding_error = true;
    let decoding_result = match (
        page.descriptor.primitive_type.physical_type,
        page.descriptor.primitive_type.logical_type,
        page.descriptor.primitive_type.converted_type,
    ) {
        (PhysicalType::Int32, logical_type, converted_type) => {
            match (page.encoding(), dict, logical_type, column_type) {
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnType::Short | ColumnType::Char | ColumnType::GeoShort,
                ) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedInt2ShortColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            buffers,
                            &SHORT_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnType::Short | ColumnType::Char | ColumnType::GeoShort,
                ) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedInt2ShortColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<2>::try_new(values_buffer, row_count)?,
                            buffers,
                            &SHORT_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (Encoding::Plain, _, _, ColumnType::Byte | ColumnType::GeoByte) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedInt2ByteColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            buffers,
                            &BYTE_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (Encoding::DeltaBinaryPacked, _, _, ColumnType::Byte | ColumnType::GeoByte) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedInt2ByteColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<1>::try_new(values_buffer, row_count)?,
                            buffers,
                            &BYTE_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnType::Int | ColumnType::GeoInt | ColumnType::IPv4,
                ) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedIntColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            buffers,
                            &INT_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (Encoding::Plain, _, _, ColumnType::Date) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedLongColumnSink::new(
                            &mut ValueConvertSlicer::<8, _, _>::new(
                                DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                                |int_val: &[u8], buff: &mut [u8; 8]| {
                                    let days_since_epoch = unsafe {
                                        ptr::read_unaligned(int_val.as_ptr() as *const i32)
                                    };
                                    let date = days_since_epoch as i64 * 24 * 60 * 60 * 1000;
                                    buff.copy_from_slice(&date.to_le_bytes());
                                },
                            ),
                            buffers,
                            &LONG_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnType::Int | ColumnType::GeoInt | ColumnType::IPv4,
                ) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedIntColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<4>::try_new(values_buffer, row_count)?,
                            buffers,
                            &INT_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnType::Int | ColumnType::GeoInt | ColumnType::IPv4,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_count,
                        &INT_NULL,
                    )?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedIntColumnSink::new(&mut slicer, buffers, &INT_NULL),
                    )?;

                    Ok(row_count)
                }
                (encoding, dict, logical_type, ColumnType::Double) => {
                    let scale = match logical_type {
                        Some(PrimitiveLogicalType::Decimal(_, scale)) => scale,
                        _ => match converted_type {
                            Some(PrimitiveConvertedType::Decimal(_, scale)) => scale,
                            _ => 0,
                        },
                    };

                    match (encoding, dict) {
                        (Encoding::RleDictionary | Encoding::PlainDictionary, Some(dict_page)) => {
                            let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                            let mut slicer = RleDictionarySlicer::try_new(
                                values_buffer,
                                dict_decoder,
                                row_count,
                                &INT_NULL,
                            )?;
                            decode_page(
                                version,
                                page,
                                row_count,
                                &mut IntDecimalColumnSink::new(
                                    &mut slicer,
                                    buffers,
                                    &DOUBLE_NULL,
                                    scale as i32,
                                ),
                            )?;

                            Ok(row_count)
                        }
                        (Encoding::Plain, _) => {
                            decode_page(
                                version,
                                page,
                                row_count,
                                &mut IntDecimalColumnSink::new(
                                    &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                                    buffers,
                                    &DOUBLE_NULL,
                                    scale as i32,
                                ),
                            )?;
                            Ok(row_count)
                        }
                        _ => Err(encoding_error),
                    }
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnType::Short | ColumnType::Char | ColumnType::GeoShort,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_count,
                        &INT_NULL,
                    )?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedInt2ShortColumnSink::new(&mut slicer, buffers, &SHORT_NULL),
                    )?;

                    Ok(row_count)
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Int64, logical_type, _) => {
            match (page.encoding(), dict, logical_type, column_type) {
                (
                    Encoding::Plain,
                    _,
                    Some(PrimitiveLogicalType::Timestamp {
                        is_adjusted_to_utc: _,
                        unit: TimeUnit::Nanoseconds | TimeUnit::Milliseconds,
                    }),
                    ColumnType::Timestamp,
                ) => {
                    if let Some(PrimitiveLogicalType::Timestamp {
                        is_adjusted_to_utc: _,
                        unit: ts_unit,
                    }) = logical_type
                    {
                        let mut slicer = ValueConvertSlicer::<8, _, _>::new(
                            DataPageFixedSlicer::<8>::new(values_buffer, row_count),
                            |nano_ts, out_buff| {
                                let ts =
                                    unsafe { ptr::read_unaligned(nano_ts.as_ptr() as *const i64) };
                                let ts = match ts_unit {
                                    TimeUnit::Nanoseconds => ts / 1000,
                                    TimeUnit::Microseconds => ts,
                                    TimeUnit::Milliseconds => ts * 1000,
                                };
                                out_buff.copy_from_slice(&ts.to_le_bytes());
                            },
                        );

                        decode_page(
                            version,
                            page,
                            row_count,
                            &mut FixedLongColumnSink::new(&mut slicer, buffers, &LONG_NULL),
                        )?;
                    } else {
                        unreachable!("Timestamp logical type must be set");
                    }
                    Ok(row_count)
                }
                (
                    Encoding::Plain,
                    _,
                    _,
                    ColumnType::Long
                    | ColumnType::Date
                    | ColumnType::GeoLong
                    | ColumnType::Timestamp,
                ) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedLongColumnSink::new(
                            &mut DataPageFixedSlicer::<8>::new(values_buffer, row_count),
                            buffers,
                            &LONG_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::DeltaBinaryPacked,
                    _,
                    _,
                    ColumnType::Long
                    | ColumnType::Timestamp
                    | ColumnType::Date
                    | ColumnType::GeoLong,
                ) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedLongColumnSink::new(
                            &mut DeltaBinaryPackedSlicer::<8>::try_new(values_buffer, row_count)?,
                            buffers,
                            &LONG_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    _,
                    ColumnType::Long
                    | ColumnType::Timestamp
                    | ColumnType::Date
                    | ColumnType::GeoLong,
                ) => {
                    let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_count,
                        &LONG_NULL,
                    )?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedLongColumnSink::new(&mut slicer, buffers, &LONG_NULL),
                    )?;

                    Ok(row_count)
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(16), Some(PrimitiveLogicalType::Uuid), _) => {
            match (page.encoding(), column_type) {
                (Encoding::Plain, ColumnType::Uuid) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut ReverseFixedColumnSink::new(
                            &mut DataPageFixedSlicer::<16>::new(values_buffer, row_count),
                            buffers,
                            UUID_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::FixedLenByteArray(32), _logical_type, _) => {
            match (page.encoding(), column_type) {
                (Encoding::Plain, ColumnType::Long256) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedLong256ColumnSink::new(
                            &mut DataPageFixedSlicer::<32>::new(values_buffer, row_count),
                            buffers,
                            &LONG256_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String), _)
        | (PhysicalType::ByteArray, _, Some(PrimitiveConvertedType::Utf8)) => {
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

                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnType::Varchar,
                ) => {
                    let dict_decoder = VarDictDecoder::try_new(dict_page, true)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_count,
                        &LONG256_NULL,
                    )?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut VarcharColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }

                (Encoding::Plain, None, ColumnType::String) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut StringColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }

                (Encoding::Plain, _, ColumnType::Varchar) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut VarcharColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }

                (Encoding::DeltaByteArray, _, ColumnType::Varchar) => {
                    let mut slicer = DeltaBytesArraySlicer::try_new(values_buffer, row_count)?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut VarcharColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }

                _ => Err(encoding_error),
            }
        }
        (PhysicalType::ByteArray, None, _) => {
            let encoding = page.encoding();
            match (encoding, dict, column_type) {
                (Encoding::Plain, None, ColumnType::Binary) => {
                    let mut slicer = PlainVarSlicer::new(values_buffer, row_count);
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut BinaryColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }
                (Encoding::DeltaLengthByteArray, None, ColumnType::Binary) => {
                    let mut slicer = DeltaLengthArraySlicer::try_new(values_buffer, row_count)?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut BinaryColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    ColumnType::Binary,
                ) => {
                    let dict_decoder = VarDictDecoder::try_new(dict_page, false)?;
                    let mut slicer =
                        RleDictionarySlicer::try_new(values_buffer, dict_decoder, row_count, &[])?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut BinaryColumnSink::new(&mut slicer, buffers),
                    )?;
                    Ok(row_count)
                }
                _ => Err(encoding_error),
            }
        }
        (PhysicalType::Int96, logical_type, _) => {
            // Int96 is used for nano timestamps
            match (page.encoding(), dict, logical_type, column_type) {
                (Encoding::Plain, _, _, ColumnType::Timestamp) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut NanoTimestampColumnSink::new(
                            &mut DataPageFixedSlicer::<12>::new(values_buffer, row_count),
                            buffers,
                            &LONG_NULL,
                        ),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::PlainDictionary | Encoding::RleDictionary,
                    Some(dict_page),
                    _,
                    ColumnType::Timestamp,
                ) => {
                    let dict_decoder = FixedDictDecoder::<12>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_count,
                        &TIMESTAMP_96_EMPTY,
                    )?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut NanoTimestampColumnSink::new(&mut slicer, buffers, &LONG_NULL),
                    )?;
                    Ok(row_count)
                }
                _ => Err(encoding_error),
            }
        }
        (typ, None, _) => {
            buffers.aux_vec.clear();
            buffers.aux_ptr = ptr::null_mut();

            match (page.encoding(), dict, typ, column_type) {
                (Encoding::Plain, None, PhysicalType::Double, ColumnType::Double) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedDoubleColumnSink::new(
                            &mut DataPageFixedSlicer::<8>::new(values_buffer, row_count),
                            buffers,
                            &DOUBLE_NULL,
                        ),
                    )?;

                    Ok(row_count)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    PhysicalType::Double,
                    ColumnType::Double,
                ) => {
                    let dict_decoder = FixedDictDecoder::<8>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_count,
                        &DOUBLE_NULL,
                    )?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedDoubleColumnSink::new(&mut slicer, buffers, &DOUBLE_NULL),
                    )?;
                    Ok(row_count)
                }
                (
                    Encoding::RleDictionary | Encoding::PlainDictionary,
                    Some(dict_page),
                    PhysicalType::Float,
                    ColumnType::Float,
                ) => {
                    let dict_decoder = FixedDictDecoder::<4>::try_new(dict_page)?;
                    let mut slicer = RleDictionarySlicer::try_new(
                        values_buffer,
                        dict_decoder,
                        row_count,
                        &FLOAT_NULL,
                    )?;
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedFloatColumnSink::new(&mut slicer, buffers, &FLOAT_NULL),
                    )?;
                    Ok(row_count)
                }
                (Encoding::Plain, None, PhysicalType::Float, ColumnType::Float) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedFloatColumnSink::new(
                            &mut DataPageFixedSlicer::<4>::new(values_buffer, row_count),
                            buffers,
                            &FLOAT_NULL,
                        ),
                    )?;

                    Ok(row_count)
                }
                (Encoding::Plain, None, PhysicalType::Boolean, ColumnType::Boolean) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedBooleanColumnSink::new(
                            &mut BooleanBitmapSlicer::new(values_buffer, row_count),
                            buffers,
                            &[0],
                        ),
                    )?;

                    Ok(row_count)
                }
                (Encoding::Rle, None, PhysicalType::Boolean, ColumnType::Boolean) => {
                    decode_page(
                        version,
                        page,
                        row_count,
                        &mut FixedBooleanColumnSink::new(
                            &mut BooleanBitmapSlicer::new(values_buffer, row_count),
                            buffers,
                            &[0],
                        ),
                    )?;

                    Ok(row_count)
                }
                _ => Err(encoding_error),
            }
        }
        _ => Err(encoding_error),
    };

    match decoding_result {
        Ok(row_count) => Ok(row_count),
        Err(_) => {
            Err(anyhow!(
            "encoding not supported, physical type: {:?}, encoding {:?}, logical type {:?}, converted type: {:?}, column type {:?}",
            page.descriptor.primitive_type.physical_type,
            page.encoding(),
            page.descriptor.primitive_type.logical_type,
            page.descriptor.primitive_type.converted_type,
            column_type
            ))
        }
    }
}

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
                FilteredHybridEncoded::Bitmap { values, offset, length } => {
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
                FilteredHybridEncoded::Skipped(valids) => {
                    sink.skip(valids);
                }
            };
        }
    } else {
        sink.push_slice(row_count);
    }
    sink.result()
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
    use crate::parquet_read::decode::{INT_NULL, LONG_NULL, UUID_NULL};
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
        let expected_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        let column_count = 1;
        let file = write_parquet_file(
            row_count,
            row_group_size,
            data_page_size,
            version,
            expected_buff.data_vec.as_ref(),
        );

        let mut decoder = ParquetDecoder::read(file).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;

        for column_index in 0..column_count {
            let column_typ = decoder.columns[column_index].typ;
            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(row_group_index, column_index, column_index, column_typ)
                    .unwrap();

                let ccb = &decoder.column_buffers[column_index];
                assert_eq!(ccb.data_size, expected_buff.data_vec.len());
                assert_eq!(ccb.aux_size, 0);
                assert_eq!(ccb.data_vec, expected_buff.data_vec);
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

        let mut expected_buffs: Vec<(ColumnBuffers, ColumnType)> = Vec::new();
        let expected_int_buff =
            create_col_data_buff::<i32, 4, _>(row_count, INT_NULL, |int| int.to_le_bytes());
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "int_col",
            expected_int_buff.data_vec.as_ref(),
            ColumnType::Int,
        ));
        expected_buffs.push((expected_int_buff, ColumnType::Int));

        let expected_long_buff =
            create_col_data_buff::<i64, 8, _>(row_count, LONG_NULL, |int| int.to_le_bytes());
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "long_col",
            expected_long_buff.data_vec.as_ref(),
            ColumnType::Long,
        ));
        expected_buffs.push((expected_long_buff, ColumnType::Long));

        let string_buffers = create_col_data_buff_string(row_count, 3);
        columns.push(create_var_column(
            columns.len() as i32,
            row_count,
            "string_col",
            string_buffers.data_vec.as_ref(),
            string_buffers.aux_vec.as_ref().unwrap(),
            ColumnType::String,
        ));
        expected_buffs.push((string_buffers, ColumnType::String));

        let string_buffers = create_col_data_buff_varchar(row_count, 3);
        columns.push(create_var_column(
            columns.len() as i32,
            row_count,
            "varchar_col",
            string_buffers.data_vec.as_ref(),
            string_buffers.aux_vec.as_ref().unwrap(),
            ColumnType::Varchar,
        ));
        expected_buffs.push((string_buffers, ColumnType::Varchar));

        let symbol_buffs = create_col_data_buff_symbol(row_count, 10);
        columns.push(create_symbol_column(
            columns.len() as i32,
            row_count,
            "string_col",
            symbol_buffs.data_vec.as_ref(),
            symbol_buffs.sym_chars.as_ref().unwrap(),
            symbol_buffs.sym_offsets.as_ref().unwrap(),
            ColumnType::Symbol,
        ));
        expected_buffs.push((symbol_buffs, ColumnType::Varchar));

        assert_columns(
            row_count,
            row_group_size,
            data_page_size,
            version,
            columns,
            &expected_buffs,
        );
    }

    #[test]
    fn test_decode_column_type2() {
        let row_count = 10000;
        let row_group_size = 1000;
        let data_page_size = 1000;
        let version = Version::V2;
        let mut columns = Vec::new();
        let mut expected_buffs: Vec<(ColumnBuffers, ColumnType)> = Vec::new();

        let expected_bool_buff = create_col_data_buff_bool(row_count);
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_col",
            expected_bool_buff.data_vec.as_ref(),
            ColumnType::Boolean,
        ));
        expected_buffs.push((expected_bool_buff, ColumnType::Boolean));

        let expected_col_buff =
            create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| {
                short.to_le_bytes()
            });
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_short",
            expected_col_buff.data_vec.as_ref(),
            ColumnType::Short,
        ));
        expected_buffs.push((expected_col_buff, ColumnType::Short));

        let expected_bool_buff =
            create_col_data_buff::<i16, 2, _>(row_count, i16::MIN.to_le_bytes(), |short| {
                short.to_le_bytes()
            });
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_char",
            expected_bool_buff.data_vec.as_ref(),
            ColumnType::Char,
        ));
        expected_buffs.push((expected_bool_buff, ColumnType::Char));

        let expected_uuid_buff =
            create_col_data_buff::<i128, 16, _>(row_count, UUID_NULL, |uuid| uuid.to_le_bytes());
        columns.push(create_fix_column(
            columns.len() as i32,
            row_count,
            "bool_char",
            expected_uuid_buff.data_vec.as_ref(),
            ColumnType::Uuid,
        ));
        expected_buffs.push((expected_uuid_buff, ColumnType::Uuid));

        assert_columns(
            row_count,
            row_group_size,
            data_page_size,
            version,
            columns,
            &expected_buffs,
        );
    }

    fn assert_columns(
        row_count: usize,
        row_group_size: usize,
        data_page_size: usize,
        version: Version,
        columns: Vec<Column>,
        expected_buffs: &[(ColumnBuffers, ColumnType)],
    ) {
        let column_count = columns.len();
        let file = write_cols_to_parquet_file(row_group_size, data_page_size, version, columns);

        let mut decoder = ParquetDecoder::read(file).unwrap();
        assert_eq!(decoder.columns.len(), column_count);
        assert_eq!(decoder.row_count, row_count);
        let row_group_count = decoder.row_group_count as usize;

        for (column_index, (column_buffs, column_type)) in expected_buffs.iter().enumerate() {
            let column_type = *column_type;
            let mut data_offset = 0usize;
            let mut col_row_count = 0usize;
            let expected = column_buffs
                .expected_data_buff
                .as_ref()
                .unwrap_or(column_buffs.data_vec.as_ref());
            let expected_aux = column_buffs
                .expected_aux_buff
                .as_ref()
                .or(column_buffs.aux_vec.as_ref());

            for row_group_index in 0..row_group_count {
                decoder
                    .decode_column_chunk(row_group_index, column_index, column_index, column_type)
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

    fn create_col_data_buff_bool(row_count: usize) -> ColumnBuffers {
        let value_size = 1;
        let mut buff = vec![0u8; row_count * value_size];
        for i in 0..row_count {
            let value = i % 3 == 0;
            let offset = i * value_size;
            let bval = if value { 1u8 } else { 0u8 };
            buff[offset] = bval;
        }
        ColumnBuffers {
            data_vec: buff,
            aux_vec: None,
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
    }

    fn create_col_data_buff<T, const N: usize, F>(
        row_count: usize,
        null_value: [u8; N],
        to_le_bytes: F,
    ) -> ColumnBuffers
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
        ColumnBuffers {
            data_vec: buff,
            aux_vec: None,
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
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

    struct ColumnBuffers {
        data_vec: Vec<u8>,
        aux_vec: Option<Vec<u8>>,
        sym_offsets: Option<Vec<u64>>,
        sym_chars: Option<Vec<u8>>,
        expected_data_buff: Option<Vec<u8>>,
        expected_aux_buff: Option<Vec<u8>>,
    }

    fn create_col_data_buff_symbol(row_count: usize, distinct_values: usize) -> ColumnBuffers {
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
                append_varchar_null(&mut expected_aux_buff, &expected_data_buff);
                i += 1;
            }
        }

        ColumnBuffers {
            data_vec: symbol_data_buff,
            aux_vec: None,
            sym_offsets: Some(symbol_offsets_buff),
            sym_chars: Some(symbol_chars_buff),
            expected_data_buff: Some(expected_data_buff),
            expected_aux_buff: Some(expected_aux_buff),
        }
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

    fn create_col_data_buff_varchar(row_count: usize, distinct_values: usize) -> ColumnBuffers {
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
                append_varchar_null(&mut aux_buff, &data_buff);
                i += 1;
            }
        }
        ColumnBuffers {
            data_vec: data_buff,
            aux_vec: Some(aux_buff),
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
    }

    fn create_col_data_buff_string(row_count: usize, distinct_values: usize) -> ColumnBuffers {
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
        ColumnBuffers {
            data_vec: data_buff,
            aux_vec: Some(aux_buff),
            sym_offsets: None,
            sym_chars: None,
            expected_data_buff: None,
            expected_aux_buff: None,
        }
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
