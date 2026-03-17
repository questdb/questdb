use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use parquet2::encoding::hybrid_rle::encode_u32;
use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPageHeader, DataPageHeaderV1};
use parquet2::read::levels::get_bit_width;
use parquet2::schema::types::{FieldInfo, PhysicalType, PrimitiveType};
use parquet2::schema::Repetition;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_read::decode::def_level_iter::DefLevelBatchIter;
use questdbr::parquet_read::decode::vectorized::{
    GtInt64FilterSink, PrimitiveSinkWrapper, SumDoubleSink,
};
use questdbr::parquet_read::decode::{decode_page, BoundPageDecoder, PageDecoder};
use questdbr::parquet_read::decoders::PlainPrimitiveDecoder;
use questdbr::parquet_read::page;
use questdbr::parquet_read::ColumnChunkBuffers;
use questdbr::allocator::{MemTracking, QdbAllocator};
use questdbr::parquet::QdbMetaCol;
use std::hint::black_box;
use std::sync::atomic::AtomicUsize;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct BenchAllocator {
    _mem_tracking: Box<MemTracking>,
    _tagged_used: Box<AtomicUsize>,
    allocator: QdbAllocator,
}

impl BenchAllocator {
    fn new() -> Self {
        let mem_tracking = Box::new(MemTracking::new());
        let tagged_used = Box::new(AtomicUsize::new(0));
        let allocator = QdbAllocator::new(&*mem_tracking, &*tagged_used, 65);
        Self {
            _mem_tracking: mem_tracking,
            _tagged_used: tagged_used,
            allocator,
        }
    }
}

fn make_double_descriptor() -> Descriptor {
    Descriptor {
        primitive_type: PrimitiveType {
            field_info: FieldInfo {
                name: "col".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            physical_type: PhysicalType::Double,
        },
        max_def_level: 1,
        max_rep_level: 0,
    }
}

fn make_int64_descriptor() -> Descriptor {
    Descriptor {
        primitive_type: PrimitiveType {
            field_info: FieldInfo {
                name: "col".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            physical_type: PhysicalType::Int64,
        },
        max_def_level: 1,
        max_rep_level: 0,
    }
}

fn make_header(num_values: usize) -> DataPageHeader {
    DataPageHeader::V1(DataPageHeaderV1 {
        num_values: num_values as i32,
        encoding: Encoding::Plain.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: None,
    })
}

fn encode_def_levels(bitmap: &[bool]) -> Vec<u8> {
    let def_levels: Vec<u32> = bitmap.iter().map(|&v| if v { 1 } else { 0 }).collect();
    let len = def_levels.len();
    let bit_width = get_bit_width(1);
    let mut encoded = Vec::new();
    encode_u32(&mut encoded, def_levels.into_iter(), len, bit_width).unwrap();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    buf.extend_from_slice(&encoded);
    buf
}

fn build_double_page(values: &[f64], null_bitmap: Option<&[bool]>) -> (Vec<u8>, Vec<u8>) {
    let values_bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let page_buf = if let Some(bitmap) = null_bitmap {
        let mut buf = encode_def_levels(bitmap);
        // Only encode non-null values
        for (i, v) in values.iter().enumerate() {
            if bitmap[i] {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        buf
    } else {
        let all_non_null = vec![true; values.len()];
        let mut buf = encode_def_levels(&all_non_null);
        buf.extend_from_slice(&values_bytes);
        buf
    };
    (page_buf, values_bytes)
}

fn build_int64_page(values: &[i64], null_bitmap: Option<&[bool]>) -> (Vec<u8>, Vec<u8>) {
    let values_bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let page_buf = if let Some(bitmap) = null_bitmap {
        let mut buf = encode_def_levels(bitmap);
        for (i, v) in values.iter().enumerate() {
            if bitmap[i] {
                buf.extend_from_slice(&v.to_le_bytes());
            }
        }
        buf
    } else {
        let all_non_null = vec![true; values.len()];
        let mut buf = encode_def_levels(&all_non_null);
        buf.extend_from_slice(&values_bytes);
        buf
    };
    (page_buf, values_bytes)
}

// ---------------------------------------------------------------------------
// Aggregation benchmarks: SUM(double) via PageDecoder<SumDoubleSink>
// vs. materialize-then-sum via decode_page + ColumnChunkBuffers
// ---------------------------------------------------------------------------

fn bench_sum_double(c: &mut Criterion) {
    let mut group = c.benchmark_group("sum_double");

    for &n in &[1_000, 10_000, 100_000, 1_000_000] {
        let values: Vec<f64> = (0..n).map(|i| (i as f64) * 0.7).collect();
        let (page_buf, values_bytes) = build_double_page(&values, None);
        let header = make_header(n);
        let descriptor = make_double_descriptor();

        group.throughput(Throughput::Elements(n as u64));

        // Baseline: materialize into ColumnChunkBuffers, then sum
        let ba = BenchAllocator::new();
        let bufs: &'static mut ColumnChunkBuffers =
            Box::leak(Box::new(ColumnChunkBuffers::new(ba.allocator.clone())));
        let page_buf_ref: &'static [u8] = Box::leak(page_buf.clone().into_boxed_slice());
        let header_ref: &'static DataPageHeader = Box::leak(Box::new(header.clone()));
        let descriptor_ref: &'static Descriptor = Box::leak(Box::new(descriptor.clone()));

        group.bench_with_input(
            BenchmarkId::new("materialize_then_sum", n),
            &n,
            |b, &n| {
                let col_info = QdbMetaCol {
                    column_type: ColumnType::new(ColumnTypeTag::Double, 0),
                    column_top: 0,
                    format: None,
                    ascii: None,
                };
                b.iter(|| {
                    bufs.data_vec.clear();
                    bufs.aux_vec.clear();
                    let page_ref = page::DataPage {
                        buffer: page_buf_ref,
                        header: header_ref,
                        descriptor: descriptor_ref,
                    };
                    decode_page(
                        black_box(&page_ref),
                        None,
                        bufs,
                        col_info,
                        0,
                        n,
                    )
                    .unwrap();

                    // Now sum from materialized buffer
                    let data = &bufs.data_vec;
                    let count = data.len() / 8;
                    let mut sum = 0.0f64;
                    for i in 0..count {
                        let v = f64::from_le_bytes(
                            data[i * 8..(i + 1) * 8].try_into().unwrap(),
                        );
                        if !v.is_nan() {
                            sum += v;
                        }
                    }
                    black_box(sum);
                })
            },
        );

        // Vectorized: PageDecoder<SumDoubleSink> — no materialization
        let values_bytes_ref: &'static [u8] =
            Box::leak(values_bytes.clone().into_boxed_slice());
        let page_buf_ref2: &'static [u8] = Box::leak(page_buf.clone().into_boxed_slice());
        let header_ref2: &'static DataPageHeader = Box::leak(Box::new(header.clone()));
        let descriptor_ref2: &'static Descriptor = Box::leak(Box::new(descriptor.clone()));

        group.bench_with_input(
            BenchmarkId::new("vectorized_sum", n),
            &n,
            |b, &n| {
                b.iter(|| {
                    let page_ref = page::DataPage {
                        buffer: page_buf_ref2,
                        header: header_ref2,
                        descriptor: descriptor_ref2,
                    };
                    let iter = DefLevelBatchIter::new(
                        black_box(&page_ref),
                        0,
                        n,
                    )
                    .unwrap();
                    let mut sink = SumDoubleSink::new();
                    let pushable = PlainPrimitiveDecoder::<f64, f64>::new_for_sink(
                        values_bytes_ref, &mut sink, f64::NAN,
                    );
                    let mut wrapper = PrimitiveSinkWrapper(sink);
                    let mut decoder = BoundPageDecoder::new(iter, pushable);
                    decoder.decode_all(&mut wrapper).unwrap();
                    black_box(wrapper.0.sum);
                })
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Filter benchmarks: filter(i64 > threshold) via PageDecoder<FilterSink>
// vs. materialize-then-filter via decode_page + ColumnChunkBuffers
// ---------------------------------------------------------------------------

fn bench_filter_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_int64");

    for &n in &[1_000, 10_000, 100_000] {
        // ~10% selectivity: values 0..n, threshold = 90% of n
        let values: Vec<i64> = (0..n as i64).collect();
        let threshold = (n as i64) * 9 / 10;
        let (page_buf, values_bytes) = build_int64_page(&values, None);
        let header = make_header(n);
        let descriptor = make_int64_descriptor();

        group.throughput(Throughput::Elements(n as u64));

        // Baseline: materialize into ColumnChunkBuffers, then filter
        let ba = BenchAllocator::new();
        let bufs: &'static mut ColumnChunkBuffers =
            Box::leak(Box::new(ColumnChunkBuffers::new(ba.allocator.clone())));
        let page_buf_ref: &'static [u8] = Box::leak(page_buf.clone().into_boxed_slice());
        let header_ref: &'static DataPageHeader = Box::leak(Box::new(header.clone()));
        let descriptor_ref: &'static Descriptor = Box::leak(Box::new(descriptor.clone()));

        group.bench_with_input(
            BenchmarkId::new("materialize_then_filter", n),
            &n,
            |b, &n| {
                let col_info = QdbMetaCol {
                    column_type: ColumnType::new(ColumnTypeTag::Long, 0),
                    column_top: 0,
                    format: None,
                    ascii: None,
                };
                b.iter(|| {
                    bufs.data_vec.clear();
                    bufs.aux_vec.clear();
                    let page_ref = page::DataPage {
                        buffer: page_buf_ref,
                        header: header_ref,
                        descriptor: descriptor_ref,
                    };
                    decode_page(
                        black_box(&page_ref),
                        None,
                        bufs,
                        col_info,
                        0,
                        n,
                    )
                    .unwrap();

                    // Filter from materialized buffer
                    let data = &bufs.data_vec;
                    let count = data.len() / 8;
                    let mut matching = Vec::new();
                    for i in 0..count {
                        let v = i64::from_le_bytes(
                            data[i * 8..(i + 1) * 8].try_into().unwrap(),
                        );
                        if v > threshold {
                            matching.push(i);
                        }
                    }
                    black_box(matching.len());
                })
            },
        );

        // Vectorized: PageDecoder<FilterSink> — no materialization
        let values_bytes_ref: &'static [u8] =
            Box::leak(values_bytes.clone().into_boxed_slice());
        let page_buf_ref2: &'static [u8] = Box::leak(page_buf.clone().into_boxed_slice());
        let header_ref2: &'static DataPageHeader = Box::leak(Box::new(header.clone()));
        let descriptor_ref2: &'static Descriptor = Box::leak(Box::new(descriptor.clone()));

        group.bench_with_input(
            BenchmarkId::new("vectorized_filter", n),
            &n,
            |b, &n| {
                b.iter(|| {
                    let page_ref = page::DataPage {
                        buffer: page_buf_ref2,
                        header: header_ref2,
                        descriptor: descriptor_ref2,
                    };
                    let iter = DefLevelBatchIter::new(
                        black_box(&page_ref),
                        0,
                        n,
                    )
                    .unwrap();
                    let mut sink = GtInt64FilterSink::new(threshold);
                    let pushable = PlainPrimitiveDecoder::<i64, i64>::new_for_sink(
                        values_bytes_ref, &mut sink, i64::MIN,
                    );
                    let mut wrapper = PrimitiveSinkWrapper(sink);
                    let mut decoder = BoundPageDecoder::new(iter, pushable);
                    decoder.decode_all(&mut wrapper).unwrap();
                    black_box(wrapper.0.inner.matching_rows.len());
                })
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_sum_double, bench_filter_int64);
criterion_main!(benches);
