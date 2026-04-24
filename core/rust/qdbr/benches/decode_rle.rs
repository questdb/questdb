/// Benchmark for hybrid-RLE decoding with controlled bitpacked/RLE run ratios.
///
/// The existing `decode_page.rs` benchmark only exercises the bitpacked decoder
/// path because `encode_u32()` always emits 100% bitpacked runs. This benchmark
/// constructs hybrid-RLE streams with controlled ratios of bitpacked vs RLE runs
/// to measure how the run mix affects decoding throughput and encoded size.
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use parquet2::encoding::bitpacked;
use parquet2::encoding::uleb128;
use parquet2::encoding::{ceil8, Encoding};
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DictPage};
use parquet2::schema::types::{ParquetType, PrimitiveType};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::allocator::{MemTracking, QdbAllocator};
use questdbr::parquet::QdbMetaCol;
use questdbr::parquet_read::decode::decode_page;
use questdbr::parquet_read::page;
use questdbr::parquet_read::ColumnChunkBuffers;
use questdbr::parquet_write::schema::column_type_to_parquet_type;
use std::hint::black_box;
use std::sync::atomic::AtomicUsize;

const ROW_COUNT: usize = 100_000;
const RLE_PCTS: [u8; 5] = [0, 25, 50, 75, 100];
const CARDINALITIES: [usize; 4] = [10, 100, 256, 1000];
const RUN_LEN: usize = 64;

// ---------------------------------------------------------------------------
// Memory management helpers (mirrored from decode_page.rs)
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

    fn allocator(&self) -> QdbAllocator {
        self.allocator.clone()
    }
}

struct DecodeBenchState {
    _alloc: BenchAllocator,
    bufs: ColumnChunkBuffers,
}

impl DecodeBenchState {
    fn new() -> Self {
        let alloc = BenchAllocator::new();
        let bufs = ColumnChunkBuffers::new(alloc.allocator());
        Self {
            _alloc: alloc,
            bufs,
        }
    }
}

// ---------------------------------------------------------------------------
// Hybrid-RLE stream builder with controlled RLE ratio
// ---------------------------------------------------------------------------

/// Generate indices with perfect runs of `run_len` identical values.
/// Value at position `i` = `(i / run_len) % cardinality`.
fn make_indices_with_runs(row_count: usize, cardinality: usize, run_len: usize) -> Vec<u32> {
    (0..row_count)
        .map(|i| ((i / run_len) % cardinality) as u32)
        .collect()
}

/// Append a ULEB128-encoded value to `buf`.
fn write_uleb128(buf: &mut Vec<u8>, value: u64) {
    let mut container = [0u8; 10];
    let used = uleb128::encode(value, &mut container);
    buf.extend_from_slice(&container[..used]);
}

/// Append a bitpacked run header + packed data for `values`.
/// `values.len()` must be a multiple of 8.
fn write_bitpacked_run(buf: &mut Vec<u8>, values: &[u32], num_bits: usize) {
    debug_assert!(
        values.len().is_multiple_of(8),
        "bitpacked run length must be a multiple of 8, got {}",
        values.len()
    );
    let num_groups = values.len() / 8;
    // Header: (num_groups << 1) | 1
    write_uleb128(buf, ((num_groups as u64) << 1) | 1);
    // Packed data: bitpacked::encode processes in chunks of 32 values, each
    // chunk needing (32 * num_bits + 7) / 8 bytes. Allocate enough for full
    // chunks even if the last one is partial.
    let pack_chunk_size = 32;
    let bytes_per_chunk = (pack_chunk_size * num_bits).div_ceil(8);
    let full_chunks = values.len() / pack_chunk_size;
    let has_remainder = !values.len().is_multiple_of(pack_chunk_size);
    let bytes_needed = (full_chunks + has_remainder as usize) * bytes_per_chunk;
    let start = buf.len();
    buf.resize(start + bytes_needed, 0);
    bitpacked::encode::<u32>(values, num_bits, &mut buf[start..]);
}

/// Append an RLE run header + value bytes.
fn write_rle_run(buf: &mut Vec<u8>, value: u32, run_length: usize, num_bits: usize) {
    // Header: (run_length << 1) | 0
    write_uleb128(buf, (run_length as u64) << 1);
    // Value: ceil8(num_bits) bytes, little-endian
    let rle_bytes = ceil8(num_bits);
    let le = value.to_le_bytes();
    buf.extend_from_slice(&le[..rle_bytes]);
}

/// Identifies a maximal run of consecutive identical values.
#[derive(Clone, Copy)]
struct Run {
    start: usize,
    len: usize,
}

/// Encode `indices` as a hybrid-RLE stream where approximately `rle_pct`% of
/// values are encoded as RLE runs and the rest as bitpacked groups of 8.
fn encode_hybrid_rle_mixed(indices: &[u32], num_bits: usize, rle_pct: u8) -> Vec<u8> {
    let n = indices.len();
    if n == 0 {
        return Vec::new();
    }

    // 1. Find all maximal runs of identical values (length >= 8 to be useful as RLE).
    let mut runs: Vec<Run> = Vec::new();
    let mut i = 0;
    while i < n {
        let val = indices[i];
        let start = i;
        while i < n && indices[i] == val {
            i += 1;
        }
        let len = i - start;
        if len >= 8 {
            runs.push(Run { start, len });
        }
    }

    // 2. Greedily select longest runs until we reach the target RLE count.
    let rle_target = n * rle_pct as usize / 100;
    let mut selected = vec![false; runs.len()];
    let mut sorted_indices: Vec<usize> = (0..runs.len()).collect();
    sorted_indices.sort_by(|&a, &b| runs[b].len.cmp(&runs[a].len));

    let mut rle_count = 0usize;
    for &ri in &sorted_indices {
        if rle_count >= rle_target {
            break;
        }
        selected[ri] = true;
        rle_count += runs[ri].len;
    }

    // 3. Build a set of positions that belong to selected RLE runs.
    //    For efficiency, store (start, end) intervals and walk linearly.
    let mut rle_intervals: Vec<(usize, usize)> = Vec::new();
    for (ri, run) in runs.iter().enumerate() {
        if selected[ri] {
            rle_intervals.push((run.start, run.start + run.len));
        }
    }
    rle_intervals.sort_by_key(|&(s, _)| s);

    // 4. Walk through indices, emitting RLE for selected runs and bitpacked
    //    (in groups of 8) for everything else.
    let mut buf = Vec::new();
    let mut pos = 0;
    let mut interval_idx = 0;

    while pos < n {
        // Check if we're at the start of a selected RLE interval.
        if interval_idx < rle_intervals.len() && pos == rle_intervals[interval_idx].0 {
            let (start, end) = rle_intervals[interval_idx];
            // The run length must be a multiple of 8 for clean boundaries.
            // Trim to a multiple of 8; leftover goes to bitpacked.
            let run_len_aligned = ((end - start) / 8) * 8;
            if run_len_aligned >= 8 {
                write_rle_run(&mut buf, indices[start], run_len_aligned, num_bits);
                pos = start + run_len_aligned;
            }
            // Any remainder (end - pos) falls through to the bitpacked path below.
            interval_idx += 1;
            continue;
        }

        // Determine how far we can go with bitpacking before the next RLE interval.
        let next_rle_start = if interval_idx < rle_intervals.len() {
            rle_intervals[interval_idx].0
        } else {
            n
        };
        let bp_end = next_rle_start;
        let bp_len = bp_end - pos;

        if bp_len == 0 {
            continue;
        }

        // Round up to a multiple of 8 (pad with zeros if at the very end).
        let bp_len_aligned = bp_len.div_ceil(8) * 8;
        let mut chunk = Vec::with_capacity(bp_len_aligned);
        chunk.extend_from_slice(&indices[pos..pos + bp_len]);
        chunk.resize(bp_len_aligned, 0);
        write_bitpacked_run(&mut buf, &chunk, num_bits);
        pos += bp_len;
    }

    buf
}

// ---------------------------------------------------------------------------
// Dictionary + data page construction
// ---------------------------------------------------------------------------

fn dict_bit_width(max: u64) -> u8 {
    if max == 0 {
        return 1;
    }
    (64 - max.leading_zeros()) as u8
}

fn primitive_type_for(column_type: ColumnType) -> PrimitiveType {
    let raw_array_encoding = column_type.tag() == ColumnTypeTag::Array;
    let parquet_type =
        column_type_to_parquet_type(0, "col", column_type, false, raw_array_encoding)
            .expect("parquet type");
    match parquet_type {
        ParquetType::PrimitiveType(prim) => prim,
        ParquetType::GroupType { .. } => panic!("expected primitive type"),
    }
}

/// Build a DictPage + DataPage for i64 values with a controlled RLE ratio.
///
/// - `cardinality`: number of distinct dictionary entries
/// - `rle_pct`: percentage of values to encode as RLE runs
fn build_rle_ratio_pages(
    row_count: usize,
    cardinality: usize,
    rle_pct: u8,
) -> (DataPage, DictPage, usize) {
    let indices = make_indices_with_runs(row_count, cardinality, RUN_LEN);
    let num_bits = dict_bit_width(cardinality as u64 - 1);

    // Dict page: raw concatenated i64 LE bytes, one per dictionary entry.
    let mut dict_buffer = Vec::with_capacity(cardinality * 8);
    for i in 0..cardinality {
        dict_buffer.extend_from_slice(&(i as i64 * 1000).to_le_bytes());
    }

    // Data page: no nulls, so max_def_level = 0 (no definition levels needed).
    let mut data_buffer = Vec::new();

    // Bit-width byte (required by RleDictionary encoding).
    data_buffer.push(num_bits);

    // Hybrid-RLE stream with controlled ratio.
    let rle_stream = encode_hybrid_rle_mixed(&indices, num_bits as usize, rle_pct);
    let encoded_size = rle_stream.len();
    data_buffer.extend_from_slice(&rle_stream);

    let primitive_type = primitive_type_for(ColumnType::new(ColumnTypeTag::Long, 0));

    let header = DataPageHeader::V1(DataPageHeaderV1 {
        num_values: row_count as i32,
        encoding: Encoding::RleDictionary.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: None,
    });
    let data_page = DataPage::new(
        header,
        data_buffer,
        Descriptor {
            primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(row_count),
    );
    let dict_page = DictPage::new(dict_buffer, cardinality, false);
    (data_page, dict_page, encoded_size)
}

// ---------------------------------------------------------------------------
// Benchmark harness
// ---------------------------------------------------------------------------

fn decode_page_ref(
    page: &DataPage,
    dict: Option<&DictPage>,
    bufs: &mut ColumnChunkBuffers,
    col_info: QdbMetaCol,
    row_lo: usize,
    row_hi: usize,
) {
    let pg = page::DataPage {
        header: &page.header,
        buffer: &page.buffer,
        descriptor: &page.descriptor,
    };
    let dp = dict.map(|d| page::DictPage {
        buffer: &d.buffer,
        num_values: d.num_values,
        is_sorted: d.is_sorted,
    });
    decode_page(black_box(&pg), dp.as_ref(), bufs, col_info, row_lo, row_hi).expect("decode_page");
}

fn bench_decode_rle(c: &mut Criterion) {
    verify_all_encodings();
    let mut group = c.benchmark_group("decode_rle");

    for &cardinality in &CARDINALITIES {
        for &rle_pct in &RLE_PCTS {
            let (data_page, dict_page, encoded_size) =
                build_rle_ratio_pages(ROW_COUNT, cardinality, rle_pct);

            let name = format!("card_{}/rle_{}pct", cardinality, rle_pct);
            eprintln!(
                "{}: encoded_size={} bytes ({:.1} bits/value)",
                name,
                encoded_size,
                encoded_size as f64 * 8.0 / ROW_COUNT as f64,
            );

            let col_info = QdbMetaCol {
                column_type: ColumnType::new(ColumnTypeTag::Long, 0),
                column_top: 0,
                format: None,
                ascii: None,
                not_null: false,
            };

            group.throughput(Throughput::Elements(ROW_COUNT as u64));
            let state: &'static mut DecodeBenchState = Box::leak(Box::new(DecodeBenchState::new()));
            group.bench_function(name, move |b| {
                b.iter(|| {
                    state.bufs.data_vec.clear();
                    state.bufs.aux_vec.clear();
                    decode_page_ref(
                        black_box(&data_page),
                        Some(&dict_page),
                        &mut state.bufs,
                        col_info,
                        0,
                        ROW_COUNT,
                    );
                })
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_decode_rle);
criterion_main!(benches);

// ---------------------------------------------------------------------------
// Round-trip verification (called during benchmark setup)
// ---------------------------------------------------------------------------

fn verify_round_trip(indices: &[u32], num_bits: usize, rle_pct: u8) {
    use parquet2::encoding::hybrid_rle::HybridRleDecoder;

    let encoded = encode_hybrid_rle_mixed(indices, num_bits, rle_pct);
    let decoder = HybridRleDecoder::try_new(&encoded, num_bits as u32, indices.len()).unwrap();
    let decoded: Vec<u32> = decoder.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(
        decoded, indices,
        "round-trip failed for num_bits={}, rle_pct={}",
        num_bits, rle_pct
    );
}

/// Verify that encode_hybrid_rle_mixed produces correct output for all
/// benchmark configurations. Called once at startup before benchmarks run.
fn verify_all_encodings() {
    // Small dataset, low cardinality, all RLE percentages.
    let indices = make_indices_with_runs(1000, 10, 64);
    let num_bits = dict_bit_width(9) as usize;
    for &rle_pct in &RLE_PCTS {
        verify_round_trip(&indices, num_bits, rle_pct);
    }

    // High cardinality.
    let indices = make_indices_with_runs(1000, 256, 64);
    let num_bits = dict_bit_width(255) as usize;
    verify_round_trip(&indices, num_bits, 50);

    // Full-size dataset with all cardinalities and RLE percentages.
    for &cardinality in &CARDINALITIES {
        let indices = make_indices_with_runs(ROW_COUNT, cardinality, RUN_LEN);
        let num_bits = dict_bit_width(cardinality as u64 - 1) as usize;
        for &rle_pct in &RLE_PCTS {
            verify_round_trip(&indices, num_bits, rle_pct);
        }
    }

    eprintln!("All round-trip verifications passed.");
}
