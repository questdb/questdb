use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use parquet2::compression::CompressionOptions;
use parquet2::write::Version;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::bench::{
    create_row_group_from_partitions, to_compressions, to_encodings, to_parquet_schema, Column,
    Partition, WriteOptions, DEFAULT_BLOOM_FILTER_FPP,
};
use std::collections::HashSet;
use std::hint::black_box;
use std::ptr::null;

const ROW_COUNT: usize = 100_000;

fn write_options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        version: Version::V1,
        compression: CompressionOptions::Uncompressed,
        row_group_size: None,
        data_page_size: None,
        raw_array_encoding: false,
        min_compression_ratio: 0.0,
        bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
    }
}

fn make_column<T>(name: &'static str, tag: ColumnTypeTag, data: &[T]) -> Column {
    Column::from_raw_data(
        0,
        name,
        ColumnType::new(tag, 0).code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        std::mem::size_of_val(data),
        null(),
        0,
        null(),
        0,
        false,
        false,
        0,
    )
    .expect("Column::from_raw_data")
}

/// Build a single partition with mixed column types: Int, Long, Double, Long.
/// Uses owned Vecs that the caller must keep alive for the partition's lifetime.
struct PartitionData {
    int_col: Vec<i32>,
    long_col: Vec<i64>,
    double_col: Vec<f64>,
    ts_col: Vec<i64>,
}

impl PartitionData {
    fn new(rows: usize, base: i64) -> Self {
        let int_col: Vec<i32> = (0..rows)
            .map(|i| (i as i32).wrapping_add(base as i32))
            .collect();
        let long_col: Vec<i64> = (0..rows).map(|i| (i as i64) + base).collect();
        let double_col: Vec<f64> = (0..rows).map(|i| i as f64 * 0.5 + base as f64).collect();
        let ts_col: Vec<i64> = (0..rows).map(|i| (i as i64) + base * 1_000_000).collect();
        Self {
            int_col,
            long_col,
            double_col,
            ts_col,
        }
    }

    fn to_partition(&self) -> Partition {
        Partition {
            table: "bench".to_string(),
            columns: vec![
                make_column("int_col", ColumnTypeTag::Int, &self.int_col),
                make_column("long_col", ColumnTypeTag::Long, &self.long_col),
                make_column("double_col", ColumnTypeTag::Double, &self.double_col),
                make_column("ts_col", ColumnTypeTag::Timestamp, &self.ts_col),
            ],
        }
    }

    fn rows(&self) -> usize {
        self.int_col.len()
    }
}

fn bench_row_group(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_row_group");
    let options = write_options();

    // Single-partition row group (100K rows, mixed types).
    {
        let data = PartitionData::new(ROW_COUNT, 0);
        let partition = data.to_partition();
        let (schema, _meta) = to_parquet_schema(&partition, false, -1).expect("schema");
        let encodings = to_encodings(&partition);
        let compressions = to_compressions(&partition);
        let bloom_cols = HashSet::new();

        group.throughput(Throughput::Elements(ROW_COUNT as u64));
        group.bench_function("mixed_single_partition", |b| {
            b.iter(|| {
                let (row_group, _bloom) = create_row_group_from_partitions(
                    &[&partition],
                    0,
                    data.rows(),
                    schema.fields(),
                    &encodings,
                    options,
                    &compressions,
                    &bloom_cols,
                    false,
                )
                .expect("row group");
                // Drain the iter to consume all column chunks.
                for col in row_group {
                    let _col = black_box(col.expect("col"));
                }
            });
        });
    }

    // Four-partition row group (4 × 25K rows, mixed types).
    {
        let parts: Vec<PartitionData> = (0..4)
            .map(|i| PartitionData::new(ROW_COUNT / 4, i * 25_000))
            .collect();
        let partitions: Vec<Partition> = parts.iter().map(|p| p.to_partition()).collect();
        let partition_refs: Vec<&Partition> = partitions.iter().collect();
        let (schema, _meta) = to_parquet_schema(&partitions[0], false, -1).expect("schema");
        let encodings = to_encodings(&partitions[0]);
        let compressions = to_compressions(&partitions[0]);
        let bloom_cols = HashSet::new();
        let last_end = parts[parts.len() - 1].rows();

        group.throughput(Throughput::Elements(ROW_COUNT as u64));
        group.bench_function("mixed_4_partitions", |b| {
            b.iter(|| {
                let (row_group, _bloom) = create_row_group_from_partitions(
                    &partition_refs,
                    0,
                    last_end,
                    schema.fields(),
                    &encodings,
                    options,
                    &compressions,
                    &bloom_cols,
                    false,
                )
                .expect("row group");
                for col in row_group {
                    let _col = black_box(col.expect("col"));
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_row_group);
criterion_main!(benches);
