use criterion::{black_box, criterion_group, criterion_main, Criterion};
use questdbr::parquet_jit::*;

/// 100k i64 values from a 16-entry RLE dictionary, GT filter, aggregate SUM.
///
/// Compares three approaches:
///   1. **dict_filter JIT**: bitmap-check on dict index → decode only matching values
///   2. **plain JIT (post-decode filter)**: decode all → filter → aggregate
///   3. **Rust baseline**: decode all → filter → aggregate in compiled Rust
fn bench_dict_filter_i64_100k(c: &mut Criterion) {
    let n = 100_000u32;
    let dict: [i64; 16] = [
        5, 10, 15, 20, 30, 40, 50, 60,
        70, 80, 90, 100, 110, 120, 130, 140,
    ];

    // Pre-decoded flat index array (u32 per row, cycling 0..15).
    let indices: Vec<u8> = (0..n)
        .flat_map(|i| (i as u32 % 16).to_le_bytes())
        .collect();
    // Pre-decoded flat value array (for the plain/Rust baselines).
    let values_i64: Vec<i64> = (0..n as usize).map(|i| dict[i % 16]).collect();
    let values_bytes: Vec<u8> = values_i64.iter().flat_map(|v| v.to_le_bytes()).collect();

    let dict_bytes: Vec<u8> = dict.iter().flat_map(|v| v.to_le_bytes()).collect();

    // GT threshold=60 → entries 70,80,90,100,110,120,130,140 pass (8/16 = 50%).
    let threshold_50: i64 = 60;
    let match_bm_50: Vec<u8> = dict.iter().map(|&v| u8::from(v > threshold_50)).collect();

    // GT threshold=120 → entries 130,140 pass (2/16 = 12.5%).
    let threshold_12: i64 = 120;
    let match_bm_12: Vec<u8> = dict.iter().map(|&v| u8::from(v > threshold_12)).collect();

    // ================================================================
    //  dict_filter JIT  (bitmap check before decode)
    // ================================================================
    let pipe_dict = generate_pipeline(&PipelineSpec {
        encoding: JitEncoding::RleDictionary,
        physical_type: JitPhysicalType::Int64,
        filter_op: JitFilterOp::Gt,
        aggregate_op: JitAggregateOp::Sum,
        output_mode: JitOutputMode::Aggregate,
    }).unwrap();

    c.bench_function("dict_jit_sum_i64_100k_gt_50pct", |b| {
        b.iter(|| {
            let mut r = PipelineResult::new();
            unsafe {
                (pipe_dict.fn_ptr())(
                    black_box(indices.as_ptr()),
                    std::ptr::null(),
                    black_box(n),
                    black_box(match_bm_50.as_ptr() as i64),
                    black_box(dict_bytes.as_ptr() as i64),
                    &mut r as *mut _,
                );
            }
            black_box(r.value_i64)
        });
    });

    c.bench_function("dict_jit_sum_i64_100k_gt_12pct", |b| {
        b.iter(|| {
            let mut r = PipelineResult::new();
            unsafe {
                (pipe_dict.fn_ptr())(
                    black_box(indices.as_ptr()),
                    std::ptr::null(),
                    black_box(n),
                    black_box(match_bm_12.as_ptr() as i64),
                    black_box(dict_bytes.as_ptr() as i64),
                    &mut r as *mut _,
                );
            }
            black_box(r.value_i64)
        });
    });

    // ================================================================
    //  plain JIT  (decode all, then filter per-value)
    // ================================================================
    let pipe_plain = generate_pipeline(&PipelineSpec {
        encoding: JitEncoding::Plain,
        physical_type: JitPhysicalType::Int64,
        filter_op: JitFilterOp::Gt,
        aggregate_op: JitAggregateOp::Sum,
        output_mode: JitOutputMode::Aggregate,
    }).unwrap();

    c.bench_function("plain_jit_sum_i64_100k_gt_50pct", |b| {
        b.iter(|| {
            let mut r = PipelineResult::new();
            unsafe {
                execute_pipeline(
                    &pipe_plain,
                    black_box(values_bytes.as_ptr()),
                    std::ptr::null(),
                    black_box(n),
                    black_box(threshold_50),
                    0,
                    &mut r,
                );
            }
            black_box(r.value_i64)
        });
    });

    c.bench_function("plain_jit_sum_i64_100k_gt_12pct", |b| {
        b.iter(|| {
            let mut r = PipelineResult::new();
            unsafe {
                execute_pipeline(
                    &pipe_plain,
                    black_box(values_bytes.as_ptr()),
                    std::ptr::null(),
                    black_box(n),
                    black_box(threshold_12),
                    0,
                    &mut r,
                );
            }
            black_box(r.value_i64)
        });
    });

    // ================================================================
    //  Rust baseline
    // ================================================================
    c.bench_function("rust_sum_i64_100k_gt_50pct", |b| {
        b.iter(|| {
            let s: i64 = black_box(&values_i64).iter()
                .filter(|&&v| v > threshold_50).sum();
            black_box(s)
        });
    });

    c.bench_function("rust_sum_i64_100k_gt_12pct", |b| {
        b.iter(|| {
            let s: i64 = black_box(&values_i64).iter()
                .filter(|&&v| v > threshold_12).sum();
            black_box(s)
        });
    });
}

fn bench_codegen_time(c: &mut Criterion) {
    let specs = [
        ("plain_i64_gt_sum", PipelineSpec {
            encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::Gt, aggregate_op: JitAggregateOp::Sum,
            output_mode: JitOutputMode::Aggregate,
        }),
        ("plain_f64_between_min", PipelineSpec {
            encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Double,
            filter_op: JitFilterOp::Between, aggregate_op: JitAggregateOp::Min,
            output_mode: JitOutputMode::Aggregate,
        }),
        ("dict_i64_gt_sum", PipelineSpec {
            encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::Gt, aggregate_op: JitAggregateOp::Sum,
            output_mode: JitOutputMode::Aggregate,
        }),
        ("plain_i64_gt_materialize", PipelineSpec {
            encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::Gt, aggregate_op: JitAggregateOp::Count,
            output_mode: JitOutputMode::Materialize,
        }),
        ("plain_i128_eq_max", PipelineSpec {
            encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int128,
            filter_op: JitFilterOp::Eq, aggregate_op: JitAggregateOp::Max,
            output_mode: JitOutputMode::Aggregate,
        }),
        ("plain_i256_none_count", PipelineSpec {
            encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int256,
            filter_op: JitFilterOp::None, aggregate_op: JitAggregateOp::Count,
            output_mode: JitOutputMode::Aggregate,
        }),
    ];

    for (name, spec) in &specs {
        c.bench_function(&format!("codegen_{name}"), |b| {
            b.iter(|| {
                let p = generate_pipeline(black_box(spec)).unwrap();
                black_box(p.fn_ptr());
            });
        });
    }
}

/// 100k i64 rows with ~20% nulls, materialize mode.
///
/// Compares three approaches:
///   1. **page_kernel JIT**: fused def_levels iteration + JIT materialize (process_page_jit)
///   2. **flat-array JIT**: pre-decoded flat array with null bitmap, JIT materialize
///   3. **Rust baseline**: compiled Rust loop with null check + output copy
fn bench_materialize_i64_100k_nulls(c: &mut Criterion) {
    use questdbr::parquet_jit::multi::*;
    use questdbr::parquet_read::jit_decode::*;
    use parquet2::encoding::hybrid_rle::encode_bool;

    let n = 100_000usize;

    // ~20% null pattern: every 5th row is null.
    let is_non_null: Vec<bool> = (0..n).map(|i| i % 5 != 0).collect();
    let null_count = is_non_null.iter().filter(|&&nn| !nn).count();
    let _non_null_count = n - null_count;

    // Source values (all rows, including positions that are null).
    let all_values: Vec<i64> = (0..n as i64).map(|i| i * 7 + 13).collect();

    // Values buffer for parquet page: only non-null values.
    let page_values: Vec<u8> = all_values.iter().zip(&is_non_null)
        .filter(|(_, &nn)| nn)
        .flat_map(|(&v, _)| v.to_le_bytes())
        .collect();

    // Hybrid-RLE encoded def_levels.
    let mut def_levels = Vec::new();
    encode_bool(&mut def_levels, is_non_null.iter().copied(), n).unwrap();

    // Flat-array values (all rows, nulls have garbage/zero).
    let flat_values: Vec<u8> = all_values.iter().flat_map(|v| v.to_le_bytes()).collect();

    // Null bitmap (1 byte per row: 1=non-null, 0=null).
    let null_bitmap: Vec<u8> = is_non_null.iter().map(|&nn| nn as u8).collect();

    // Output buffers.
    let mut output_page = vec![0u8; n * 8];
    let mut output_flat = vec![0u8; n * 8];
    let mut output_rust = vec![0i64; n];

    // ================================================================
    //  page_kernel JIT (fused def_levels + materialize)
    // ================================================================
    let spec = MultiPipelineSpec {
        columns: vec![ColumnSpec {
            encoding: JitEncoding::Plain,
            physical_type: JitPhysicalType::Int64,
            role: ColumnRole::Materialize,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Count,
        }],
        filter_combine: FilterCombine::And,
        output_mode: JitOutputMode::Materialize,
    };
    let page_kernel = generate_page_kernel(&spec).unwrap();

    c.bench_function("page_kernel_mat_i64_100k_20pct_null", |b| {
        b.iter(|| {
            let mut result = PipelineResult::new();
            let col = JitColumnPage {
                values_data: black_box(&page_values),
                def_levels: black_box(&def_levels),
                num_values: n,
                null_count,
                filter_lo: output_page.as_mut_ptr() as i64,
                filter_hi: 0,
                physical_type: JitPhysicalType::Int64,
            };
            let mut cursors = MaterializeCursors {
                cursors: [std::ptr::null_mut(); MAX_PIPELINE_COLUMNS],
                count: 0,
                kernel_count_before: 0,
            };
            cursors.cursors[0] = output_page.as_mut_ptr();
            unsafe {
                process_page_jit(
                    black_box(&page_kernel), &spec, &[col],
                    &mut result, Some(&mut cursors),
                );
            }
            black_box(cursors.count)
        });
    });

    // ================================================================
    //  flat-array JIT (pre-decoded + null bitmap, single-column)
    // ================================================================
    let flat_pipe = generate_pipeline(&PipelineSpec {
        encoding: JitEncoding::Plain,
        physical_type: JitPhysicalType::Int64,
        filter_op: JitFilterOp::None,
        aggregate_op: JitAggregateOp::Count,
        output_mode: JitOutputMode::Materialize,
    }).unwrap();

    c.bench_function("flat_jit_mat_i64_100k_20pct_null", |b| {
        b.iter(|| {
            let mut count: u64 = 0;
            unsafe {
                (flat_pipe.fn_ptr())(
                    black_box(flat_values.as_ptr()),
                    black_box(null_bitmap.as_ptr()),
                    black_box(n as u32),
                    0,
                    &mut count as *mut u64 as i64,
                    output_flat.as_mut_ptr() as *mut PipelineResult,
                );
            }
            black_box(count)
        });
    });

    // ================================================================
    //  Rust baseline (compiled loop)
    // ================================================================
    c.bench_function("rust_mat_i64_100k_20pct_null", |b| {
        b.iter(|| {
            let vals = black_box(&all_values);
            let nn = black_box(&is_non_null);
            let out = black_box(&mut output_rust);
            let mut cursor = 0usize;
            for i in 0..n {
                if nn[i] {
                    out[cursor] = vals[i];
                } else {
                    out[cursor] = i64::MIN; // sentinel
                }
                cursor += 1;
            }
            black_box(cursor)
        });
    });

    // ================================================================
    //  page_kernel JIT — no nulls baseline (to measure null overhead)
    // ================================================================
    let all_nn_values: Vec<u8> = all_values.iter().flat_map(|v| v.to_le_bytes()).collect();

    c.bench_function("page_kernel_mat_i64_100k_no_null", |b| {
        b.iter(|| {
            let mut result = PipelineResult::new();
            let col = JitColumnPage {
                values_data: black_box(&all_nn_values),
                def_levels: &[],
                num_values: n,
                null_count: 0,
                filter_lo: output_page.as_mut_ptr() as i64,
                filter_hi: 0,
                physical_type: JitPhysicalType::Int64,
            };
            let mut cursors = MaterializeCursors {
                cursors: [std::ptr::null_mut(); MAX_PIPELINE_COLUMNS],
                count: 0,
                kernel_count_before: 0,
            };
            cursors.cursors[0] = output_page.as_mut_ptr();
            unsafe {
                process_page_jit(
                    black_box(&page_kernel), &spec, &[col],
                    &mut result, Some(&mut cursors),
                );
            }
            black_box(cursors.count)
        });
    });
}

/// 100k i64 rows with ~20% nulls, aggregate SUM mode.
///
/// Compares page_kernel vs flat-array JIT for aggregation.
/// For aggregate mode, the page_kernel calls the kernel once per RLE run
/// (much fewer calls than materialize since we don't need row-order output).
fn bench_aggregate_i64_100k_nulls(c: &mut Criterion) {
    use questdbr::parquet_jit::multi::*;
    use questdbr::parquet_read::jit_decode::*;
    use parquet2::encoding::hybrid_rle::encode_bool;

    let n = 100_000usize;
    let is_non_null: Vec<bool> = (0..n).map(|i| i % 5 != 0).collect();
    let null_count = is_non_null.iter().filter(|&&nn| !nn).count();

    let all_values: Vec<i64> = (0..n as i64).map(|i| i * 7 + 13).collect();
    let page_values: Vec<u8> = all_values.iter().zip(&is_non_null)
        .filter(|(_, &nn)| nn)
        .flat_map(|(&v, _)| v.to_le_bytes())
        .collect();
    let flat_values: Vec<u8> = all_values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let null_bitmap: Vec<u8> = is_non_null.iter().map(|&nn| nn as u8).collect();
    let mut def_levels = Vec::new();
    encode_bool(&mut def_levels, is_non_null.iter().copied(), n).unwrap();

    // page_kernel aggregate
    let spec_agg = MultiPipelineSpec {
        columns: vec![ColumnSpec {
            encoding: JitEncoding::Plain,
            physical_type: JitPhysicalType::Int64,
            role: ColumnRole::Aggregate,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Sum,
        }],
        filter_combine: FilterCombine::And,
        output_mode: JitOutputMode::Aggregate,
    };
    let page_kernel_agg = generate_page_kernel(&spec_agg).unwrap();

    c.bench_function("page_kernel_agg_i64_100k_20pct_null", |b| {
        b.iter(|| {
            let mut result = PipelineResult::new();
            let col = JitColumnPage {
                values_data: black_box(&page_values),
                def_levels: black_box(&def_levels),
                num_values: n,
                null_count,
                filter_lo: 0,
                filter_hi: 0,
                physical_type: JitPhysicalType::Int64,
            };
            unsafe {
                process_page_jit(
                    black_box(&page_kernel_agg), &spec_agg, &[col],
                    &mut result, None,
                );
            }
            black_box(result.value_i64)
        });
    });

    // flat-array JIT aggregate
    let flat_agg = generate_pipeline(&PipelineSpec {
        encoding: JitEncoding::Plain,
        physical_type: JitPhysicalType::Int64,
        filter_op: JitFilterOp::None,
        aggregate_op: JitAggregateOp::Sum,
        output_mode: JitOutputMode::Aggregate,
    }).unwrap();

    c.bench_function("flat_jit_agg_i64_100k_20pct_null", |b| {
        b.iter(|| {
            let mut r = PipelineResult::new();
            unsafe {
                execute_pipeline(
                    &flat_agg,
                    black_box(flat_values.as_ptr()),
                    black_box(null_bitmap.as_ptr()),
                    black_box(n as u32),
                    0, 0,
                    &mut r,
                );
            }
            black_box(r.value_i64)
        });
    });

    // Rust baseline
    c.bench_function("rust_agg_i64_100k_20pct_null", |b| {
        b.iter(|| {
            let vals = black_box(&all_values);
            let nn = black_box(&is_non_null);
            let mut sum = 0i64;
            for i in 0..n {
                if nn[i] {
                    sum += vals[i];
                }
            }
            black_box(sum)
        });
    });
}

criterion_group!(
    benches,
    bench_dict_filter_i64_100k,
    bench_codegen_time,
    bench_materialize_i64_100k_nulls,
    bench_aggregate_i64_100k_nulls,
);
criterion_main!(benches);
