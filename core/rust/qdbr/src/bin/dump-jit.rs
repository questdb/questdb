//! Dump IR and disassembled machine code for page-kernel JIT pipelines.
//!
//! Usage:
//!   cargo run --bin dump-jit [spec...]
//!
//! Specs (optional, defaults to a curated set):
//!   mat_i64          - materialize i64, no filter
//!   mat_i64_gt       - materialize i64, filter > threshold
//!   agg_i64_sum      - aggregate sum i64, no filter
//!   agg_f64_min_gt   - aggregate min f64, filter > threshold
//!
//! Example:
//!   cargo run --bin dump-jit
//!   cargo run --bin dump-jit -- mat_i64 agg_i64_sum

use questdbr::parquet_jit::encode::multi_page;
use questdbr::parquet_jit::ir_builder::IrBuilder;
use questdbr::parquet_jit::multi::*;
use questdbr::parquet_jit::*;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();

    let specs: Vec<(&str, MultiPipelineSpec)> = if args.is_empty() {
        default_specs()
    } else {
        args.iter()
            .filter_map(|name| {
                spec_by_name(name).map(|s| (name.as_str(), s))
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|(n, s)| (leak_str(n), s))
            .collect()
    };

    if specs.is_empty() {
        eprintln!("No valid specs. Available: mat_i64, mat_i64_gt, agg_i64_sum, agg_f64_min_gt");
        std::process::exit(1);
    }

    for (name, spec) in &specs {
        dump_spec(name, spec);
    }
}

fn leak_str(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

fn default_specs() -> Vec<(&'static str, MultiPipelineSpec)> {
    vec![
        ("mat_i64", spec_by_name("mat_i64").unwrap()),
        ("mat_i64_gt", spec_by_name("mat_i64_gt").unwrap()),
        ("agg_i64_sum", spec_by_name("agg_i64_sum").unwrap()),
        ("agg_f64_min_gt", spec_by_name("agg_f64_min_gt").unwrap()),
    ]
}

fn spec_by_name(name: &str) -> Option<MultiPipelineSpec> {
    match name {
        "mat_i64" => Some(MultiPipelineSpec {
            columns: vec![ColumnSpec {
                encoding: JitEncoding::Plain,
                physical_type: JitPhysicalType::Int64,
                role: ColumnRole::Materialize,
                filter_op: JitFilterOp::None,
                aggregate_op: JitAggregateOp::Count,
            }],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        }),
        "mat_i64_gt" => Some(MultiPipelineSpec {
            columns: vec![
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Int64,
                    role: ColumnRole::Filter,
                    filter_op: JitFilterOp::Gt,
                    aggregate_op: JitAggregateOp::Count,
                },
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Int64,
                    role: ColumnRole::Materialize,
                    filter_op: JitFilterOp::None,
                    aggregate_op: JitAggregateOp::Count,
                },
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        }),
        "agg_i64_sum" => Some(MultiPipelineSpec {
            columns: vec![ColumnSpec {
                encoding: JitEncoding::Plain,
                physical_type: JitPhysicalType::Int64,
                role: ColumnRole::Aggregate,
                filter_op: JitFilterOp::None,
                aggregate_op: JitAggregateOp::Sum,
            }],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        }),
        "agg_f64_min_gt" => Some(MultiPipelineSpec {
            columns: vec![ColumnSpec {
                encoding: JitEncoding::Plain,
                physical_type: JitPhysicalType::Double,
                role: ColumnRole::FilterAggregate,
                filter_op: JitFilterOp::Gt,
                aggregate_op: JitAggregateOp::Min,
            }],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        }),
        _ => None,
    }
}

fn dump_spec(name: &str, spec: &MultiPipelineSpec) {
    println!("================================================================");
    println!("  {name}");
    println!("================================================================");
    println!();

    // Print spec summary.
    println!("Spec: output_mode={:?}, filter_combine={:?}, columns={{",
        spec.output_mode, spec.filter_combine);
    for (i, col) in spec.columns.iter().enumerate() {
        println!("  [{i}] {:?} {:?} | filter={:?} agg={:?}",
            col.physical_type, col.role, col.filter_op, col.aggregate_op);
    }
    println!("}}");
    println!();

    // Generate IR.
    let mut builder = IrBuilder::new();
    if let Err(e) = multi_page::emit_ir(&mut builder, spec) {
        println!("IR generation failed: {e}");
        println!();
        return;
    }
    let ir = builder.finish();

    println!("--- IR ({} blocks, {} values) ---", ir.blocks.len(), ir.val_types.len());
    println!();
    print!("{}", ir.display());

    // Compile to machine code.
    let kernel = match generate_page_kernel(spec) {
        Ok(k) => k,
        Err(e) => {
            println!("Compilation failed: {e}");
            println!();
            return;
        }
    };

    println!("--- Machine code ({} bytes) ---", kernel.code_size());
    println!();

    let fn_ptr = kernel.fn_ptr() as *const u8;
    let code: Vec<u8> = (0..kernel.code_size())
        .map(|i| unsafe { *fn_ptr.add(i) })
        .collect();
    let base_addr = fn_ptr as u64;

    disassemble_aarch64(&code, base_addr);
    println!();
}

fn disassemble_aarch64(code: &[u8], base_addr: u64) {
    for (i, chunk) in code.chunks(4).enumerate() {
        if chunk.len() < 4 {
            break;
        }
        let word = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let addr = base_addr + (i * 4) as u64;
        match bad64::decode(word, addr) {
            Ok(insn) => println!("  {addr:#010x}:  {word:08x}    {insn}"),
            Err(_) => println!("  {addr:#010x}:  {word:08x}    .word {word:#010x}"),
        }
    }
}
