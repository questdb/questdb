//! CLI tool to decode and print the contents of a `_pm` parquet metadata file.
//!
//! Usage:
//!   `pm_inspect <path>`                              — dump _pm contents
//!   `pm_inspect --check <path>`                      — compare against sibling `data.parquet`
//!   `pm_inspect --check <pm-path> <parquet-path>`    — compare against explicit parquet file
//!
//! `<path>` can be a `_pm` file directly or a directory containing one.

use parquet2::metadata::FileMetaData;
use parquet2::read::read_metadata;
use qdb_core::col_type::ColumnTypeTag;
use questdbr::parquet_metadata::types::{
    Codec, ColumnFlags, EncodingMask, FieldRepetition, StatFlags,
};
use questdbr::parquet_metadata::ParquetMetaReader;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::{env, fs, process};

enum Mode {
    Dump,
    Check { parquet_path: Option<PathBuf> },
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let (mode, path_arg) = match args.len() {
        2 => (Mode::Dump, &args[1]),
        3 if args[1] == "--check" => (Mode::Check { parquet_path: None }, &args[2]),
        4 if args[1] == "--check" => (
            Mode::Check { parquet_path: Some(PathBuf::from(&args[3])) },
            &args[2],
        ),
        _ => {
            eprintln!(
                "Usage: {} <path>\n       {} --check <path> [parquet-path]",
                args[0], args[0]
            );
            process::exit(1);
        }
    };

    let mut path = PathBuf::from(path_arg);
    if path.is_dir() {
        path.push("_pm");
    }

    if !path.exists() {
        eprintln!("File not found: {}", path.display());
        process::exit(1);
    }

    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Failed to read {}: {}", path.display(), e);
            process::exit(1);
        }
    };

    let file_size = bytes.len() as u64;
    let reader = match ParquetMetaReader::from_file_size(&bytes, file_size) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Failed to parse _pm file: {}", e);
            process::exit(1);
        }
    };

    match mode {
        Mode::Dump => run_dump(&path, file_size, &reader),
        Mode::Check { parquet_path } => run_check(&path, &reader, parquet_path.as_deref()),
    }
}

// ── Dump mode ───────────────────────────────────────────────────────────

fn run_dump(path: &Path, file_size: u64, reader: &ParquetMetaReader) {
    println!("File: {}", path.display());
    println!("Size: {} bytes", file_size);
    println!();

    match reader.verify_checksum() {
        Ok(()) => println!("Checksum: OK"),
        Err(e) => println!("Checksum: FAILED ({})", e),
    }
    println!();

    // Header
    println!("=== Header ===");
    println!("Columns: {}", reader.column_count());
    match reader.designated_timestamp() {
        Some(idx) => println!("Designated timestamp: column {}", idx),
        None => println!("Designated timestamp: none"),
    }
    let flags = reader.feature_flags();
    if flags.0 != 0 {
        print!("Feature flags: 0x{:016x}", flags.0);
        if flags.has_bloom_filters() {
            print!(" [BLOOM_FILTERS]");
        }
        if flags.has_bloom_filters_external() {
            print!(" [BLOOM_FILTERS_EXTERNAL]");
        }
        if flags.has_sorting_is_dts_asc() {
            print!(" [SORTING_IS_DTS_ASC]");
        }
        println!();
    }
    println!("Sorting columns: {}", reader.sorting_column_count());
    for i in 0..reader.sorting_column_count() as usize {
        if let Ok(col_idx) = reader.sorting_column(i) {
            println!("  [{}] column {}", i, col_idx);
        }
    }
    if flags.has_sorting_is_dts_asc() {
        println!("  (implied by SORTING_IS_DTS_ASC flag)");
    }
    println!();

    // Column descriptors
    println!("=== Column Descriptors ===");
    for i in 0..reader.column_count() as usize {
        print_column_descriptor(reader, i);
    }
    println!();

    // Footer
    println!("=== Footer ===");
    println!(
        "Parquet footer: offset={}, length={}",
        reader.parquet_footer_offset(),
        reader.parquet_footer_length()
    );
    println!("Row groups: {}", reader.row_group_count());
    println!("Footer offset in file: {}", reader.footer_offset());
    println!(
        "Prev parquet meta file size: {}",
        reader.prev_parquet_meta_file_size()
    );
    let footer_flags = reader.footer_feature_flags();
    if footer_flags.0 != 0 {
        println!("Footer feature flags: 0x{:016x}", footer_flags.0);
    }
    println!();

    // Row groups
    for rg_idx in 0..reader.row_group_count() as usize {
        print_row_group(reader, rg_idx);
    }
}

fn print_column_descriptor(reader: &ParquetMetaReader, i: usize) {
    let name = reader.column_name(i).unwrap_or("<error>");
    let desc = match reader.column_descriptor(i) {
        Ok(d) => d,
        Err(e) => {
            println!("  Column {}: <error: {}>", i, e);
            return;
        }
    };

    let type_name = if desc.col_type >= 0 {
        ColumnTypeTag::try_from(desc.col_type as u8)
            .map(|t| t.name())
            .unwrap_or("unknown")
    } else {
        "untyped"
    };

    let flags = ColumnFlags(desc.flags);
    let repetition = match flags.repetition() {
        Ok(FieldRepetition::Required) => "required",
        Ok(FieldRepetition::Optional) => "optional",
        Ok(FieldRepetition::Repeated) => "repeated",
        Err(_) => "?",
    };

    print!(
        "  [{}] \"{}\" type={} ({}), id={}, {}",
        i, name, desc.col_type, type_name, desc.id, repetition
    );
    if flags.is_local_key_global() {
        print!(", localKeyIsGlobal");
    }
    if flags.is_ascii() {
        print!(", ascii");
    }
    if flags.is_descending() {
        print!(", descending");
    }
    println!();
}

fn print_row_group(reader: &ParquetMetaReader, rg_idx: usize) {
    let rg = match reader.row_group(rg_idx) {
        Ok(r) => r,
        Err(e) => {
            println!("Row group {}: <error: {}>", rg_idx, e);
            return;
        }
    };

    println!("=== Row Group {} ===", rg_idx);
    println!("  Rows: {}", rg.num_rows());

    for col_idx in 0..reader.column_count() as usize {
        let chunk = match rg.column_chunk(col_idx) {
            Ok(c) => c,
            Err(e) => {
                println!("  Column {}: <error: {}>", col_idx, e);
                continue;
            }
        };

        let col_name = reader.column_name(col_idx).unwrap_or("?");
        let codec_name = Codec::try_from(chunk.codec)
            .map(|c| format!("{:?}", c))
            .unwrap_or_else(|_| format!("unknown({})", chunk.codec));

        let enc_str = format_encodings(chunk.encodings);

        println!("  Column {} (\"{}\"):", col_idx, col_name);
        println!("    Codec: {}", codec_name);
        println!("    Encodings: {}", enc_str);
        println!(
            "    Byte range: {} .. {} ({} bytes compressed)",
            chunk.byte_range_start,
            chunk.byte_range_start + chunk.total_compressed,
            chunk.total_compressed,
        );
        println!("    Values: {}", chunk.num_values);

        let sf = StatFlags(chunk.stat_flags);
        if sf.has_null_count() {
            println!("    Null count: {}", chunk.null_count);
        }
        if sf.has_distinct_count() {
            println!("    Distinct count: {}", chunk.distinct_count);
        }
        if sf.has_min_stat() {
            let inline = if sf.is_min_inlined() {
                "inline"
            } else {
                "out-of-line"
            };
            let exact = if sf.is_min_exact() { "exact" } else { "approx" };
            println!("    Min: 0x{:016x} ({}, {})", chunk.min_stat, inline, exact);
        }
        if sf.has_max_stat() {
            let inline = if sf.is_max_inlined() {
                "inline"
            } else {
                "out-of-line"
            };
            let exact = if sf.is_max_exact() { "exact" } else { "approx" };
            println!("    Max: 0x{:016x} ({}, {})", chunk.max_stat, inline, exact);
        }

        if chunk._reserved != 0 {
            println!("    _reserved (should be 0): {}", chunk._reserved);
        }
    }
    println!();
}

fn format_encodings(mask: u8) -> String {
    let enc = EncodingMask(mask);
    let mut names = Vec::new();
    if enc.has_plain() {
        names.push("PLAIN");
    }
    if enc.has_rle_dictionary() {
        names.push("RLE_DICTIONARY");
    }
    if enc.has_delta_binary_packed() {
        names.push("DELTA_BINARY_PACKED");
    }
    if enc.has_delta_length_byte_array() {
        names.push("DELTA_LENGTH_BYTE_ARRAY");
    }
    if enc.has_delta_byte_array() {
        names.push("DELTA_BYTE_ARRAY");
    }
    if enc.has_byte_stream_split() {
        names.push("BYTE_STREAM_SPLIT");
    }
    if names.is_empty() {
        "none".to_string()
    } else {
        names.join(", ")
    }
}

// ── Check mode ──────────────────────────────────────────────────────────

fn run_check(pm_path: &Path, reader: &ParquetMetaReader, explicit_parquet: Option<&Path>) {
    // Use the explicit parquet path if given, otherwise look for sibling data.parquet.
    let parquet_path = match explicit_parquet {
        Some(p) => p.to_path_buf(),
        None => pm_path
            .parent()
            .map(|dir| dir.join("data.parquet"))
            .unwrap_or_else(|| PathBuf::from("data.parquet")),
    };

    if !parquet_path.exists() {
        eprintln!(
            "Parquet file not found: {}{}",
            parquet_path.display(),
            if explicit_parquet.is_none() {
                format!(" (expected next to {})", pm_path.display())
            } else {
                String::new()
            }
        );
        process::exit(1);
    }

    let mut parquet_file = match File::open(&parquet_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open {}: {}", parquet_path.display(), e);
            process::exit(1);
        }
    };

    let parquet_meta = match read_metadata(&mut parquet_file) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("Failed to read parquet metadata: {}", e);
            process::exit(1);
        }
    };

    println!(
        "Checking _pm ({}) against parquet ({})",
        pm_path.display(),
        parquet_path.display()
    );
    println!();

    // Checksum
    match reader.verify_checksum() {
        Ok(()) => println!("[OK] _pm checksum valid"),
        Err(e) => println!("[FAIL] _pm checksum: {}", e),
    }

    let mut errors = 0u32;

    // Parquet footer offset/length
    let parquet_file_size = fs::metadata(&parquet_path).map(|m| m.len()).unwrap_or(0);
    errors += check_parquet_footer(reader, parquet_file_size);

    // Column count
    let pm_cols = reader.column_count() as usize;
    let pq_cols = parquet_meta.schema_descr.columns().len();
    if pm_cols != pq_cols {
        println!("[FAIL] column count: _pm={}, parquet={}", pm_cols, pq_cols);
        errors += 1;
    } else {
        println!("[OK] column count: {}", pm_cols);
    }

    // Row group count
    let pm_rgs = reader.row_group_count() as usize;
    let pq_rgs = parquet_meta.row_groups.len();
    if pm_rgs != pq_rgs {
        println!("[FAIL] row group count: _pm={}, parquet={}", pm_rgs, pq_rgs);
        errors += 1;
    } else {
        println!("[OK] row group count: {}", pm_rgs);
    }

    // Column names
    errors += check_column_names(reader, &parquet_meta);

    // Per-row-group checks
    let rg_count = pm_rgs.min(pq_rgs);
    for rg_idx in 0..rg_count {
        errors += check_row_group(reader, &parquet_meta, rg_idx);
    }

    println!();
    if errors == 0 {
        println!("All checks passed.");
    } else {
        println!("{} discrepancy(ies) found.", errors);
        process::exit(1);
    }
}

fn check_parquet_footer(reader: &ParquetMetaReader, parquet_file_size: u64) -> u32 {
    let pm_footer_offset = reader.parquet_footer_offset();
    let pm_footer_length = reader.parquet_footer_length() as u64;

    // Parquet layout: [data][thrift footer][footer_len:i32][PAR1:4B]
    // So footer ends at file_size - 8, and footer_len is at file_size - 8.
    // footer_offset = file_size - 8 - footer_length
    let expected_end = pm_footer_offset + pm_footer_length;
    let actual_end = parquet_file_size.saturating_sub(8);

    if expected_end != actual_end {
        println!(
            "[FAIL] parquet footer: _pm says offset={} length={} (end={}), \
             but parquet file size {} implies footer ends at {}",
            pm_footer_offset, pm_footer_length, expected_end, parquet_file_size, actual_end
        );
        1
    } else {
        println!(
            "[OK] parquet footer: offset={}, length={}",
            pm_footer_offset, pm_footer_length
        );
        0
    }
}

fn check_column_names(reader: &ParquetMetaReader, parquet_meta: &FileMetaData) -> u32 {
    let mut errors = 0;
    let count = (reader.column_count() as usize).min(parquet_meta.schema_descr.columns().len());
    for i in 0..count {
        let pm_name = reader.column_name(i).unwrap_or("<error>");
        let pq_name = &parquet_meta.schema_descr.columns()[i]
            .base_type
            .get_field_info()
            .name;
        if pm_name != pq_name {
            println!(
                "[FAIL] column {} name: _pm=\"{}\", parquet=\"{}\"",
                i, pm_name, pq_name
            );
            errors += 1;
        }
    }
    if errors == 0 && count > 0 {
        println!("[OK] column names match");
    }
    errors
}

fn check_row_group(reader: &ParquetMetaReader, parquet_meta: &FileMetaData, rg_idx: usize) -> u32 {
    let mut errors = 0;
    let pq_rg = &parquet_meta.row_groups[rg_idx];

    let pm_rg = match reader.row_group(rg_idx) {
        Ok(r) => r,
        Err(e) => {
            println!("[FAIL] row group {}: cannot read _pm block: {}", rg_idx, e);
            return 1;
        }
    };

    // num_rows
    let pm_rows = pm_rg.num_rows();
    let pq_rows = pq_rg.num_rows() as u64;
    if pm_rows != pq_rows {
        println!(
            "[FAIL] rg {} num_rows: _pm={}, parquet={}",
            rg_idx, pm_rows, pq_rows
        );
        errors += 1;
    }

    // Per-column checks
    let col_count = (reader.column_count() as usize).min(pq_rg.columns().len());
    for col_idx in 0..col_count {
        let pm_chunk = match pm_rg.column_chunk(col_idx) {
            Ok(c) => c,
            Err(e) => {
                println!(
                    "[FAIL] rg {} col {}: cannot read _pm chunk: {}",
                    rg_idx, col_idx, e
                );
                errors += 1;
                continue;
            }
        };
        let pq_col = &pq_rg.columns()[col_idx];
        let col_name = reader.column_name(col_idx).unwrap_or("?");

        let prefix = format!("rg {} col {} (\"{}\")", rg_idx, col_idx, col_name);

        // byte_range
        let (pq_start, pq_len) = pq_col.byte_range();
        if pm_chunk.byte_range_start != pq_start {
            println!(
                "[FAIL] {} byte_range_start: _pm={}, parquet={}",
                prefix, pm_chunk.byte_range_start, pq_start
            );
            errors += 1;
        }
        if pm_chunk.total_compressed != pq_len {
            println!(
                "[FAIL] {} total_compressed: _pm={}, parquet={}",
                prefix, pm_chunk.total_compressed, pq_len
            );
            errors += 1;
        }

        // codec
        let pq_codec = Codec::from(pq_col.compression());
        let pm_codec = Codec::try_from(pm_chunk.codec).unwrap_or(Codec::Uncompressed);
        if pm_codec != pq_codec {
            println!(
                "[FAIL] {} codec: _pm={:?}, parquet={:?}",
                prefix, pm_codec, pq_codec
            );
            errors += 1;
        }

        // num_values
        let pq_num_values = pq_col.num_values().max(0) as u64;
        if pm_chunk.num_values != pq_num_values {
            println!(
                "[FAIL] {} num_values: _pm={}, parquet={}",
                prefix, pm_chunk.num_values, pq_num_values
            );
            errors += 1;
        }

        // bloom filter: column chunk _reserved must be 0 (offsets moved to footer section)
        if pm_chunk._reserved != 0 {
            println!(
                "[FAIL] {} column chunk _reserved is {} (should be 0)",
                prefix, pm_chunk._reserved
            );
            errors += 1;
        }

        // null_count
        let sf = StatFlags(pm_chunk.stat_flags);
        if sf.has_null_count() {
            if let Some(Ok(stats)) = pq_col.statistics() {
                if let Some(pq_nc) = stats.null_count() {
                    if pm_chunk.null_count != pq_nc.max(0) as u64 {
                        println!(
                            "[FAIL] {} null_count: _pm={}, parquet={}",
                            prefix, pm_chunk.null_count, pq_nc
                        );
                        errors += 1;
                    }
                }
            }
        }
    }

    if errors == 0 {
        println!(
            "[OK] rg {}: {} rows, {} columns",
            rg_idx, pm_rows, col_count
        );
    }

    errors
}
