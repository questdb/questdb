//! CLI tool to generate a `_pm` metadata file from a parquet file.
//!
//! Usage:
//!   `pm_generate <parquet-path> [output-path]`
//!
//! If `output-path` is omitted, writes `_pm` next to the parquet file.
//! If `<parquet-path>` is a directory, looks for `data.parquet` inside it.

use parquet2::read::read_metadata_with_size;
use questdbr::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use questdbr::parquet_metadata::convert::convert_from_parquet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::{env, fs, process};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        eprintln!("Usage: {} <parquet-path> [output-path]", args[0]);
        eprintln!();
        eprintln!("  <parquet-path>  path to a .parquet file or directory containing data.parquet");
        eprintln!("  [output-path]   where to write the _pm file (default: sibling of parquet)");
        process::exit(1);
    }

    let mut parquet_path = PathBuf::from(&args[1]);
    if parquet_path.is_dir() {
        parquet_path.push("data.parquet");
    }

    if !parquet_path.exists() {
        eprintln!("Parquet file not found: {}", parquet_path.display());
        process::exit(1);
    }

    let output_path = if args.len() == 3 {
        PathBuf::from(&args[2])
    } else {
        parquet_path
            .parent()
            .map(|dir| dir.join("_pm"))
            .unwrap_or_else(|| PathBuf::from("_pm"))
    };

    match generate(&parquet_path, &output_path) {
        Ok(size) => {
            println!(
                "Generated {} ({} bytes) from {}",
                output_path.display(),
                size,
                parquet_path.display()
            );
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}

fn generate(
    parquet_path: &PathBuf,
    output_path: &PathBuf,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut parquet_file = File::open(parquet_path)?;
    let parquet_file_size = fs::metadata(parquet_path)?.len();

    let metadata = read_metadata_with_size(&mut parquet_file, parquet_file_size)
        .map_err(|e| format!("failed to read parquet metadata: {}", e))?;

    let qdb_meta = extract_qdb_meta(&metadata);

    let footer_length = read_parquet_footer_length(&mut parquet_file, parquet_file_size)?;
    let parquet_footer_offset = parquet_file_size
        .checked_sub(8 + footer_length as u64)
        .ok_or("parquet footer length exceeds file size")?;

    let (pm_bytes, _footer_offset) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        footer_length,
        None,
    )
    .map_err(|e| format!("failed to convert: {}", e))?;

    let parquet_meta_file_size = pm_bytes.len() as u64;
    let mut out = File::create(output_path)?;
    out.write_all(&pm_bytes)?;

    Ok(parquet_meta_file_size)
}

fn extract_qdb_meta(metadata: &parquet2::metadata::FileMetaData) -> Option<QdbMeta> {
    let kvs = metadata.key_value_metadata.as_ref()?;
    let kv = kvs.iter().find(|kv| kv.key == QDB_META_KEY)?;
    let json = kv.value.as_deref()?;
    QdbMeta::deserialize(json).ok()
}

fn read_parquet_footer_length(
    file: &mut File,
    file_size: u64,
) -> Result<u32, Box<dyn std::error::Error>> {
    file.seek(SeekFrom::Start(file_size - 8))?;
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}
