//! Subcommand `meta`. This subcommand shows the parquet metadata information
use parquet2::{
    read::read_metadata,
    schema::{types::ParquetType, types::PrimitiveType},
    statistics::Statistics,
};
use std::{fs::File, io::Write, path::Path};

use crate::{Result, SEPARATOR};
use std::sync::Arc;

// Shows meta data from the file. If the `extra` flag is available, then
// extra data that the file may contain is presented
pub fn show_meta<T, W>(file_name: T, extra: bool, show_stats: bool, writer: &mut W) -> Result<()>
where
    T: AsRef<Path> + std::fmt::Display,
    W: Write,
{
    let mut file = File::open(&file_name)?;

    let metadata = read_metadata(&mut file)?;
    let empty = "".to_string();

    // This section presents the main or general information from the file
    writeln!(writer, "\t{:20}{}", "file:", file_name)?;
    writeln!(
        writer,
        "\t{:20}{}",
        "creator:",
        metadata.created_by.unwrap_or(empty)
    )?;
    writeln!(writer, "\t{:20}{}", "version:", metadata.version)?;
    writeln!(writer, "\t{:20}{}", "groups:", metadata.row_groups.len())?;

    // The extra information is presented when the option is enabled
    // and if there is extra information in the file
    if extra {
        writeln!(writer, "{}", SEPARATOR)?;
        writeln!(writer, "\tExtra data:")?;
        if let Some(key_value) = metadata.key_value_metadata {
            for val in key_value.iter() {
                let value = val.value.as_ref().map_or("", |v| v);
                writeln!(writer, "\t{}:  {}", &val.key, value)?;
            }
        }
    }

    // The schema data presents the column information from the file.
    // The first row will be the root and then all its children.
    // The data is presented in this order: Repetition, Index, Physical type,
    // Logical type and Converted type
    writeln!(writer, "{}", SEPARATOR)?;
    writeln!(writer, "\tfile schema:  {}", metadata.schema_descr.name())?;
    writeln!(writer, "{}", SEPARATOR)?;

    for field in metadata.schema_descr.fields() {
        writeln!(writer, "{}", parquet_type_str(field))?;
    }

    writeln!(writer, "{}", SEPARATOR)?;
    writeln!(writer, "\tdata information")?;
    for (i, col) in metadata.row_groups.iter().enumerate() {
        writeln!(
            writer,
            "\t{:20}{} R:{} B:{}",
            "group:",
            i + 1,
            col.num_rows(),
            col.total_byte_size()
        )?;
        writeln!(writer, "{}", SEPARATOR)?;

        for (index, c) in col.columns().iter().enumerate() {
            writeln!(
                writer,
                "{:4}: {:27}{:?} {:?} DO:{} RC:{} SZ:{}/{}/{:.2} ENC:{:?}{}",
                index,
                c.descriptor().path_in_schema.join("."),
                c.physical_type(),
                c.compression(),
                c.data_page_offset(),
                c.num_values(),
                c.compressed_size(),
                c.uncompressed_size(),
                c.uncompressed_size() as f32 / c.compressed_size() as f32,
                c.column_encoding(),
                if show_stats {
                    statistics_str(&c.statistics().transpose().unwrap())
                } else {
                    "".to_string()
                },
            )?;
        }
    }

    Ok(())
}

// String creator to print information from ParquetType
fn parquet_type_str(parquet_type: &ParquetType) -> String {
    match parquet_type {
        ParquetType::PrimitiveType(PrimitiveType {
            field_info,
            logical_type,
            converted_type,
            physical_type,
        }) => {
            format!(
                "{:27} {:?} {:?} P:{:?} L:{:?} C:{:?}",
                &field_info.name,
                field_info.repetition,
                field_info.id,
                physical_type,
                logical_type,
                converted_type,
            )
        }
        ParquetType::GroupType {
            field_info,
            logical_type,
            converted_type,
            ..
        } => {
            format!(
                "{:27} {:?} {:?} L:{:?} C:{:?}",
                ":", field_info.repetition, field_info.id, logical_type, converted_type,
            )
        }
    }
}

// Creates a string showing the column statistics.
// The max and min data are PLAIN encoded as Vec<u8>
fn statistics_str(statistics: &Option<Arc<dyn Statistics>>) -> String {
    match statistics {
        None => "".to_string(),
        Some(stats) => {
            format!("ST:{:?}", stats.as_ref())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_meta() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        show_meta(file_name, false, false, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();

        let expected = "\tfile:               data/sample.parquet
\tcreator:            parquet-cpp version 1.5.1-SNAPSHOT
\tversion:            1
\tgroups:             1
--------------------------------------------------
\tfile schema:  schema
--------------------------------------------------
ID                          Optional None P:Int64 L:None C:None
Name                        Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Sex                         Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Age                         Optional None P:Double L:None C:None
Height                      Optional None P:Double L:None C:None
Weight                      Optional None P:Double L:None C:None
Team                        Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
NOC                         Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Games                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Year                        Optional None P:Int64 L:None C:None
Season                      Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
City                        Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Sport                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Event                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Medal                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
__index_level_0__           Optional None P:Int64 L:None C:None
--------------------------------------------------
\tdata information
\tgroup:              1 R:100 B:9597
--------------------------------------------------
   0: ID                         Int64 Snappy DO:531 RC:100 SZ:694/981/1.41 ENC:[PlainDictionary, Plain, Rle]
   1: Name                       ByteArray Snappy DO:2938 RC:100 SZ:2329/2544/1.09 ENC:[PlainDictionary, Plain, Rle]
   2: Sex                        ByteArray Snappy DO:3260 RC:100 SZ:78/74/0.95 ENC:[PlainDictionary, Plain, Rle]
   3: Age                        Double Snappy DO:3488 RC:100 SZ:264/339/1.28 ENC:[PlainDictionary, Plain, Rle]
   4: Height                     Double Snappy DO:3921 RC:100 SZ:337/475/1.41 ENC:[PlainDictionary, Plain, Rle]
   5: Weight                     Double Snappy DO:4377 RC:100 SZ:357/507/1.42 ENC:[PlainDictionary, Plain, Rle]
   6: Team                       ByteArray Snappy DO:5256 RC:100 SZ:765/903/1.18 ENC:[PlainDictionary, Plain, Rle]
   7: NOC                        ByteArray Snappy DO:5779 RC:100 SZ:434/515/1.19 ENC:[PlainDictionary, Plain, Rle]
   8: Games                      ByteArray Snappy DO:6218 RC:100 SZ:389/708/1.82 ENC:[PlainDictionary, Plain, Rle]
   9: Year                       Int64 Snappy DO:6577 RC:100 SZ:276/362/1.31 ENC:[PlainDictionary, Plain, Rle]
  10: Season                     ByteArray Snappy DO:6851 RC:100 SZ:99/95/0.96 ENC:[PlainDictionary, Plain, Rle]
  11: City                       ByteArray Snappy DO:7398 RC:100 SZ:547/570/1.04 ENC:[PlainDictionary, Plain, Rle]
  12: Sport                      ByteArray Snappy DO:7977 RC:100 SZ:487/548/1.13 ENC:[PlainDictionary, Plain, Rle]
  13: Event                      ByteArray Snappy DO:9715 RC:100 SZ:1732/3370/1.95 ENC:[PlainDictionary, Plain, Rle]
  14: Medal                      ByteArray Snappy DO:10101 RC:100 SZ:110/107/0.97 ENC:[PlainDictionary, Plain, Rle]
  15: __index_level_0__          Int64 Snappy DO:10778 RC:100 SZ:699/981/1.40 ENC:[PlainDictionary, Plain, Rle]\n";

        assert_eq!(expected, string_output);
    }

    #[test]
    fn test_show_meta_stats() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        show_meta(file_name, false, true, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();

        let expected = "\tfile:               data/sample.parquet
\tcreator:            parquet-cpp version 1.5.1-SNAPSHOT
\tversion:            1
\tgroups:             1
--------------------------------------------------
\tfile schema:  schema
--------------------------------------------------
ID                          Optional None P:Int64 L:None C:None
Name                        Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Sex                         Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Age                         Optional None P:Double L:None C:None
Height                      Optional None P:Double L:None C:None
Weight                      Optional None P:Double L:None C:None
Team                        Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
NOC                         Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Games                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Year                        Optional None P:Int64 L:None C:None
Season                      Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
City                        Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Sport                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Event                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
Medal                       Optional None P:ByteArray L:Some(STRING(StringType)) C:Some(Utf8)
__index_level_0__           Optional None P:Int64 L:None C:None
--------------------------------------------------
\tdata information
\tgroup:              1 R:100 B:9597
--------------------------------------------------
   0: ID                         Int64 Snappy DO:531 RC:100 SZ:694/981/1.41 ENC:[PlainDictionary, Plain, Rle]ST:[max:  min: \u{12}\u{5}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0} null: 0]
   1: Name                       ByteArray Snappy DO:2938 RC:100 SZ:2329/2544/1.09 ENC:[PlainDictionary, Plain, Rle]ST:[max: ric Claude Jacques Poujade min: Abigail Jean \"Abby\" Larson null: 0]
   2: Sex                        ByteArray Snappy DO:3260 RC:100 SZ:78/74/0.95 ENC:[PlainDictionary, Plain, Rle]ST:[max: M min: F null: 0]
   3: Age                        Double Snappy DO:3488 RC:100 SZ:264/339/1.28 ENC:[PlainDictionary, Plain, Rle]ST:[max: \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}I@ min: \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}.@ null: 3]
   4: Height                     Double Snappy DO:3921 RC:100 SZ:337/475/1.41 ENC:[PlainDictionary, Plain, Rle]ST:[max: \u{0}\u{0}\u{0}\u{0}\u{0} i@ min: \u{0}\u{0}\u{0}\u{0}\u{0} b@ null: 22]
   5: Weight                     Double Snappy DO:4377 RC:100 SZ:357/507/1.42 ENC:[PlainDictionary, Plain, Rle]ST:[max:  min: \u{0}\u{0}\u{0}\u{0}\u{0}\u{0}D@ null: 23]
   6: Team                       ByteArray Snappy DO:5256 RC:100 SZ:765/903/1.18 ENC:[PlainDictionary, Plain, Rle]ST:[max: Yugoslavia min: Andorra null: 0]
   7: NOC                        ByteArray Snappy DO:5779 RC:100 SZ:434/515/1.19 ENC:[PlainDictionary, Plain, Rle]ST:[max: YUG min: AND null: 0]
   8: Games                      ByteArray Snappy DO:6218 RC:100 SZ:389/708/1.82 ENC:[PlainDictionary, Plain, Rle]ST:[max: 2016 Summer min: 1900 Summer null: 0]
   9: Year                       Int64 Snappy DO:6577 RC:100 SZ:276/362/1.31 ENC:[PlainDictionary, Plain, Rle]ST:[max:  min: l\u{7}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0} null: 0]
  10: Season                     ByteArray Snappy DO:6851 RC:100 SZ:99/95/0.96 ENC:[PlainDictionary, Plain, Rle]ST:[max: Winter min: Summer null: 0]
  11: City                       ByteArray Snappy DO:7398 RC:100 SZ:547/570/1.04 ENC:[PlainDictionary, Plain, Rle]ST:[max: Vancouver min: Albertville null: 0]
  12: Sport                      ByteArray Snappy DO:7977 RC:100 SZ:487/548/1.13 ENC:[PlainDictionary, Plain, Rle]ST:[max: Wrestling min: Alpine Skiing null: 0]
  13: Event                      ByteArray Snappy DO:9715 RC:100 SZ:1732/3370/1.95 ENC:[PlainDictionary, Plain, Rle]ST:[max: Wrestling Men\'s Super-Heavyweight, Greco-Roman min: Alpine Skiing Men\'s Downhill null: 0]
  14: Medal                      ByteArray Snappy DO:10101 RC:100 SZ:110/107/0.97 ENC:[PlainDictionary, Plain, Rle]ST:[max: Silver min: Bronze null: 87]
  15: __index_level_0__          Int64 Snappy DO:10778 RC:100 SZ:699/981/1.40 ENC:[PlainDictionary, Plain, Rle]ST:[max:  min: \u{18}\t\u{0}\u{0}\u{0}\u{0}\u{0}\u{0} null: 0]\n";

        assert_eq!(expected, string_output);
    }
}
