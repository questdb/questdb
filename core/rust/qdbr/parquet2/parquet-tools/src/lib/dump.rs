//! Subcommand `dump`. This subcommand shows the parquet metadata information
use parquet2::{
    page::{
        BinaryPageDict, DataPageHeader, DictPage, FixedLenByteArrayPageDict, PrimitivePageDict,
    },
    read::{get_page_iterator, read_metadata},
    schema::types::PhysicalType,
};

use std::{fs::File, io::Write, path::Path, sync::Arc};

use crate::{Result, SEPARATOR};

// Dumps data from the file.
// The function prints a sample of the data from each of the RowGroups.
// The the columns to be printed and the sample size is controlled using the
// arguments introduced in the command line
pub fn dump_file<T, W>(
    file_name: T,
    sample_size: usize,
    columns: Option<Vec<usize>>,
    writer: &mut W,
) -> Result<()>
where
    T: AsRef<Path> + std::fmt::Display,
    W: Write,
{
    let mut file = File::open(file_name)?;

    let metadata = read_metadata(&mut file)?;

    let columns = match columns {
        Some(cols) => cols,
        None => {
            let num_cols = metadata.schema().fields().len();
            (0..num_cols).collect()
        }
    };

    for (i, group) in metadata.row_groups.iter().enumerate() {
        writeln!(
            writer,
            "Group: {:<10}Rows: {:<15}Bytes: {:}",
            i,
            group.num_rows(),
            group.total_byte_size()
        )?;
        writeln!(writer, "{}", SEPARATOR)?;

        for column in &columns {
            let column_meta = &group.columns()[*column];
            let iter =
                get_page_iterator(column_meta, &mut file, None, Vec::with_capacity(4 * 1024))?;
            for (page_ind, page) in iter.enumerate() {
                let page = page?;
                writeln!(
                    writer,
                    "\nPage: {:<10}Column: {:<15} Bytes: {:}",
                    page_ind,
                    column,
                    page.uncompressed_size()
                )?;
                let (dict, msg_type) = match page.header() {
                    DataPageHeader::V1(_) => {
                        if let Some(dict) = page.dictionary_page {
                            (dict, "PageV1")
                        } else {
                            continue;
                        }
                    }
                    DataPageHeader::V2(_) => {
                        if let Some(dict) = page.dictionary_page {
                            (dict, "PageV2")
                        } else {
                            continue;
                        }
                    }
                };

                writeln!(
                    writer,
                    "Compressed page: {:<15} Physical type: {:?}",
                    msg_type,
                    dict.physical_type()
                )?;

                print_dictionary(dict, sample_size, writer)?;
            }
        }
    }

    Ok(())
}

fn print_dictionary<W>(dict: Arc<dyn DictPage>, sample_size: usize, writer: &mut W) -> Result<()>
where
    W: Write,
{
    match dict.physical_type() {
        PhysicalType::Boolean => {
            writeln!(writer, "Boolean physical type cannot be dictionary-encoded")?;
        }
        PhysicalType::Int32 => {
            if let Some(res) = dict.as_any().downcast_ref::<PrimitivePageDict<i32>>() {
                print_iterator(res.values().iter(), sample_size, writer)?;
            }
        }
        PhysicalType::Int64 => {
            if let Some(res) = dict.as_any().downcast_ref::<PrimitivePageDict<i64>>() {
                print_iterator(res.values().iter(), sample_size, writer)?;
            }
        }
        PhysicalType::Int96 => {
            if let Some(res) = dict.as_any().downcast_ref::<PrimitivePageDict<[u32; 3]>>() {
                print_iterator(res.values().iter(), sample_size, writer)?;
            }
        }
        PhysicalType::Float => {
            if let Some(res) = dict.as_any().downcast_ref::<PrimitivePageDict<f32>>() {
                print_iterator(res.values().iter(), sample_size, writer)?;
            }
        }
        PhysicalType::Double => {
            if let Some(res) = dict.as_any().downcast_ref::<PrimitivePageDict<f64>>() {
                print_iterator(res.values().iter(), sample_size, writer)?;
            }
        }
        PhysicalType::ByteArray => {
            if let Some(res) = dict.as_any().downcast_ref::<BinaryPageDict>() {
                for (i, pair) in res.offsets().windows(2).enumerate().take(sample_size) {
                    let bytes = &res.values()[pair[0] as usize..pair[1] as usize];
                    let msg = String::from_utf8_lossy(bytes);

                    writeln!(writer, "Value: {:<10}\t{:?}", i, msg)?;
                }
            }
        }
        PhysicalType::FixedLenByteArray(size) => {
            if let Some(res) = dict.as_any().downcast_ref::<FixedLenByteArrayPageDict>() {
                for (i, bytes) in res
                    .values()
                    .chunks(*size as usize)
                    .enumerate()
                    .take(sample_size)
                {
                    let msg = String::from_utf8_lossy(bytes);

                    writeln!(writer, "Value: {:<10}\t{:?}", i, msg)?;
                }
            }
        }
    }

    Ok(())
}

fn print_iterator<I, T, W>(iter: I, sample_size: usize, writer: &mut W) -> Result<()>
where
    I: Iterator<Item = T>,
    T: std::fmt::Debug,
    W: Write,
{
    for (i, val) in iter.enumerate().take(sample_size) {
        writeln!(writer, "Value: {:<10}\t{:?}", i, val)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_meta() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        dump_file(file_name, 1, None, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();

        let expected = "Group: 0         Rows: 100            Bytes: 9597
--------------------------------------------------

Page: 0         Column: 0               Bytes: 100
Compressed page: PageV1          Physical type: Int64
Value: 0         \t97007

Page: 0         Column: 1               Bytes: 100
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Leif Tommy Prim\"

Page: 0         Column: 2               Bytes: 22
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"M\"

Page: 0         Column: 3               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t20.0

Page: 0         Column: 4               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t185.0

Page: 0         Column: 5               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t76.0

Page: 0         Column: 6               Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Sweden\"

Page: 0         Column: 7               Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"SWE\"

Page: 0         Column: 8               Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"1976 Summer\"

Page: 0         Column: 9               Bytes: 74
Compressed page: PageV1          Physical type: Int64
Value: 0         \t1976

Page: 0         Column: 10              Bytes: 23
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Summer\"

Page: 0         Column: 11              Bytes: 87
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Montreal\"

Page: 0         Column: 12              Bytes: 74
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Cycling\"

Page: 0         Column: 13              Bytes: 100
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Cycling Men\\\'s 100 kilometres Team Time Trial\"

Page: 0         Column: 14              Bytes: 26
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"Bronze\"

Page: 0         Column: 15              Bytes: 100
Compressed page: PageV1          Physical type: Int64
Value: 0         \t193210\n";

        assert_eq!(expected, string_output);
    }

    #[test]
    fn test_show_meta_cols() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        let columns = Some(vec![0, 2, 5]);

        dump_file(file_name, 1, columns, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();

        let expected = "Group: 0         Rows: 100            Bytes: 9597
--------------------------------------------------

Page: 0         Column: 0               Bytes: 100
Compressed page: PageV1          Physical type: Int64
Value: 0         \t97007

Page: 0         Column: 2               Bytes: 22
Compressed page: PageV1          Physical type: ByteArray
Value: 0         \t\"M\"

Page: 0         Column: 5               Bytes: 83
Compressed page: PageV1          Physical type: Double
Value: 0         \t76.0\n";

        assert_eq!(expected, string_output);
    }
}
