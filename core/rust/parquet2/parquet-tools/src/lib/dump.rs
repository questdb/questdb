//! Subcommand `dump`. This subcommand shows the parquet metadata information
use parquet2::{
    page::{CompressedPage, DataPageHeader},
    read::{get_page_iterator, read_metadata},
};

use std::{fs::File, io::Write, path::Path};

use crate::{Result, SEPARATOR};

// Dumps data from the file.
// The function prints a sample of the data from each of the RowGroups.
// The columns to be printed is controlled using the arguments introduced in the command line
pub fn dump_file<T, W>(file_name: T, columns: Option<Vec<usize>>, writer: &mut W) -> Result<()>
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
            let iter = get_page_iterator(
                column_meta,
                &mut file,
                None,
                Vec::with_capacity(4 * 1024),
                usize::MAX,
            )?;
            for (page_ind, page) in iter.enumerate() {
                let page = page?;
                match page {
                    CompressedPage::Data(data_page) => {
                        let page_type = match data_page.header() {
                            DataPageHeader::V1(_) => "PageV1",
                            DataPageHeader::V2(_) => "PageV2",
                        };
                        writeln!(
                            writer,
                            "Page: {:<10}Column: {:<15}Type: {:<10}Bytes: {:}",
                            page_ind,
                            column,
                            page_type,
                            data_page.uncompressed_size()
                        )?;
                    }
                    CompressedPage::Dict(_) => {
                        writeln!(
                            writer,
                            "Page: {:<10}Column: {:<15}Type: DictPage",
                            page_ind, column
                        )?;
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dump_file() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        dump_file(file_name, None, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();
        assert!(string_output.contains("Group: 0"));
        assert!(string_output.contains("Rows: 100"));
    }
}
