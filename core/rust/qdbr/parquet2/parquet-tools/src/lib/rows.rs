use parquet2::read::read_metadata;
use std::{fs::File, io::Write, path::Path};

use super::Result;

pub fn show_rows<T, W>(file_name: T, writer: &mut W) -> Result<()>
where
    T: AsRef<Path>,
    W: Write,
{
    let mut file = File::open(file_name)?;

    let metadata = read_metadata(&mut file)?;

    write!(writer, "Total RowCount: {}", metadata.num_rows)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_show_rows() {
        let file_name = "data/sample.parquet";
        let mut buf = Vec::new();

        show_rows(file_name, &mut buf).unwrap();

        let string_output = String::from_utf8(buf).unwrap();
        let expected = "Total RowCount: 100".to_string();
        assert_eq!(expected, string_output);
    }
}
