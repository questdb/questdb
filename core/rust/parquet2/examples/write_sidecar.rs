use parquet2::{
    error::Error,
    metadata::SchemaDescriptor,
    schema::types::{ParquetType, PhysicalType},
    write::{write_metadata_sidecar, FileWriter, Version, WriteOptions},
};

fn main() -> Result<(), Error> {
    // say we have 10 files with the same schema:
    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical(
            "c1".to_string(),
            PhysicalType::Int32,
        )],
    );

    // we can collect their metadata after they are written
    let mut metadatas = vec![];
    for i in 0..10 {
        let relative_path = format!("part-{i}.parquet");
        let writer = std::io::Cursor::new(vec![]);
        let mut writer = FileWriter::new(
            writer,
            schema.clone(),
            WriteOptions {
                write_statistics: true,
                version: Version::V2,
                bloom_filter_fpp: 0.01,
            },
            None,
        );

        // we write row groups to it
        // writer.write(row_group)

        // and the footer
        writer.end(None)?;
        let (_, mut metadata) = writer.into_inner_and_metadata();

        // once done, we write the relative path to the column chunks
        metadata.row_groups.iter_mut().for_each(|row_group| {
            row_group
                .columns
                .iter_mut()
                .for_each(|column| column.file_path = Some(relative_path.clone()))
        });
        // and collect the metadata
        metadatas.push(metadata);
    }

    // we can then merge their row groups
    let first = metadatas.pop().unwrap();
    let sidecar = metadatas.into_iter().fold(first, |mut acc, metadata| {
        acc.row_groups.extend(metadata.row_groups.into_iter());
        acc
    });

    // and write the metadata on a separate file
    let mut writer = std::io::Cursor::new(vec![]);
    write_metadata_sidecar(&mut writer, &sidecar)?;

    Ok(())
}
