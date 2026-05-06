use async_compat::Compat;
use parquet2::{error::Result, read::read_metadata_async};
use tokio::fs;

#[tokio::main]
async fn main() -> Result<()> {
    let mut reader = Compat::new(fs::File::open("path.parquet").await?);
    let metadata = read_metadata_async(&mut reader).await?;

    // metadata
    println!("{:#?}", metadata);
    Ok(())
}
