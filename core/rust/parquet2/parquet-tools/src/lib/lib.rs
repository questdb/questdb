pub mod dump;
pub mod meta;
pub mod rows;

const SEPARATOR: &str = "--------------------------------------------------";
pub type Result<T> = ::std::result::Result<T, Box<dyn std::error::Error>>;
