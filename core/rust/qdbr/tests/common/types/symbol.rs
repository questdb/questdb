use parquet::data_type::ByteArray;

pub fn generate_values(count: usize) -> Vec<ByteArray> {
    let labels = ["alpha", "beta", "gamma", "delta", "epsilon"];
    (0..count)
        .map(|i| ByteArray::from(labels[i % labels.len()]))
        .collect()
}
