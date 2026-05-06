use parquet::data_type::ByteArray;

pub fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| {
            let bytes: Vec<u8> = (0..10).map(|j| ((i * 7 + j) % 256) as u8).collect();
            ByteArray::from(bytes)
        })
        .collect()
}
