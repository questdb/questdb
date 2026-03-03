use parquet::data_type::ByteArray;

pub fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| {
            if i % 11 == 0 {
                ByteArray::from(format!("caf\u{00e9}_{i}").as_str())
            } else if i % 13 == 0 {
                ByteArray::from(format!("emoji\u{1F600}_{i}").as_str())
            } else {
                ByteArray::from(format!("str_{i:04}").as_str())
            }
        })
        .collect()
}

pub fn expected_str_value(i: usize) -> String {
    if i % 11 == 0 {
        format!("caf\u{00e9}_{i}")
    } else if i % 13 == 0 {
        format!("emoji\u{1F600}_{i}")
    } else {
        format!("str_{i:04}")
    }
}
