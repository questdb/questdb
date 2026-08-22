use parquet::data_type::ByteArray;

pub fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| {
            if i.is_multiple_of(11) {
                ByteArray::from(format!("caf\u{00e9}_{i}").as_str())
            } else if i.is_multiple_of(13) {
                ByteArray::from(format!("emoji\u{1F600}_{i}").as_str())
            } else {
                ByteArray::from(format!("str_{i:04}").as_str())
            }
        })
        .collect()
}

pub fn expected_str_value(i: usize) -> String {
    if i.is_multiple_of(11) {
        format!("caf\u{00e9}_{i}")
    } else if i.is_multiple_of(13) {
        format!("emoji\u{1F600}_{i}")
    } else {
        format!("str_{i:04}")
    }
}
