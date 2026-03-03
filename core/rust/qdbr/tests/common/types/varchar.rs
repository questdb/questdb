use parquet::data_type::ByteArray;

pub const HEADER_FLAG_INLINED: u8 = 1;
pub const HEADER_FLAG_NULL: u8 = 4;
pub const HEADER_FLAGS_WIDTH: u8 = 4;

pub fn read_offset(aux: &[u8], base: usize) -> usize {
    let lo = u16::from_le_bytes([aux[base], aux[base + 1]]) as usize;
    let hi =
        u32::from_le_bytes([aux[base + 2], aux[base + 3], aux[base + 4], aux[base + 5]]) as usize;
    lo | (hi << 16)
}

pub fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| {
            if i % 7 == 0 {
                ByteArray::from(format!("overflow_value_{i:06}").as_str())
            } else if i % 11 == 0 {
                ByteArray::from(format!("caf\u{00e9}_{i}").as_str())
            } else {
                ByteArray::from(format!("val_{i:04}").as_str())
            }
        })
        .collect()
}

pub fn expected_varchar_str(i: usize) -> String {
    if i % 7 == 0 {
        format!("overflow_value_{i:06}")
    } else if i % 11 == 0 {
        format!("caf\u{00e9}_{i}")
    } else {
        format!("val_{i:04}")
    }
}
