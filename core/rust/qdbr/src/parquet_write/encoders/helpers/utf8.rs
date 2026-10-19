use std::char::DecodeUtf16Error;

/// Decode a UTF-16 iterator and append the resulting UTF-8 bytes to `dest`.
/// Returns the number of UTF-8 bytes written.
pub fn write_utf8_from_utf16_iter(
    dest: &mut Vec<u8>,
    src: impl Iterator<Item = u16>,
) -> Result<usize, DecodeUtf16Error> {
    let start_count = dest.len();
    for c in char::decode_utf16(src) {
        let c = c?;
        match c.len_utf8() {
            1 => dest.push(c as u8),
            _ => dest.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes()),
        }
    }
    Ok(dest.len() - start_count)
}
