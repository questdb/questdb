use std::char::DecodeUtf16Error;

/// Decode a UTF-16 iterator and append the resulting UTF-8 bytes to `dest`.
/// Returns the number of UTF-8 bytes written.
///
/// `strict` controls how unpaired surrogates are handled:
/// - `false` (default): substitute with `U+FFFD` (REPLACEMENT CHARACTER) and
///   keep going. This matches `String::from_utf16_lossy` and QuestDB's
///   `QwpColumnScratch.encodeUtf8` (the most recent surrogate-handling code
///   in the repo).
/// - `true`: return the underlying `DecodeUtf16Error` so callers can surface a
///   fatal error. Used by the storage / export paths when the user opts into
///   strict mode via `cairo.{parquet.export,partition.encoder.parquet}.fail.on.invalid.utf16`.
pub fn write_utf8_from_utf16_iter(
    dest: &mut Vec<u8>,
    src: impl Iterator<Item = u16>,
    strict: bool,
) -> Result<usize, DecodeUtf16Error> {
    let start_count = dest.len();
    for c in char::decode_utf16(src) {
        let c = match c {
            Ok(c) => c,
            Err(_) if !strict => char::REPLACEMENT_CHARACTER,
            Err(e) => return Err(e),
        };
        match c.len_utf8() {
            1 => dest.push(c as u8),
            _ => dest.extend_from_slice(c.encode_utf8(&mut [0; 4]).as_bytes()),
        }
    }
    Ok(dest.len() - start_count)
}
