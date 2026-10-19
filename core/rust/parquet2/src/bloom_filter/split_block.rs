use std::convert::TryInto;

/// magic numbers taken from https://github.com/apache/parquet-format/blob/master/BloomFilter.md
const SALT: [u32; 8] = [
    1203114875, 1150766481, 2284105051, 2729912477, 1884591559, 770785867, 2667333959, 1550580529,
];

#[inline]
fn hash_to_block_index(hash: u64, len: usize) -> usize {
    let number_of_blocks = len as u64 / 32;
    let high_hash = hash >> 32;
    ((high_hash * number_of_blocks) >> 32) as usize
}

#[inline]
pub fn is_in_set(bitset: &[u8], hash: u64) -> bool {
    let block_index = hash_to_block_index(hash, bitset.len());
    let key = hash as u32;
    let block_offset = block_index * 32;

    for (i, &salt) in SALT.iter().enumerate() {
        let mask = 0x1u32 << ((key.wrapping_mul(salt)) >> 27);
        let word_offset = block_offset + i * 4;
        let word_bytes = &bitset[word_offset..word_offset + 4];
        let word = u32::from_le_bytes(word_bytes.try_into().unwrap());
        if word & mask == 0 {
            return false;
        }
    }
    true
}

/// Inserts a new hash to the set
#[inline]
pub fn insert(bitset: &mut [u8], hash: u64) {
    let block_index = hash_to_block_index(hash, bitset.len());
    let key = hash as u32;
    let block_offset = block_index * 32;

    for (i, &salt) in SALT.iter().enumerate() {
        let mask = 0x1u32 << ((key.wrapping_mul(salt)) >> 27);
        let word_offset = block_offset + i * 4;
        let word_bytes = &mut bitset[word_offset..word_offset + 4];
        let word = u32::from_le_bytes(word_bytes.try_into().unwrap());
        let new_word = word | mask;
        word_bytes.copy_from_slice(&new_word.to_le_bytes());
    }
}
