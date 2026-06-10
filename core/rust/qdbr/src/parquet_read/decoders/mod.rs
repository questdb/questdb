//! Encoding-specific decoders used by `parquet_read`.
//!
//! These modules translate Parquet page payloads into QuestDB column buffers and
//! share small conversion/iteration utilities used across decode paths.

/// Physical-to-logical value converters shared by primitive decoders.
mod converters;
/// Delta-binary-packed primitive decoder (`DELTA_BINARY_PACKED`).
mod delta_binary_packed;
/// Delta-length-byte-array VarcharSlice decoder (`DELTA_LENGTH_BYTE_ARRAY`).
mod delta_length_array;
/// Dictionary page readers and dictionary lookup abstractions.
mod dictionary;
/// Plain-encoded primitive decoder (`PLAIN`).
mod plain;
/// Lightweight iterators used by hybrid RLE dictionary decoding.
mod rle;
/// Specialized RLE dictionary decoder for VarcharSlice columns.
mod rle_dict_varchar_slice;
/// Hybrid RLE + dictionary index decoder (`RLE_DICTIONARY`).
mod rle_dictionary;
pub(crate) mod unpack;

use crate::parquet::error::{fmt_err, ParquetResult};

pub use self::dictionary::{
    BasePrimitiveDictDecoder, BaseVarDictDecoder, ConvertablePrimitiveDictDecoder,
    FixedDictDecoder, PrimitiveDictDecoder, RleLocalIsGlobalSymbolDictDecoder, VarDictDecoder,
};
pub use converters::*;
pub use delta_binary_packed::DeltaBinaryPackedDecoder;
pub(crate) use delta_binary_packed::MiniblockIterator;
pub use delta_length_array::DeltaLAVarcharSliceDecoder;
pub use plain::{PlainBooleanDecoder, PlainPrimitiveDecoder};
pub use rle::{RepeatN, RleBooleanDecoder, RleIterator};
pub use rle_dict_varchar_slice::RleDictVarcharSliceDecoder;
pub use rle_dictionary::RleDictionaryDecoder;

/// Fallibly reserves capacity for `num_values` dictionary entries of type `T`,
/// returning a recoverable `OutOfMemory` error instead of aborting the process
/// when the reservation fails.
///
/// Dictionary `num_values` comes from a page header and is attacker-controlled.
/// Each decoder validates it against the dict buffer (so it cannot exceed the
/// buffer), but for a compressed page that buffer is the *decompressed* one,
/// sized only by uncompressed_page_size -- up to i32::MAX and NOT bounded by
/// max_page_size. When `T` is wider than a value's footprint in the buffer the
/// reservation amplifies the buffer (up to 32x for `Decimal256` over a 1-byte
/// FixedLenByteArray dict, ~4x for the 16-byte var-width slice/aux entries), so
/// a multi-GiB decompressed dict can drive a tens-of-gigabytes reservation. A
/// plain `Vec::with_capacity` aborts the JVM over JNI on allocation failure;
/// `try_reserve_exact` keeps it recoverable while still decoding any dictionary
/// that genuinely fits in memory. The size is exact and final (callers push
/// exactly `num_values` entries), so `_exact` avoids over-allocation. The reason
/// is `OutOfMemory` (not `Layout`): the layout is already validated, so a failure
/// here is purely an allocation shortfall, and the write path must retry on
/// transient pressure rather than suspend the table. `what` names the entry kind
/// for the error message.
pub(crate) fn try_reserve_dict_values<T>(num_values: usize, what: &str) -> ParquetResult<Vec<T>> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(num_values)
        .map_err(|_| fmt_err!(OutOfMemory(None), "cannot allocate {num_values} {what}"))?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::try_reserve_dict_values;
    use crate::parquet::error::ParquetErrorReason;

    #[test]
    fn try_reserve_dict_values_rejects_unsatisfiable_count_instead_of_aborting() {
        // num_values comes from a dictionary page header and is bounded only
        // against the (possibly multi-GiB, decompressed) dict buffer, so the
        // reservation can be tens of gigabytes. A genuine such request may
        // succeed on a large host, so to pin the fallible path deterministically
        // we ask for usize::MAX entries: try_reserve_exact fails with
        // CapacityOverflow without attempting (and aborting on) a real
        // allocation. Proves the sizing surfaces a clean error rather than the
        // process-aborting Vec::with_capacity.
        let err = try_reserve_dict_values::<[u64; 2]>(usize::MAX, "dictionary aux entries")
            .expect_err("an unsatisfiable dictionary value count must error, not abort");
        assert!(
            err.to_string().contains("cannot allocate"),
            "unexpected error: {err}"
        );
        // Classify the failure OutOfMemory, not Layout: the layout is already
        // validated, and on the write path (a parquet merge) a Layout error
        // suspends the table while OutOfMemory backs off and retries.
        assert!(
            matches!(err.reason(), ParquetErrorReason::OutOfMemory(_)),
            "allocation failure must be classified OutOfMemory, not Layout: {err:?}"
        );
    }
}
