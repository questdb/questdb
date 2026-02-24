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
/// Hybrid RLE + dictionary index decoder (`RLE_DICTIONARY`).
mod rle_dictionary;
/// Specialized RLE dictionary decoder for VarcharSlice columns.
mod rle_dict_varchar_slice;
pub(crate) mod unpack;

pub use self::dictionary::{
    BasePrimitiveDictDecoder, BaseVarDictDecoder, ConvertablePrimitiveDictDecoder,
    FixedDictDecoder, PrimitiveDictDecoder, RleLocalIsGlobalSymbolDictDecoder, VarDictDecoder,
};
pub use converters::*;
pub use delta_binary_packed::DeltaBinaryPackedDecoder;
pub use delta_length_array::DeltaLAVarcharSliceDecoder;
pub use plain::{PlainBooleanDecoder, PlainPrimitiveDecoder};
pub use rle::{RepeatN, RleBooleanDecoder, RleIterator};
pub use rle_dict_varchar_slice::RleDictVarcharSliceDecoder;
pub use rle_dictionary::RleDictionaryDecoder;
