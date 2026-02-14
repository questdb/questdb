//! Encoding-specific decoders used by `parquet_read`.
//!
//! These modules translate Parquet page payloads into QuestDB column buffers and
//! share small conversion/iteration utilities used across decode paths.

/// Physical-to-logical value converters shared by primitive decoders.
mod converters;
/// Delta-binary-packed primitive decoder (`DELTA_BINARY_PACKED`).
mod delta_binary_packed;
/// Dictionary page readers and dictionary lookup abstractions.
mod dictionary;
/// Plain-encoded primitive decoder (`PLAIN`).
mod plain;
/// Lightweight iterators used by hybrid RLE dictionary decoding.
mod rle;
/// Hybrid RLE + dictionary index decoder (`RLE_DICTIONARY`).
mod rle_dictionary;

pub use self::dictionary::{
    BasePrimitiveDictDecoder, BaseVarDictDecoder, ConvertablePrimitiveDictDecoder,
    FixedDictDecoder, RleLocalIsGlobalSymbolDictDecoder, VarDictDecoder,
};
pub use converters::*;
pub use delta_binary_packed::DeltaBinaryPackedPrimitiveDecoder;
pub use plain::{PlainBooleanDecoder, PlainPrimitiveDecoder};
pub use rle::{RepeatN, RleBooleanDecoder, RleIterator};
pub use rle_dictionary::RleDictionaryDecoder;
