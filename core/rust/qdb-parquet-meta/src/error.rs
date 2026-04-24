/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! Error types specific to the `_pm` metadata file format.

use std::fmt;

/// Classifies `_pm` format errors into programmatically matchable categories.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ParquetMetaErrorKind {
    /// Format version in the file does not match the expected version.
    VersionMismatch { found: u32, expected: u32 },
    /// CRC32 checksum mismatch between stored and computed values.
    ChecksumMismatch { stored: u32, computed: u32 },
    /// Data is truncated: a section is smaller than expected.
    Truncated,
    /// A byte offset violates the required alignment.
    Alignment,
    /// A field contains a value outside its valid range.
    InvalidValue,
    /// Column counts differ between metadata sources.
    SchemaMismatch,
    /// Parquet-to-pm conversion failed (unsupported feature, stat type, etc.).
    Conversion,
    /// File requires feature flags that this reader does not support.
    UnsupportedFeature { flags: u64 },
}

impl fmt::Display for ParquetMetaErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::VersionMismatch { found, expected } => {
                write!(f, "version mismatch: found {found}, expected {expected}")
            }
            Self::ChecksumMismatch { stored, computed } => {
                write!(
                    f,
                    "checksum mismatch: stored 0x{stored:08X}, computed 0x{computed:08X}"
                )
            }
            Self::Truncated => write!(f, "truncated data"),
            Self::Alignment => write!(f, "alignment error"),
            Self::InvalidValue => write!(f, "invalid value"),
            Self::SchemaMismatch => write!(f, "schema mismatch"),
            Self::Conversion => write!(f, "conversion error"),
            Self::UnsupportedFeature { flags } => {
                write!(f, "unsupported required feature flags: 0x{flags:016X}")
            }
        }
    }
}

/// Self-contained error type carrying a kind plus a descriptive message.
///
/// qdbr wraps this in its richer `ParquetError` via a `From` impl, so `?`
/// transparently lifts `ParquetMetaResult` into `ParquetResult` at the crate
/// boundary.
#[derive(Debug, Clone)]
pub struct ParquetMetaError {
    pub kind: ParquetMetaErrorKind,
    pub msg: String,
}

impl ParquetMetaError {
    pub fn with_descr(kind: ParquetMetaErrorKind, descr: impl Into<String>) -> Self {
        Self {
            kind,
            msg: descr.into(),
        }
    }
}

impl fmt::Display for ParquetMetaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for ParquetMetaError {}

pub type ParquetMetaResult<T> = Result<T, ParquetMetaError>;

/// Builds a [`ParquetMetaError`] with a [`ParquetMetaErrorKind`] and a
/// format-string description.
///
/// Two forms:
/// - `parquet_meta_err!(kind, "format string", args...)`
/// - `parquet_meta_err!(kind)` — uses the kind's `Display` impl as the description.
///
/// Note: qdbr keeps its own `parquet_meta_err!` macro that produces its
/// `ParquetError` directly. Same name, different crate — callers import
/// whichever one matches the error type they return.
#[macro_export]
macro_rules! parquet_meta_err {
    ($kind:expr, $($arg:tt)+) => {
        $crate::error::ParquetMetaError::with_descr($kind, format!($($arg)+))
    };
    ($kind:expr) => {{
        let k = $kind;
        $crate::error::ParquetMetaError::with_descr(k, k.to_string())
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_all_variants() {
        assert_eq!(
            ParquetMetaErrorKind::VersionMismatch {
                found: 99,
                expected: 1
            }
            .to_string(),
            "version mismatch: found 99, expected 1"
        );
        assert_eq!(
            ParquetMetaErrorKind::ChecksumMismatch {
                stored: 0xAABB,
                computed: 0xCCDD
            }
            .to_string(),
            "checksum mismatch: stored 0x0000AABB, computed 0x0000CCDD"
        );
        assert_eq!(
            ParquetMetaErrorKind::Truncated.to_string(),
            "truncated data"
        );
        assert_eq!(
            ParquetMetaErrorKind::Alignment.to_string(),
            "alignment error"
        );
        assert_eq!(
            ParquetMetaErrorKind::InvalidValue.to_string(),
            "invalid value"
        );
        assert_eq!(
            ParquetMetaErrorKind::SchemaMismatch.to_string(),
            "schema mismatch"
        );
        assert_eq!(
            ParquetMetaErrorKind::Conversion.to_string(),
            "conversion error"
        );
        assert_eq!(
            ParquetMetaErrorKind::UnsupportedFeature {
                flags: 0x1_0000_0000
            }
            .to_string(),
            "unsupported required feature flags: 0x0000000100000000"
        );
    }

    #[test]
    fn error_preserves_kind_and_msg() {
        let err = ParquetMetaError::with_descr(ParquetMetaErrorKind::Truncated, "oops");
        assert_eq!(err.kind, ParquetMetaErrorKind::Truncated);
        assert_eq!(err.msg, "oops");
        assert_eq!(err.to_string(), "oops");
    }

    #[test]
    fn macro_two_forms_yield_consistent_kind() {
        let a: ParquetMetaError = parquet_meta_err!(ParquetMetaErrorKind::InvalidValue, "x={}", 1);
        let b: ParquetMetaError = parquet_meta_err!(ParquetMetaErrorKind::InvalidValue);
        assert_eq!(a.kind, ParquetMetaErrorKind::InvalidValue);
        assert_eq!(a.msg, "x=1");
        assert_eq!(b.kind, ParquetMetaErrorKind::InvalidValue);
        assert_eq!(b.msg, "invalid value");
    }
}
