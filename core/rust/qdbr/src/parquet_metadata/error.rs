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

//! Error types specific to the `.qdbp` metadata file format.

use std::fmt;

/// Classifies `.qdbp` format errors into programmatically matchable categories.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum QdbpErrorKind {
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
    /// Parquet-to-qdbp conversion failed (unsupported feature, stat type, etc.).
    Conversion,
}

impl fmt::Display for QdbpErrorKind {
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
        }
    }
}

/// Creates a [`ParquetError`](crate::parquet::error::ParquetError) with a
/// [`QdbpErrorKind`] reason.
///
/// Two forms:
/// - `qdbp_err!(kind, "format string", args...)` — uses the format string as the description.
/// - `qdbp_err!(kind)` — uses the kind's `Display` impl as the description.
macro_rules! qdbp_err {
    ($kind:expr, $($arg:tt)+) => {
        $crate::parquet::error::ParquetError::with_descr(
            $crate::parquet::error::ParquetErrorReason::Qdbp($kind),
            format!($($arg)+))
    };
    ($kind:expr) => {{
        let k = $kind;
        $crate::parquet::error::ParquetError::with_descr(
            $crate::parquet::error::ParquetErrorReason::Qdbp(k),
            k.to_string())
    }};
}

pub(crate) use qdbp_err;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_all_variants() {
        assert_eq!(
            QdbpErrorKind::VersionMismatch { found: 99, expected: 1 }.to_string(),
            "version mismatch: found 99, expected 1"
        );
        assert_eq!(
            QdbpErrorKind::ChecksumMismatch { stored: 0xAABB, computed: 0xCCDD }.to_string(),
            "checksum mismatch: stored 0x0000AABB, computed 0x0000CCDD"
        );
        assert_eq!(QdbpErrorKind::Truncated.to_string(), "truncated data");
        assert_eq!(QdbpErrorKind::Alignment.to_string(), "alignment error");
        assert_eq!(QdbpErrorKind::InvalidValue.to_string(), "invalid value");
        assert_eq!(QdbpErrorKind::SchemaMismatch.to_string(), "schema mismatch");
        assert_eq!(QdbpErrorKind::Conversion.to_string(), "conversion error");
    }
}
