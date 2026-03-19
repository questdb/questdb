//! Layer 3 (Encoding) — Encoding-specific loop structures.
//!
//! - **PLAIN / DELTA / BSS**: Direct flat-array loop with per-value filter.
//! - **RLE_DICTIONARY**: Dictionary-filtered loop — checks a pre-computed
//!   `match_bitmap` per dictionary index and only decodes matching values.

pub mod dict_filter;
pub mod multi;
pub mod multi_page;
pub mod plain;

#[cfg(target_arch = "x86_64")]
use super::codegen::Codegen;
use super::{JitEncoding, JitError, PipelineSpec};

#[cfg(target_arch = "x86_64")]
pub fn emit(cg: &mut Codegen, spec: &PipelineSpec) -> Result<(), JitError> {
    match spec.encoding {
        JitEncoding::RleDictionary => dict_filter::emit(cg, spec),
        JitEncoding::Plain
        | JitEncoding::DeltaBinaryPacked
        | JitEncoding::DeltaLengthByteArray
        | JitEncoding::ByteStreamSplit => plain::emit(cg, spec),
    }
}

use super::ir_builder::IrBuilder;

/// Emit IR for the given pipeline specification.
pub fn emit_ir(b: &mut IrBuilder, spec: &PipelineSpec) -> Result<(), JitError> {
    match spec.encoding {
        JitEncoding::RleDictionary => dict_filter::emit_ir(b, spec),
        JitEncoding::Plain
        | JitEncoding::DeltaBinaryPacked
        | JitEncoding::DeltaLengthByteArray
        | JitEncoding::ByteStreamSplit => plain::emit_ir(b, spec),
    }
}
