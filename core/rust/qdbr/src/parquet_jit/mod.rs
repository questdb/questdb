//! JIT code generation for fused parquet decode+filter+aggregate pipelines.
//!
//! Architecture follows Tidy Tuples (Kersten et al.):
//!
//! - **Layer 5 – Codegen** (`codegen.rs`): Platform-independent instruction
//!   emission with comparison-branch fusion (Flying Start style).
//! - **Layer 4 – Values** (`values.rs`): Type-dispatched load / compare /
//!   aggregate operations.
//! - **Layer 3 – Encoding** (`encode/`): Encoding-specific loop structures
//!   that compose Values and Codegen operations.
//! - **Layer 2 – Pipeline** (`generate_pipeline()`): Top-level entry point
//!   that orchestrates prologue, encoding emission, and epilogue.
//!
//! All encodings share the same JIT inner-loop template operating on flat
//! value arrays.  Non-PLAIN encodings (RLE_DICTIONARY, DELTA_BINARY_PACKED,
//! BYTE_STREAM_SPLIT) are pre-decoded to flat buffers by compiled Rust code
//! before the JIT function is called; the JIT treats them identically to
//! PLAIN.  Encoding-specific JIT optimisations (dictionary pre-filter, RLE
//! run aggregation) will be added incrementally.

pub mod backend;
#[cfg(target_arch = "x86_64")]
mod codegen;
pub mod encode;
pub mod ir;
pub mod ir_builder;
pub mod jni;
pub mod multi;
mod registry;
mod values;

pub use registry::REGISTRY;

// -----------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------

/// Parquet encoding type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum JitEncoding {
    Plain = 0,
    RleDictionary = 1,
    DeltaBinaryPacked = 2,
    DeltaLengthByteArray = 3,
    ByteStreamSplit = 4,
}

/// Physical value type.
///
/// Maps every QuestDB column type to a JIT category:
///
/// | JIT type | Size | QuestDB types |
/// |----------|------|---------------|
/// | Int8     | 1 B  | Byte, GeoByte, Decimal8, Boolean |
/// | Int16    | 2 B  | Short, Char, GeoShort, Decimal16 |
/// | Int32    | 4 B  | Int, IPv4, GeoInt, Decimal32, Symbol |
/// | Int64    | 8 B  | Long, Date, Timestamp, GeoLong, Decimal64 |
/// | Float    | 4 B  | Float |
/// | Double   | 8 B  | Double |
/// | Int128   | 16 B | Uuid, Long128, Decimal128 |
/// | Int256   | 32 B | Long256, Decimal256 |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum JitPhysicalType {
    Int32 = 0,
    Int64 = 1,
    Float = 2,
    Double = 3,
    Int8 = 4,
    Int16 = 5,
    Int128 = 6,
    Int256 = 7,
}

/// Filter comparison operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum JitFilterOp {
    None = 0,
    Eq = 1,
    Ne = 2,
    Lt = 3,
    Le = 4,
    Gt = 5,
    Ge = 6,
    Between = 7,
    IsNull = 8,
    IsNotNull = 9,
}

/// Aggregate operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum JitAggregateOp {
    Count = 0,
    Sum = 1,
    Min = 2,
    Max = 3,
}

/// Output mode for a JIT pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum JitOutputMode {
    /// Compute an aggregate (COUNT / SUM / MIN / MAX).
    /// Result written to `result_ptr` as `PipelineResult`.
    Aggregate = 0,
    /// Write all values that pass the filter into a contiguous output buffer.
    /// `result_ptr` = output data buffer (must be `row_count * type_size`).
    /// `filter_hi` (repurposed) = pointer to a `u64` that receives the count
    /// of values written.
    Materialize = 1,
}

/// Specification for a JIT pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PipelineSpec {
    pub encoding: JitEncoding,
    pub physical_type: JitPhysicalType,
    pub filter_op: JitFilterOp,
    pub aggregate_op: JitAggregateOp,
    pub output_mode: JitOutputMode,
}

/// Result of a JIT pipeline execution.
///
/// For Int128: result is `(value_i64, value_hi)` = (lo, hi).
/// For Int256: result is `(value_i64, value_hi, value_w2, value_w3)`.
/// Filter constants for Int128/Int256 are passed as *pointers* in
/// `filter_lo` / `filter_hi` (reinterpreted as `*const u8`).
#[repr(C)]
pub struct PipelineResult {
    pub count: u64,      // 0
    pub value_i64: i64,  // 8   (lo word for Int128/Int256)
    pub value_f64: f64,  // 16
    pub has_value: u64,  // 24
    pub value_hi: i64,   // 32  (hi word for Int128; w1 for Int256)
    pub value_w2: i64,   // 40  (Int256 w2)
    pub value_w3: i64,   // 48  (Int256 w3)
}

impl PipelineResult {
    pub fn new() -> Self {
        PipelineResult {
            count: 0, value_i64: 0, value_f64: 0.0, has_value: 0,
            value_hi: 0, value_w2: 0, value_w3: 0,
        }
    }
}

/// Generated pipeline function signature.
pub type FusedPipelineFn = unsafe extern "C" fn(
    values_data: *const u8,
    null_bitmap: *const u8,
    row_count: u32,
    filter_lo: i64,
    filter_hi: i64,
    result_ptr: *mut PipelineResult,
) -> i32;

/// A compiled pipeline function with its backing executable memory.
pub struct CompiledPipeline {
    _buffer: dynasmrt::ExecutableBuffer,
    fn_ptr: FusedPipelineFn,
    spec: PipelineSpec,
    code_size: usize,
}

unsafe impl Send for CompiledPipeline {}
unsafe impl Sync for CompiledPipeline {}

impl CompiledPipeline {
    pub fn fn_ptr(&self) -> FusedPipelineFn { self.fn_ptr }
    pub fn spec(&self) -> &PipelineSpec { &self.spec }
    pub fn code_size(&self) -> usize { self.code_size }
}

#[derive(Debug)]
pub enum JitError {
    UnsupportedCombination(String),
    AssemblerError(String),
}

impl std::fmt::Display for JitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JitError::UnsupportedCombination(m) => write!(f, "unsupported JIT combination: {m}"),
            JitError::AssemblerError(m) => write!(f, "JIT assembler error: {m}"),
        }
    }
}

// -----------------------------------------------------------------------
// Pipeline (Layer 2)
// -----------------------------------------------------------------------

/// Generate a compiled pipeline for the given specification.
pub fn generate_pipeline(spec: &PipelineSpec) -> Result<CompiledPipeline, JitError> {
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    return Err(JitError::UnsupportedCombination("unsupported architecture".into()));

    #[cfg(target_arch = "aarch64")]
    {
        generate_pipeline_ir(spec)
    }

    #[cfg(target_arch = "x86_64")]
    {
        generate_pipeline_codegen(spec)
    }
}

/// IR-based pipeline generation (aarch64).
#[cfg(target_arch = "aarch64")]
fn generate_pipeline_ir(spec: &PipelineSpec) -> Result<CompiledPipeline, JitError> {
    let mut builder = ir_builder::IrBuilder::new();
    encode::emit_ir(&mut builder, spec)?;
    let ir = builder.finish();
    backend::compile(&ir, spec)
}

/// Legacy codegen pipeline generation (x86_64 fallback).
#[cfg(target_arch = "x86_64")]
fn generate_pipeline_codegen(spec: &PipelineSpec) -> Result<CompiledPipeline, JitError> {
    let uses_fp = spec.physical_type.is_float();
    let is_materialize = spec.output_mode == JitOutputMode::Materialize;
    let is_dict = spec.encoding == JitEncoding::RleDictionary;
    let mut cg = codegen::Codegen::new()?;

    cg.emit_prologue(uses_fp);
    if uses_fp && !is_materialize && !is_dict {
        cg.init_fp_filters();
    }
    if uses_fp && !is_materialize {
        cg.init_fp_acc();
    }
    if is_materialize {
        cg.init_materialize();
    }

    encode::emit(&mut cg, spec)?;

    cg.emit_epilogue(spec);

    cg.finalize(spec)
}

/// Execute a compiled pipeline.
///
/// # Safety
/// - `values_data` must point to at least `row_count * type_size` readable bytes.
/// - `null_bitmap`, when non-null, must point to at least `row_count` readable bytes.
/// - `result` must be a valid mutable reference.
pub unsafe fn execute_pipeline(
    pipeline: &CompiledPipeline,
    values_data: *const u8,
    null_bitmap: *const u8,
    row_count: u32,
    filter_lo: i64,
    filter_hi: i64,
    result: &mut PipelineResult,
) -> i32 {
    (pipeline.fn_ptr)(values_data, null_bitmap, row_count, filter_lo, filter_hi, result as *mut _)
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn run(
        spec: PipelineSpec,
        values: &[u8],
        bitmap: Option<&[u8]>,
        rows: u32,
        flo: i64,
        fhi: i64,
    ) -> PipelineResult {
        let p = generate_pipeline(&spec).unwrap();
        let mut r = PipelineResult::new();
        let bp = bitmap.map(|b| b.as_ptr()).unwrap_or(std::ptr::null());
        unsafe { execute_pipeline(&p, values.as_ptr(), bp, rows, flo, fhi, &mut r); }
        r
    }

    fn oracle_int(
        vals: &[i64], nulls: &[bool], fop: JitFilterOp, flo: i64, fhi: i64, aop: JitAggregateOp,
    ) -> (u64, i64, u64) {
        let (mut c, mut a, mut h) = (0u64, 0i64, false);
        for (i, &v) in vals.iter().enumerate() {
            let is_null = !nulls[i];
            let pass = match fop {
                JitFilterOp::IsNull => is_null,
                _ if is_null => false,
                JitFilterOp::None | JitFilterOp::IsNotNull => true,
                JitFilterOp::Eq => v == flo, JitFilterOp::Ne => v != flo,
                JitFilterOp::Lt => v < flo, JitFilterOp::Le => v <= flo,
                JitFilterOp::Gt => v > flo, JitFilterOp::Ge => v >= flo,
                JitFilterOp::Between => v >= flo && v <= fhi,
            };
            if pass {
                if fop == JitFilterOp::IsNull { c += 1; continue; }
                match aop {
                    JitAggregateOp::Count => c += 1,
                    JitAggregateOp::Sum => { a += v; c += 1; h = true; }
                    JitAggregateOp::Min => { if !h || v < a { a = v; } c += 1; h = true; }
                    JitAggregateOp::Max => { if !h || v > a { a = v; } c += 1; h = true; }
                }
            }
        }
        (c, a, u64::from(h))
    }

    fn oracle_fp(
        vals: &[f64], nulls: &[bool], fop: JitFilterOp, flo: f64, fhi: f64, aop: JitAggregateOp,
    ) -> (u64, f64, u64) {
        let (mut c, mut a, mut h) = (0u64, 0.0f64, false);
        for (i, &v) in vals.iter().enumerate() {
            let is_null = !nulls[i];
            let pass = match fop {
                JitFilterOp::IsNull => is_null,
                _ if is_null => false,
                _ if v.is_nan() => false,
                JitFilterOp::None | JitFilterOp::IsNotNull => true,
                JitFilterOp::Eq => v == flo, JitFilterOp::Ne => v != flo,
                JitFilterOp::Lt => v < flo, JitFilterOp::Le => v <= flo,
                JitFilterOp::Gt => v > flo, JitFilterOp::Ge => v >= flo,
                JitFilterOp::Between => v >= flo && v <= fhi,
            };
            if pass {
                if fop == JitFilterOp::IsNull { c += 1; continue; }
                match aop {
                    JitAggregateOp::Count => c += 1,
                    JitAggregateOp::Sum => { a += v; c += 1; h = true; }
                    JitAggregateOp::Min => { if !h || v < a { a = v; } c += 1; h = true; }
                    JitAggregateOp::Max => { if !h || v > a { a = v; } c += 1; h = true; }
                }
            }
        }
        (c, a, u64::from(h))
    }

    fn i64b(v: &[i64]) -> Vec<u8> { v.iter().flat_map(|x| x.to_le_bytes()).collect() }
    fn i32b(v: &[i32]) -> Vec<u8> { v.iter().flat_map(|x| x.to_le_bytes()).collect() }
    fn f64b(v: &[f64]) -> Vec<u8> { v.iter().flat_map(|x| x.to_le_bytes()).collect() }
    fn f32b(v: &[f32]) -> Vec<u8> { v.iter().flat_map(|x| x.to_le_bytes()).collect() }

    // ---- smoke tests ----

    #[test] fn smoke_sum_i64() {
        let r = run(spec(JitPhysicalType::Int64, JitFilterOp::None, JitAggregateOp::Sum),
            &i64b(&[1,2,3,4,5]), None, 5, 0, 0);
        assert_eq!((r.count, r.value_i64, r.has_value), (5, 15, 1));
    }

    #[test] fn smoke_count_eq() {
        let r = run(spec(JitPhysicalType::Int64, JitFilterOp::Eq, JitAggregateOp::Count),
            &i64b(&[10,20,30,20,50]), None, 5, 20, 0);
        assert_eq!(r.count, 2);
    }

    #[test] fn smoke_min_nulls() {
        let r = run(spec(JitPhysicalType::Int64, JitFilterOp::None, JitAggregateOp::Min),
            &i64b(&[10,5,3,8,1]), Some(&[1,0,1,1,0]), 5, 0, 0);
        assert_eq!((r.count, r.value_i64), (3, 3));
    }

    #[test] fn smoke_max_i32_between() {
        let r = run(spec(JitPhysicalType::Int32, JitFilterOp::Between, JitAggregateOp::Max),
            &i32b(&[1,5,10,15,20,25]), None, 6, 5, 20);
        assert_eq!((r.count, r.value_i64), (4, 20));
    }

    #[test] fn smoke_is_null() {
        let r = run(spec(JitPhysicalType::Int64, JitFilterOp::IsNull, JitAggregateOp::Count),
            &i64b(&[1,2,3,4,5]), Some(&[1,0,0,1,0]), 5, 0, 0);
        assert_eq!(r.count, 3);
    }

    #[test] fn smoke_empty() {
        let r = run(spec(JitPhysicalType::Int64, JitFilterOp::None, JitAggregateOp::Sum),
            &[], None, 0, 0, 0);
        assert_eq!((r.count, r.has_value), (0, 0));
    }

    #[test] fn smoke_i32_sign_ext() {
        let r = run(spec(JitPhysicalType::Int32, JitFilterOp::None, JitAggregateOp::Min),
            &i32b(&[-1, i32::MIN, i32::MAX]), None, 3, 0, 0);
        assert_eq!(r.value_i64, i32::MIN as i64);
    }

    // ---- float smoke tests ----

    #[test] fn smoke_sum_f64() {
        let vals = [1.0f64, 2.0, 3.0, 4.0, 5.0];
        let r = run(spec(JitPhysicalType::Double, JitFilterOp::None, JitAggregateOp::Sum),
            &f64b(&vals), None, 5, 0, 0);
        assert_eq!(r.count, 5);
        assert!((r.value_f64 - 15.0).abs() < 1e-10);
    }

    #[test] fn smoke_min_f32() {
        let vals = [3.0f32, 1.5, 2.0, 0.5, 4.0];
        let flo = 0.0f64.to_bits() as i64;
        let r = run(spec(JitPhysicalType::Float, JitFilterOp::None, JitAggregateOp::Min),
            &f32b(&vals), None, 5, flo, 0);
        assert_eq!(r.count, 5);
        assert!((r.value_f64 - 0.5).abs() < 1e-10);
    }

    #[test] fn smoke_max_f64_with_filter() {
        let vals = [1.0f64, 5.0, 3.0, 7.0, 2.0];
        let flo = 3.0f64.to_bits() as i64;
        let r = run(spec(JitPhysicalType::Double, JitFilterOp::Gt, JitAggregateOp::Max),
            &f64b(&vals), None, 5, flo, 0);
        assert_eq!(r.count, 2); // 5.0, 7.0
        assert!((r.value_f64 - 7.0).abs() < 1e-10);
    }

    #[test] fn smoke_f64_nan_rejected() {
        let vals = [1.0f64, f64::NAN, 3.0, f64::NAN, 5.0];
        let flo = 0.0f64.to_bits() as i64;
        let r = run(spec(JitPhysicalType::Double, JitFilterOp::Gt, JitAggregateOp::Sum),
            &f64b(&vals), None, 5, flo, 0);
        assert_eq!(r.count, 3); // 1.0, 3.0, 5.0 (NaN rejected)
        assert!((r.value_f64 - 9.0).abs() < 1e-10);
    }

    #[test] fn smoke_f64_between() {
        let vals = [1.0f64, 2.5, 3.0, 4.5, 6.0];
        let flo = 2.0f64.to_bits() as i64;
        let fhi = 4.0f64.to_bits() as i64;
        let r = run(spec(JitPhysicalType::Double, JitFilterOp::Between, JitAggregateOp::Count),
            &f64b(&vals), None, 5, flo, fhi);
        assert_eq!(r.count, 2); // 2.5, 3.0
    }

    // ---- comprehensive oracle tests ----

    #[test] fn test_all_i128() {
        // 10 values mixing small, large, negative, and cross-word boundaries.
        let vals: Vec<i128> = vec![
            1, 2, 3, 4, 5, -1, -2, 0, 100, -100,
        ];
        run_all_int128(&vals);
    }

    #[test] fn test_all_i128_wide() {
        // Values that exercise the high word.
        let big: i128 = (1i128 << 64) + 42;
        let vals: Vec<i128> = vec![
            0, 1, big, big + 1, -big, -1, i128::MAX, i128::MIN, 100, -100,
        ];
        run_all_int128(&vals);
    }

    #[test] fn test_all_i256() {
        // 10 values as [w0,w1,w2,w3] (little-endian word order, w3 signed).
        let vals: Vec<[u64; 4]> = vec![
            [1, 0, 0, 0],                                   // 1
            [2, 0, 0, 0],                                   // 2
            [3, 0, 0, 0],                                   // 3
            [4, 0, 0, 0],                                   // 4
            [5, 0, 0, 0],                                   // 5
            [u64::MAX, u64::MAX, u64::MAX, u64::MAX],       // -1
            [u64::MAX - 1, u64::MAX, u64::MAX, u64::MAX],   // -2
            [0, 0, 0, 0],                                   // 0
            [100, 0, 0, 0],                                 // 100
            [u64::MAX - 99, u64::MAX, u64::MAX, u64::MAX],  // -100
        ];
        run_all_int256(&vals);
    }

    #[test] fn test_all_i64() { run_all_int(JitPhysicalType::Int64, &[1,2,3,4,5,-1,-2,0,100,-100]); }
    #[test] fn test_all_i32() {
        let v32: Vec<i32> = [1,2,3,4,5,-1,-2,0,100,-100].into();
        let bytes = i32b(&v32);
        let v64: Vec<i64> = v32.iter().map(|&x| x as i64).collect();
        run_all_int_bytes(JitPhysicalType::Int32, &bytes, &v64);
    }
    #[test] fn test_all_i16() {
        let v16: Vec<i16> = vec![1,2,3,4,5,-1,-2,0,100,-100];
        let bytes: Vec<u8> = v16.iter().flat_map(|x| x.to_le_bytes()).collect();
        let v64: Vec<i64> = v16.iter().map(|&x| x as i64).collect();
        run_all_int_bytes(JitPhysicalType::Int16, &bytes, &v64);
    }
    #[test] fn test_all_i8() {
        let v8: Vec<i8> = vec![1,2,3,4,5,-1,-2,0,100,-100];
        let bytes: Vec<u8> = v8.iter().map(|x| *x as u8).collect();
        let v64: Vec<i64> = v8.iter().map(|&x| x as i64).collect();
        run_all_int_bytes(JitPhysicalType::Int8, &bytes, &v64);
    }

    #[test] fn test_all_f64() { run_all_fp(JitPhysicalType::Double, &[1.0,2.0,3.0,4.0,5.0,-1.0,-2.0,0.0,100.0,-100.0]); }
    #[test] fn test_all_f32() {
        let v32: Vec<f32> = vec![1.0,2.0,3.0,4.0,5.0,-1.0,-2.0,0.0,100.0,-100.0];
        let bytes = f32b(&v32);
        let v64: Vec<f64> = v32.iter().map(|&x| x as f64).collect();
        run_all_fp_bytes(JitPhysicalType::Float, &bytes, &v64);
    }

    #[test] fn test_registry() {
        let s = spec(JitPhysicalType::Int64, JitFilterOp::None, JitAggregateOp::Count);
        let p1 = REGISTRY.get_or_generate(&s).unwrap();
        let p2 = REGISTRY.get_or_generate(&s).unwrap();
        assert!(std::sync::Arc::ptr_eq(&p1, &p2));
    }

    // ---- dict_filter tests ----

    #[test] fn test_dict_filter_sum_i64() {
        // 16-entry i64 dictionary.
        let dict: Vec<i64> = vec![5,10,15,20,30,40,50,60,70,80,90,100,110,120,130,140];
        let dict_bytes: Vec<u8> = dict.iter().flat_map(|v| v.to_le_bytes()).collect();
        // match_bitmap: entries > 60 → indices 8..15 match.
        let match_bm: Vec<u8> = dict.iter().map(|&v| if v > 60 { 1 } else { 0 }).collect();
        // 10 rows cycling through dict indices 0..9.
        let indices: Vec<u8> = (0u32..10).flat_map(|i| i.to_le_bytes()).collect();

        let pipeline = generate_pipeline(&PipelineSpec {
            encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::Gt, // filter semantics baked into match_bm
            aggregate_op: JitAggregateOp::Sum, output_mode: JitOutputMode::Aggregate,
        }).unwrap();
        let mut result = PipelineResult::new();
        unsafe {
            (pipeline.fn_ptr())(
                indices.as_ptr(),
                std::ptr::null(),
                10,
                match_bm.as_ptr() as i64,    // filter_lo = match_bitmap
                dict_bytes.as_ptr() as i64,   // filter_hi = dict_values
                &mut result as *mut _,
            );
        }
        // Matching indices: 8(70),9(80) → sum=150, count=2.
        assert_eq!(result.count, 2);
        assert_eq!(result.value_i64, 150);
    }

    #[test] fn test_dict_filter_with_nulls() {
        let dict: Vec<i64> = vec![100, 200, 300];
        let dict_bytes: Vec<u8> = dict.iter().flat_map(|v| v.to_le_bytes()).collect();
        let match_bm: Vec<u8> = vec![1, 0, 1]; // entries 0 and 2 match
        let indices: Vec<u8> = [0u32,1,2,0,1,2].iter().flat_map(|i| i.to_le_bytes()).collect();
        let bitmap = [1u8, 1, 1, 0, 1, 0]; // rows 3 and 5 are null

        let pipeline = generate_pipeline(&PipelineSpec {
            encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Sum, output_mode: JitOutputMode::Aggregate,
        }).unwrap();
        let mut result = PipelineResult::new();
        unsafe {
            (pipeline.fn_ptr())(
                indices.as_ptr(),
                bitmap.as_ptr(),
                6,
                match_bm.as_ptr() as i64,
                dict_bytes.as_ptr() as i64,
                &mut result as *mut _,
            );
        }
        // Non-null rows: 0(idx=0,match),1(idx=1,no),2(idx=2,match),4(idx=1,no)
        // Matching non-null: row 0 (100), row 2 (300) → sum=400, count=2.
        assert_eq!(result.count, 2);
        assert_eq!(result.value_i64, 400);
    }

    #[test] fn test_dict_filter_f64() {
        let dict: Vec<f64> = vec![1.5, 2.5, 3.5, 4.5];
        let dict_bytes: Vec<u8> = dict.iter().flat_map(|v| v.to_le_bytes()).collect();
        let match_bm: Vec<u8> = vec![0, 1, 1, 0]; // entries 1,2 match
        let indices: Vec<u8> = [0u32,1,2,3,1,2].iter().flat_map(|i| i.to_le_bytes()).collect();

        let pipeline = generate_pipeline(&PipelineSpec {
            encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Double,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Sum, output_mode: JitOutputMode::Aggregate,
        }).unwrap();
        let mut result = PipelineResult::new();
        unsafe {
            (pipeline.fn_ptr())(
                indices.as_ptr(),
                std::ptr::null(),
                6,
                match_bm.as_ptr() as i64,
                dict_bytes.as_ptr() as i64,
                &mut result as *mut _,
            );
        }
        // Matching: row1(2.5),row2(3.5),row4(2.5),row5(3.5) → sum=12.0
        assert_eq!(result.count, 4);
        assert!((result.value_f64 - 12.0).abs() < 1e-10);
    }

    #[test] fn test_dict_filter_min_max() {
        let dict: Vec<i64> = vec![10, 50, 30, 20, 40];
        let dict_bytes: Vec<u8> = dict.iter().flat_map(|v| v.to_le_bytes()).collect();
        let match_bm: Vec<u8> = vec![1, 1, 1, 1, 1]; // all match
        let indices: Vec<u8> = [0u32,1,2,3,4].iter().flat_map(|i| i.to_le_bytes()).collect();

        let p_min = generate_pipeline(&PipelineSpec {
            encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Min, output_mode: JitOutputMode::Aggregate,
        }).unwrap();
        let p_max = generate_pipeline(&PipelineSpec {
            encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Max, output_mode: JitOutputMode::Aggregate,
        }).unwrap();

        let mut r = PipelineResult::new();
        unsafe {
            (p_min.fn_ptr())(indices.as_ptr(), std::ptr::null(), 5,
                match_bm.as_ptr() as i64, dict_bytes.as_ptr() as i64, &mut r as *mut _);
        }
        assert_eq!(r.value_i64, 10);

        r = PipelineResult::new();
        unsafe {
            (p_max.fn_ptr())(indices.as_ptr(), std::ptr::null(), 5,
                match_bm.as_ptr() as i64, dict_bytes.as_ptr() as i64, &mut r as *mut _);
        }
        assert_eq!(r.value_i64, 50);
    }

    // ---- code size report ----

    #[test] fn report_code_sizes() {
        let cases: Vec<(&str, PipelineSpec)> = vec![
            ("plain i8 None/Count",     PipelineSpec { encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int8,   filter_op: JitFilterOp::None,    aggregate_op: JitAggregateOp::Count, output_mode: JitOutputMode::Aggregate }),
            ("plain i64 Gt/Sum",        PipelineSpec { encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int64,  filter_op: JitFilterOp::Gt,      aggregate_op: JitAggregateOp::Sum,   output_mode: JitOutputMode::Aggregate }),
            ("plain f64 Between/Min",   PipelineSpec { encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Double, filter_op: JitFilterOp::Between, aggregate_op: JitAggregateOp::Min,   output_mode: JitOutputMode::Aggregate }),
            ("plain i64 Gt/Materialize",PipelineSpec { encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int64,  filter_op: JitFilterOp::Gt,      aggregate_op: JitAggregateOp::Count, output_mode: JitOutputMode::Materialize }),
            ("dict  i64 Gt/Sum",        PipelineSpec { encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Int64, filter_op: JitFilterOp::Gt, aggregate_op: JitAggregateOp::Sum, output_mode: JitOutputMode::Aggregate }),
            ("dict  f64 None/Sum",      PipelineSpec { encoding: JitEncoding::RleDictionary, physical_type: JitPhysicalType::Double, filter_op: JitFilterOp::None, aggregate_op: JitAggregateOp::Sum, output_mode: JitOutputMode::Aggregate }),
            ("plain i128 Eq/Max",       PipelineSpec { encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int128, filter_op: JitFilterOp::Eq,      aggregate_op: JitAggregateOp::Max,   output_mode: JitOutputMode::Aggregate }),
            ("plain i256 Between/Max",  PipelineSpec { encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int256, filter_op: JitFilterOp::Between, aggregate_op: JitAggregateOp::Max,   output_mode: JitOutputMode::Aggregate }),
            ("plain i64 IsNull/Count",  PipelineSpec { encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int64,  filter_op: JitFilterOp::IsNull,  aggregate_op: JitAggregateOp::Count, output_mode: JitOutputMode::Aggregate }),
        ];
        eprintln!("\n{:<30} {:>6}", "Pipeline", "Bytes");
        eprintln!("{:-<30} {:->6}", "", "");
        for (name, spec) in &cases {
            let p = generate_pipeline(spec).unwrap();
            eprintln!("{:<30} {:>6}", name, p.code_size());
        }
        eprintln!();
    }

    // ---- encoding acceptance ----

    #[test] fn test_rle_dict_generates() {
        let s = PipelineSpec {
            encoding: JitEncoding::RleDictionary,
            physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Sum, output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_pipeline(&s).is_ok());
    }

    #[test] fn test_delta_bp_generates() {
        let s = PipelineSpec {
            encoding: JitEncoding::DeltaBinaryPacked, physical_type: JitPhysicalType::Int32,
            filter_op: JitFilterOp::Eq, aggregate_op: JitAggregateOp::Count,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_pipeline(&s).is_ok());
    }

    #[test] fn test_bss_generates() {
        let s = PipelineSpec {
            encoding: JitEncoding::ByteStreamSplit, physical_type: JitPhysicalType::Double,
            filter_op: JitFilterOp::Lt, aggregate_op: JitAggregateOp::Min,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_pipeline(&s).is_ok());
    }

    #[test] fn test_delta_len_generates() {
        let s = PipelineSpec {
            encoding: JitEncoding::DeltaLengthByteArray, physical_type: JitPhysicalType::Int64,
            filter_op: JitFilterOp::None, aggregate_op: JitAggregateOp::Count,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_pipeline(&s).is_ok());
    }

    // ---- helpers ----

    #[test] fn test_i128_eq_count() {
        let vals: Vec<i128> = vec![1, 2, 3, 4, 5];
        let bytes = i128_vals_to_bytes(&vals);
        let flo_val: i128 = 3;
        let flo_w = i128_to_words(flo_val);
        let flo_ptr = flo_w.as_ptr() as i64;
        let s = PipelineSpec {
            encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int128,
            filter_op: JitFilterOp::Eq, aggregate_op: JitAggregateOp::Count,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(s, &bytes, None, 5, flo_ptr, 0);
        assert_eq!(r.count, 1, "i128 Eq/Count: expected 1 match for value 3");
    }

    #[test] fn test_i128_eq_sum() {
        // Minimal: 2 values, one matching. flo=1, values=[1, 2].
        let vals: Vec<i128> = vec![1, 2];
        let bytes = i128_vals_to_bytes(&vals);
        let flo_val: i128 = 1;
        let flo_w = i128_to_words(flo_val);
        let flo_ptr = flo_w.as_ptr() as i64;
        let s = PipelineSpec {
            encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int128,
            filter_op: JitFilterOp::Eq, aggregate_op: JitAggregateOp::Sum,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(s, &bytes, None, 2, flo_ptr, 0);
        assert_eq!(r.count, 1, "i128 Eq/Sum count: expected 1 (value 1 == 1)");
    }

    fn spec(pt: JitPhysicalType, fop: JitFilterOp, aop: JitAggregateOp) -> PipelineSpec {
        PipelineSpec { encoding: JitEncoding::Plain, physical_type: pt, filter_op: fop, aggregate_op: aop, output_mode: JitOutputMode::Aggregate }
    }

    fn run_all_int(pt: JitPhysicalType, vals: &[i64]) {
        let bytes = i64b(vals);
        run_all_int_bytes(pt, &bytes, vals);
    }

    fn run_all_int_bytes(pt: JitPhysicalType, bytes: &[u8], vals: &[i64]) {
        let n = vals.len();
        let all_nn: Vec<bool> = vec![true; n];
        let mix_bm = vec![1u8,0,1,0,1,1,0,1,1,0];
        let mix_fl: Vec<bool> = mix_bm.iter().map(|&b| b != 0).collect();
        let (flo, fhi) = (3i64, 100i64);

        for &fop in FILTER_OPS { for &aop in AGG_OPS {
            let s = PipelineSpec { encoding: JitEncoding::Plain, physical_type: pt, filter_op: fop, aggregate_op: aop, output_mode: JitOutputMode::Aggregate };
            // no-nulls
            let (ec,ev,eh) = oracle_int(vals, &all_nn, fop, flo, fhi, aop);
            let r = run(s, bytes, None, n as u32, flo, fhi);
            assert_eq!(r.count, ec, "{fop:?}/{aop:?} nn count");
            if eh > 0 { assert_eq!(r.value_i64, ev, "{fop:?}/{aop:?} nn val"); }
            // mixed-nulls
            let (ec,ev,eh) = oracle_int(vals, &mix_fl, fop, flo, fhi, aop);
            let r = run(s, bytes, Some(&mix_bm), n as u32, flo, fhi);
            assert_eq!(r.count, ec, "{fop:?}/{aop:?} mx count");
            if eh > 0 { assert_eq!(r.value_i64, ev, "{fop:?}/{aop:?} mx val"); }
        }}
    }

    fn run_all_fp(pt: JitPhysicalType, vals: &[f64]) {
        let bytes = f64b(vals);
        run_all_fp_bytes(pt, &bytes, vals);
    }

    fn run_all_fp_bytes(pt: JitPhysicalType, bytes: &[u8], vals: &[f64]) {
        let n = vals.len();
        let all_nn: Vec<bool> = vec![true; n];
        let mix_bm = vec![1u8,0,1,0,1,1,0,1,1,0];
        let mix_fl: Vec<bool> = mix_bm.iter().map(|&b| b != 0).collect();
        let (flo_f, fhi_f) = (3.0f64, 100.0f64);
        let flo = flo_f.to_bits() as i64;
        let fhi = fhi_f.to_bits() as i64;

        for &fop in FILTER_OPS { for &aop in AGG_OPS {
            let s = PipelineSpec { encoding: JitEncoding::Plain, physical_type: pt, filter_op: fop, aggregate_op: aop, output_mode: JitOutputMode::Aggregate };
            // no-nulls
            let (ec, ev, eh) = oracle_fp(vals, &all_nn, fop, flo_f, fhi_f, aop);
            let r = run(s, bytes, None, n as u32, flo, fhi);
            assert_eq!(r.count, ec, "{fop:?}/{aop:?} nn count");
            if eh > 0 { assert!((r.value_f64 - ev).abs() < 1e-10, "{fop:?}/{aop:?} nn val: {} vs {}", r.value_f64, ev); }
            // mixed-nulls
            let (ec, ev, eh) = oracle_fp(vals, &mix_fl, fop, flo_f, fhi_f, aop);
            let r = run(s, bytes, Some(&mix_bm), n as u32, flo, fhi);
            assert_eq!(r.count, ec, "{fop:?}/{aop:?} mx count");
            if eh > 0 { assert!((r.value_f64 - ev).abs() < 1e-10, "{fop:?}/{aop:?} mx val: {} vs {}", r.value_f64, ev); }
        }}
    }

    // ---- Int128 oracle + runner ----

    fn i128_to_words(v: i128) -> [u64; 2] {
        let b = v.to_le_bytes();
        [u64::from_le_bytes(b[0..8].try_into().unwrap()),
         u64::from_le_bytes(b[8..16].try_into().unwrap())]
    }

    fn words_to_i128(w: [u64; 2]) -> i128 {
        i128::from_le_bytes({
            let mut b = [0u8; 16];
            b[0..8].copy_from_slice(&w[0].to_le_bytes());
            b[8..16].copy_from_slice(&w[1].to_le_bytes());
            b
        })
    }

    fn i128_vals_to_bytes(vals: &[i128]) -> Vec<u8> {
        vals.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    fn oracle_i128(
        vals: &[i128], nulls: &[bool], fop: JitFilterOp, flo: i128, fhi: i128, aop: JitAggregateOp,
    ) -> (u64, i128, u64) {
        let (mut c, mut a, mut h) = (0u64, 0i128, false);
        for (i, &v) in vals.iter().enumerate() {
            let is_null = !nulls[i];
            let pass = match fop {
                JitFilterOp::IsNull => is_null,
                _ if is_null => false,
                JitFilterOp::None | JitFilterOp::IsNotNull => true,
                JitFilterOp::Eq => v == flo, JitFilterOp::Ne => v != flo,
                JitFilterOp::Lt => v < flo, JitFilterOp::Le => v <= flo,
                JitFilterOp::Gt => v > flo, JitFilterOp::Ge => v >= flo,
                JitFilterOp::Between => v >= flo && v <= fhi,
            };
            if pass {
                if fop == JitFilterOp::IsNull { c += 1; continue; }
                match aop {
                    JitAggregateOp::Count => c += 1,
                    JitAggregateOp::Sum => { a = a.wrapping_add(v); c += 1; h = true; }
                    JitAggregateOp::Min => { if !h || v < a { a = v; } c += 1; h = true; }
                    JitAggregateOp::Max => { if !h || v > a { a = v; } c += 1; h = true; }
                }
            }
        }
        (c, a, u64::from(h))
    }

    fn run_all_int128(vals: &[i128]) {
        let bytes = i128_vals_to_bytes(vals);
        let n = vals.len();
        let all_nn: Vec<bool> = vec![true; n];
        let mix_bm: Vec<u8> = vec![1,0,1,0,1,1,0,1,1,0];
        let mix_fl: Vec<bool> = mix_bm.iter().map(|&b| b != 0).collect();
        let flo_val: i128 = 3;
        let fhi_val: i128 = 100;
        let flo_w = i128_to_words(flo_val);
        let fhi_w = i128_to_words(fhi_val);

        for &fop in FILTER_OPS { for &aop in AGG_OPS {
            let s = PipelineSpec {
                encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int128,
                filter_op: fop, aggregate_op: aop, output_mode: JitOutputMode::Aggregate,
            };
            let flo_ptr = flo_w.as_ptr() as i64;
            let fhi_ptr = fhi_w.as_ptr() as i64;

            // no-nulls
            let (ec, ev, eh) = oracle_i128(vals, &all_nn, fop, flo_val, fhi_val, aop);
            let r = run(s, &bytes, None, n as u32, flo_ptr, fhi_ptr);
            assert_eq!(r.count, ec, "i128 {fop:?}/{aop:?} nn count");
            if eh > 0 {
                let got = words_to_i128([r.value_i64 as u64, r.value_hi as u64]);
                assert_eq!(got, ev, "i128 {fop:?}/{aop:?} nn val");
            }
            assert_eq!(r.has_value, eh, "i128 {fop:?}/{aop:?} nn has");

            // mixed-nulls
            let (ec, ev, eh) = oracle_i128(vals, &mix_fl, fop, flo_val, fhi_val, aop);
            let r = run(s, &bytes, Some(&mix_bm), n as u32, flo_ptr, fhi_ptr);
            assert_eq!(r.count, ec, "i128 {fop:?}/{aop:?} mx count");
            if eh > 0 {
                let got = words_to_i128([r.value_i64 as u64, r.value_hi as u64]);
                assert_eq!(got, ev, "i128 {fop:?}/{aop:?} mx val");
            }
            assert_eq!(r.has_value, eh, "i128 {fop:?}/{aop:?} mx has");
        }}
    }

    // ---- Int256 oracle + runner ----

    fn i256_cmp(a: [u64; 4], b: [u64; 4]) -> std::cmp::Ordering {
        // w3 is most-significant, signed; w2..w0 are unsigned.
        match (a[3] as i64).cmp(&(b[3] as i64)) {
            std::cmp::Ordering::Equal =>
                match a[2].cmp(&b[2]) {
                    std::cmp::Ordering::Equal =>
                        match a[1].cmp(&b[1]) {
                            std::cmp::Ordering::Equal => a[0].cmp(&b[0]),
                            o => o,
                        },
                    o => o,
                },
            o => o,
        }
    }

    fn i256_add(a: [u64; 4], b: [u64; 4]) -> [u64; 4] {
        let (w0, c0) = a[0].overflowing_add(b[0]);
        let (w1a, c1a) = a[1].overflowing_add(b[1]);
        let (w1, c1b) = w1a.overflowing_add(c0 as u64);
        let (w2a, c2a) = a[2].overflowing_add(b[2]);
        let (w2, c2b) = w2a.overflowing_add((c1a || c1b) as u64);
        let w3 = a[3].wrapping_add(b[3]).wrapping_add((c2a || c2b) as u64);
        [w0, w1, w2, w3]
    }

    fn i256_vals_to_bytes(vals: &[[u64; 4]]) -> Vec<u8> {
        vals.iter().flat_map(|v| {
            v.iter().flat_map(|w| w.to_le_bytes()).collect::<Vec<u8>>()
        }).collect()
    }

    fn oracle_i256(
        vals: &[[u64; 4]], nulls: &[bool], fop: JitFilterOp,
        flo: [u64; 4], fhi: [u64; 4], aop: JitAggregateOp,
    ) -> (u64, [u64; 4], u64) {
        let (mut c, mut a, mut h) = (0u64, [0u64; 4], false);
        for (i, &v) in vals.iter().enumerate() {
            let is_null = !nulls[i];
            let pass = match fop {
                JitFilterOp::IsNull => is_null,
                _ if is_null => false,
                JitFilterOp::None | JitFilterOp::IsNotNull => true,
                JitFilterOp::Eq => v == flo, JitFilterOp::Ne => v != flo,
                JitFilterOp::Lt => i256_cmp(v, flo).is_lt(),
                JitFilterOp::Le => i256_cmp(v, flo).is_le(),
                JitFilterOp::Gt => i256_cmp(v, flo).is_gt(),
                JitFilterOp::Ge => i256_cmp(v, flo).is_ge(),
                JitFilterOp::Between => i256_cmp(v, flo).is_ge() && i256_cmp(v, fhi).is_le(),
            };
            if pass {
                if fop == JitFilterOp::IsNull { c += 1; continue; }
                match aop {
                    JitAggregateOp::Count => c += 1,
                    JitAggregateOp::Sum => { a = i256_add(a, v); c += 1; h = true; }
                    JitAggregateOp::Min => { if !h || i256_cmp(v, a).is_lt() { a = v; } c += 1; h = true; }
                    JitAggregateOp::Max => { if !h || i256_cmp(v, a).is_gt() { a = v; } c += 1; h = true; }
                }
            }
        }
        (c, a, u64::from(h))
    }

    fn run_all_int256(vals: &[[u64; 4]]) {
        let bytes = i256_vals_to_bytes(vals);
        let n = vals.len();
        let all_nn: Vec<bool> = vec![true; n];
        let mix_bm: Vec<u8> = vec![1,0,1,0,1,1,0,1,1,0];
        let mix_fl: Vec<bool> = mix_bm.iter().map(|&b| b != 0).collect();
        let flo: [u64; 4] = [3, 0, 0, 0];
        let fhi: [u64; 4] = [100, 0, 0, 0];

        for &fop in FILTER_OPS { for &aop in AGG_OPS {
            let s = PipelineSpec {
                encoding: JitEncoding::Plain, physical_type: JitPhysicalType::Int256,
                filter_op: fop, aggregate_op: aop, output_mode: JitOutputMode::Aggregate,
            };
            let flo_ptr = flo.as_ptr() as i64;
            let fhi_ptr = fhi.as_ptr() as i64;

            // no-nulls
            let (ec, ev, eh) = oracle_i256(vals, &all_nn, fop, flo, fhi, aop);
            let r = run(s, &bytes, None, n as u32, flo_ptr, fhi_ptr);
            assert_eq!(r.count, ec, "i256 {fop:?}/{aop:?} nn count");
            if eh > 0 {
                let got = [r.value_i64 as u64, r.value_hi as u64, r.value_w2 as u64, r.value_w3 as u64];
                assert_eq!(got, ev, "i256 {fop:?}/{aop:?} nn val");
            }
            assert_eq!(r.has_value, eh, "i256 {fop:?}/{aop:?} nn has");

            // mixed-nulls
            let (ec, ev, eh) = oracle_i256(vals, &mix_fl, fop, flo, fhi, aop);
            let r = run(s, &bytes, Some(&mix_bm), n as u32, flo_ptr, fhi_ptr);
            assert_eq!(r.count, ec, "i256 {fop:?}/{aop:?} mx count");
            if eh > 0 {
                let got = [r.value_i64 as u64, r.value_hi as u64, r.value_w2 as u64, r.value_w3 as u64];
                assert_eq!(got, ev, "i256 {fop:?}/{aop:?} mx val");
            }
            assert_eq!(r.has_value, eh, "i256 {fop:?}/{aop:?} mx has");
        }}
    }

    const FILTER_OPS: &[JitFilterOp] = &[
        JitFilterOp::None, JitFilterOp::Eq, JitFilterOp::Ne,
        JitFilterOp::Lt, JitFilterOp::Le, JitFilterOp::Gt, JitFilterOp::Ge,
        JitFilterOp::Between, JitFilterOp::IsNull, JitFilterOp::IsNotNull,
    ];
    const AGG_OPS: &[JitAggregateOp] = &[
        JitAggregateOp::Count, JitAggregateOp::Sum,
        JitAggregateOp::Min, JitAggregateOp::Max,
    ];
}
