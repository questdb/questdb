//! Multi-column JIT pipeline support.
//!
//! Extends the single-column pipeline to handle operations across multiple
//! columns: multi-column filters, cross-column aggregation, and multi-column
//! materialization.
//!
//! The JIT function receives a single pointer to `MultiPipelineContext` which
//! contains row count, result pointer, and per-column data (value arrays, null
//! bitmaps, filter constants). The generated code loads context fields in the
//! entry block and assigns critical pointers to callee-saved registers for the
//! loop body.

use std::mem::offset_of;

use super::ir_builder::IrBuilder;
use super::{
    JitAggregateOp, JitEncoding, JitError, JitFilterOp, JitOutputMode, JitPhysicalType,
    PipelineSpec,
};

// Compile-time assertions that our offset constants match the struct layout.
const _: () = assert!(offset_of!(MultiPipelineContext, row_count) == CTX_ROW_COUNT as usize);
const _: () = assert!(offset_of!(MultiPipelineContext, result_ptr) == CTX_RESULT_PTR as usize);
const _: () = assert!(offset_of!(MultiPipelineContext, columns) == CTX_COLUMNS as usize);
const _: () = assert!(offset_of!(ColumnSlot, values_data) == SLOT_VALUES_DATA as usize);
const _: () = assert!(offset_of!(ColumnSlot, null_bitmap) == SLOT_NULL_BITMAP as usize);
const _: () = assert!(offset_of!(ColumnSlot, filter_lo) == SLOT_FILTER_LO as usize);
const _: () = assert!(offset_of!(ColumnSlot, filter_hi) == SLOT_FILTER_HI as usize);
const _: () = assert!(size_of::<ColumnSlot>() == COLUMN_SLOT_SIZE as usize);
use std::mem::size_of;

// -----------------------------------------------------------------------
// Constants
// -----------------------------------------------------------------------

pub const MAX_PIPELINE_COLUMNS: usize = 8;

// Context struct field offsets.
pub const CTX_ROW_COUNT: u16 = 0;
pub const CTX_RESULT_PTR: u16 = 8;
pub const CTX_COLUMNS: u16 = 16;
pub const COLUMN_SLOT_SIZE: u16 = 32;

// ColumnSlot field offsets (relative to column base).
pub const SLOT_VALUES_DATA: u16 = 0;
pub const SLOT_NULL_BITMAP: u16 = 8;
pub const SLOT_FILTER_LO: u16 = 16;
pub const SLOT_FILTER_HI: u16 = 24;

/// Byte offset of a column slot field within `MultiPipelineContext`.
///
/// Panics in debug builds if `col_idx >= MAX_PIPELINE_COLUMNS`.
pub const fn col_offset(col_idx: usize, field: u16) -> u16 {
    assert!(col_idx < MAX_PIPELINE_COLUMNS, "col_idx out of bounds");
    CTX_COLUMNS + (col_idx as u16) * COLUMN_SLOT_SIZE + field
}

// -----------------------------------------------------------------------
// Context struct (passed to JIT function)
// -----------------------------------------------------------------------

/// Per-column data slot in the multi-column context.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ColumnSlot {
    /// Pointer to the flat value array for this column.
    pub values_data: *const u8,
    /// Pointer to the null bitmap (1 byte per row), or null if no nulls.
    pub null_bitmap: *const u8,
    /// Filter constant (low). For Materialize columns, repurposed as output
    /// buffer pointer.
    pub filter_lo: i64,
    /// Filter constant (high). For Between filter.
    pub filter_hi: i64,
}

/// Context struct passed to multi-column JIT functions.
///
/// All fields are 8-byte aligned. `row_count` is u64 (not u32) to allow
/// simple 64-bit loads in the generated code.
#[repr(C)]
pub struct MultiPipelineContext {
    pub row_count: u64,
    pub result_ptr: *mut u8,
    pub columns: [ColumnSlot; MAX_PIPELINE_COLUMNS],
}

/// Multi-column JIT function signature.
pub type MultiPipelineFn = unsafe extern "C" fn(ctx: *const MultiPipelineContext) -> i32;

// -----------------------------------------------------------------------
// Page-level context structs (for decode_row_group_jit)
// -----------------------------------------------------------------------

/// Per-column data slot for page-level JIT kernels.
///
/// Unlike `ColumnSlot`, this has no `null_bitmap` field — the Rust driver
/// handles def_levels directly and only calls the JIT kernel for non-null rows.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PageColumnSlot {
    /// Pointer into `values_buffer`, offset to the current batch of non-null rows.
    pub values_data: *const u8,
    /// Filter constant (low). For Materialize columns, repurposed as output buffer pointer.
    pub filter_lo: i64,
    /// Filter constant (high). For Between filter.
    pub filter_hi: i64,
}

/// Page-level context field offsets.
pub const PAGE_CTX_ROW_COUNT: u16 = 0;
pub const PAGE_CTX_RESULT_PTR: u16 = 8;
pub const PAGE_CTX_DEF_BITMAP: u16 = 16;
pub const PAGE_CTX_COLUMNS: u16 = 24;
pub const PAGE_COLUMN_SLOT_SIZE: u16 = 24;

/// Page column slot field offsets (relative to column base).
pub const PAGE_SLOT_VALUES_DATA: u16 = 0;
pub const PAGE_SLOT_FILTER_LO: u16 = 8;
pub const PAGE_SLOT_FILTER_HI: u16 = 16;

// Compile-time assertions for PageColumnSlot layout.
const _: () = assert!(offset_of!(MultiPageContext, row_count) == PAGE_CTX_ROW_COUNT as usize);
const _: () = assert!(offset_of!(MultiPageContext, result_ptr) == PAGE_CTX_RESULT_PTR as usize);
const _: () = assert!(offset_of!(MultiPageContext, def_bitmap) == PAGE_CTX_DEF_BITMAP as usize);
const _: () = assert!(offset_of!(MultiPageContext, columns) == PAGE_CTX_COLUMNS as usize);
const _: () = assert!(offset_of!(PageColumnSlot, values_data) == PAGE_SLOT_VALUES_DATA as usize);
const _: () = assert!(offset_of!(PageColumnSlot, filter_lo) == PAGE_SLOT_FILTER_LO as usize);
const _: () = assert!(offset_of!(PageColumnSlot, filter_hi) == PAGE_SLOT_FILTER_HI as usize);
const _: () = assert!(size_of::<PageColumnSlot>() == PAGE_COLUMN_SLOT_SIZE as usize);

/// Byte offset of a page column slot field within `MultiPageContext`.
pub const fn page_col_offset(col_idx: usize, field: u16) -> u16 {
    assert!(col_idx < MAX_PIPELINE_COLUMNS, "col_idx out of bounds");
    PAGE_CTX_COLUMNS + (col_idx as u16) * PAGE_COLUMN_SLOT_SIZE + field
}

/// Context struct passed to page-level JIT kernels.
///
/// The JIT kernel uses two counters:
/// - `row_ctr`: iterates 0..row_count (indexes bitmap and output)
/// - `val_ctr`: advances only for non-null rows (indexes packed values)
///
/// When `def_bitmap` is null, all rows are non-null and `val_ctr == row_ctr`.
#[repr(C)]
pub struct MultiPageContext {
    pub row_count: u64,
    pub result_ptr: *mut u8,
    /// 1 byte per row: 1=non-null, 0=null. Null pointer means all non-null.
    pub def_bitmap: *const u8,
    pub columns: [PageColumnSlot; MAX_PIPELINE_COLUMNS],
}

/// Page-level JIT kernel function signature.
pub type PageKernelFn = unsafe extern "C" fn(ctx: *const MultiPageContext) -> i32;

/// Compiled page-level JIT kernel.
pub struct CompiledPageKernel {
    pub(crate) _buffer: dynasmrt::ExecutableBuffer,
    pub(crate) fn_ptr: PageKernelFn,
    pub(crate) spec: MultiPipelineSpec,
    pub(crate) code_size: usize,
}

unsafe impl Send for CompiledPageKernel {}
unsafe impl Sync for CompiledPageKernel {}

impl CompiledPageKernel {
    pub fn fn_ptr(&self) -> PageKernelFn { self.fn_ptr }
    pub fn spec(&self) -> &MultiPipelineSpec { &self.spec }
    pub fn code_size(&self) -> usize { self.code_size }
}

// -----------------------------------------------------------------------
// Multi-column spec types
// -----------------------------------------------------------------------

/// Role of a column in a multi-column pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnRole {
    /// Column provides a filter predicate only.
    Filter,
    /// Column provides data for aggregation only.
    Aggregate,
    /// Column provides data for materialization.
    Materialize,
    /// Column provides both filter and aggregation.
    FilterAggregate,
}

/// Per-column specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnSpec {
    pub encoding: JitEncoding,
    pub physical_type: JitPhysicalType,
    pub role: ColumnRole,
    pub filter_op: JitFilterOp,
    pub aggregate_op: JitAggregateOp,
}

/// How to combine multiple filter predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterCombine {
    And,
    Or,
}

/// Specification for a multi-column JIT pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiPipelineSpec {
    pub columns: Vec<ColumnSpec>,
    pub filter_combine: FilterCombine,
    pub output_mode: JitOutputMode,
}

impl MultiPipelineSpec {
    /// Create a `PipelineSpec` suitable for the backend's block parameter
    /// assignment. The backend uses the spec to determine accumulator type
    /// and block parameter layout.
    pub fn backend_spec(&self) -> PipelineSpec {
        if self.output_mode == JitOutputMode::Materialize {
            // Materialize mode: the "physical_type" is used for uses_fp
            // detection and block param assignment. Pick the first Materialize
            // column's type.
            let mat_col = self
                .columns
                .iter()
                .find(|c| c.role == ColumnRole::Materialize)
                .expect("materialize spec must have a Materialize column");
            PipelineSpec {
                encoding: JitEncoding::Plain,
                physical_type: mat_col.physical_type,
                filter_op: JitFilterOp::None,
                aggregate_op: JitAggregateOp::Count,
                output_mode: self.output_mode,
            }
        } else {
            let agg_col = self
                .columns
                .iter()
                .find(|c| matches!(c.role, ColumnRole::Aggregate | ColumnRole::FilterAggregate))
                .expect("aggregate spec must have an Aggregate column");
            PipelineSpec {
                encoding: JitEncoding::Plain,
                physical_type: agg_col.physical_type,
                filter_op: JitFilterOp::None,
                aggregate_op: agg_col.aggregate_op,
                output_mode: self.output_mode,
            }
        }
    }

    /// Whether any column uses a floating-point type.
    pub fn uses_fp(&self) -> bool {
        self.columns.iter().any(|c| c.physical_type.is_float())
    }
}

/// Convert a `ColumnSpec` to a `PipelineSpec` for per-column helpers.
pub fn column_to_pipeline_spec(col: &ColumnSpec, output_mode: JitOutputMode) -> PipelineSpec {
    PipelineSpec {
        encoding: col.encoding,
        physical_type: col.physical_type,
        filter_op: col.filter_op,
        aggregate_op: col.aggregate_op,
        output_mode,
    }
}

// -----------------------------------------------------------------------
// Compiled multi-column pipeline
// -----------------------------------------------------------------------

pub struct CompiledMultiPipeline {
    pub(crate) _buffer: dynasmrt::ExecutableBuffer,
    pub(crate) fn_ptr: MultiPipelineFn,
    pub(crate) spec: MultiPipelineSpec,
    pub(crate) code_size: usize,
}

unsafe impl Send for CompiledMultiPipeline {}
unsafe impl Sync for CompiledMultiPipeline {}

impl CompiledMultiPipeline {
    pub fn fn_ptr(&self) -> MultiPipelineFn {
        self.fn_ptr
    }
    pub fn spec(&self) -> &MultiPipelineSpec {
        &self.spec
    }
    pub fn code_size(&self) -> usize {
        self.code_size
    }
}

// -----------------------------------------------------------------------
// Pipeline generation
// -----------------------------------------------------------------------

/// Generate a compiled multi-column pipeline for the given specification.
#[cfg(target_arch = "aarch64")]
pub fn generate_multi_pipeline(
    spec: &MultiPipelineSpec,
) -> Result<CompiledMultiPipeline, JitError> {
    validate_multi_spec(spec)?;

    let mut builder = IrBuilder::new();
    super::encode::multi::emit_ir(&mut builder, spec)?;
    let ir = builder.finish();
    super::backend::compile_multi(&ir, spec)
}

fn validate_multi_spec(spec: &MultiPipelineSpec) -> Result<(), JitError> {
    if spec.columns.is_empty() {
        return Err(JitError::UnsupportedCombination(
            "empty column list".into(),
        ));
    }
    if spec.columns.len() > MAX_PIPELINE_COLUMNS {
        return Err(JitError::UnsupportedCombination(format!(
            "too many columns: {} > {}",
            spec.columns.len(),
            MAX_PIPELINE_COLUMNS
        )));
    }

    let mut agg_count = 0u32;
    for (i, col) in spec.columns.iter().enumerate() {
        if col.physical_type.is_wide() {
            return Err(JitError::UnsupportedCombination(format!(
                "column {i}: I128/I256 not supported in multi-column pipelines"
            )));
        }

        // IsNull doesn't compose with multi-column filter logic — null
        // handling is done by the per-row bitmap check chain.
        if col.filter_op == JitFilterOp::IsNull {
            return Err(JitError::UnsupportedCombination(format!(
                "column {i}: IsNull filter not supported in multi-column pipelines \
                 (use the single-column pipeline for IS NULL queries)"
            )));
        }

        match col.role {
            ColumnRole::Aggregate | ColumnRole::FilterAggregate => agg_count += 1,
            ColumnRole::Filter => {
                // A Filter column with filter_op=None produces no predicate.
                // This is almost certainly a caller mistake.
                if col.filter_op == JitFilterOp::None {
                    return Err(JitError::UnsupportedCombination(format!(
                        "column {i}: Filter role with filter_op=None is meaningless"
                    )));
                }
            }
            ColumnRole::Materialize => {
                if spec.output_mode != JitOutputMode::Materialize {
                    return Err(JitError::UnsupportedCombination(format!(
                        "column {i}: Materialize role requires Materialize output mode"
                    )));
                }
            }
        }
    }

    if spec.output_mode == JitOutputMode::Aggregate {
        if agg_count == 0 {
            return Err(JitError::UnsupportedCombination(
                "Aggregate mode requires an Aggregate or FilterAggregate column".into(),
            ));
        }
        if agg_count > 1 {
            return Err(JitError::UnsupportedCombination(format!(
                "{agg_count} aggregate columns found, expected exactly 1"
            )));
        }
    }

    if spec.output_mode == JitOutputMode::Materialize {
        let mat_count = spec
            .columns
            .iter()
            .filter(|c| c.role == ColumnRole::Materialize)
            .count();
        if mat_count == 0 {
            return Err(JitError::UnsupportedCombination(
                "Materialize mode requires at least one Materialize column".into(),
            ));
        }
    }

    Ok(())
}

#[cfg(not(target_arch = "aarch64"))]
pub fn generate_multi_pipeline(
    _spec: &MultiPipelineSpec,
) -> Result<CompiledMultiPipeline, JitError> {
    Err(JitError::UnsupportedCombination(
        "multi-column pipeline only supported on aarch64".into(),
    ))
}

/// Generate a compiled page-level JIT kernel for the given specification.
///
/// The page kernel is a simplified variant of the multi-column pipeline:
/// no null bitmap checks (the Rust driver handles def_levels), and the
/// accumulator state is loaded/stored from result_ptr across batches.
#[cfg(target_arch = "aarch64")]
pub fn generate_page_kernel(
    spec: &MultiPipelineSpec,
) -> Result<CompiledPageKernel, JitError> {
    validate_multi_spec(spec)?;

    let mut builder = IrBuilder::new();
    super::encode::multi_page::emit_ir(&mut builder, spec)?;
    let ir = builder.finish();
    super::backend::compile_page_kernel(&ir, spec)
}

#[cfg(not(target_arch = "aarch64"))]
pub fn generate_page_kernel(
    _spec: &MultiPipelineSpec,
) -> Result<CompiledPageKernel, JitError> {
    Err(JitError::UnsupportedCombination(
        "page kernel only supported on aarch64".into(),
    ))
}

/// Execute a compiled page-level JIT kernel.
///
/// # Safety
/// The `MultiPageContext` must have valid pointers and sufficient data.
pub unsafe fn execute_page_kernel(
    kernel: &CompiledPageKernel,
    ctx: &MultiPageContext,
) -> i32 {
    (kernel.fn_ptr)(ctx as *const MultiPageContext)
}

/// Execute a compiled multi-column pipeline.
///
/// # Safety
/// The `MultiPipelineContext` must have valid pointers and sufficient data.
pub unsafe fn execute_multi_pipeline(
    pipeline: &CompiledMultiPipeline,
    ctx: &MultiPipelineContext,
) -> i32 {
    (pipeline.fn_ptr)(ctx as *const MultiPipelineContext)
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::PipelineResult;
    use super::*;

    fn i64b(v: &[i64]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }
    fn i32b(v: &[i32]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }
    fn i16b(v: &[i16]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }
    fn i8b(v: &[i8]) -> Vec<u8> {
        v.iter().map(|x| *x as u8).collect()
    }
    fn f64b(v: &[f64]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }
    fn f32b(v: &[f32]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    fn make_ctx(
        row_count: u64,
        result: *mut PipelineResult,
        col_data: &[(&[u8], Option<&[u8]>, i64, i64)],
    ) -> MultiPipelineContext {
        let mut ctx = MultiPipelineContext {
            row_count,
            result_ptr: result as *mut u8,
            columns: unsafe { std::mem::zeroed() },
        };
        for (i, (vals, bm, flo, fhi)) in col_data.iter().enumerate() {
            ctx.columns[i] = ColumnSlot {
                values_data: vals.as_ptr(),
                null_bitmap: bm.map(|b| b.as_ptr()).unwrap_or(std::ptr::null()),
                filter_lo: *flo,
                filter_hi: *fhi,
            };
        }
        ctx
    }

    fn col_filter(pt: JitPhysicalType, fop: JitFilterOp) -> ColumnSpec {
        ColumnSpec {
            encoding: JitEncoding::Plain,
            physical_type: pt,
            role: ColumnRole::Filter,
            filter_op: fop,
            aggregate_op: JitAggregateOp::Count,
        }
    }

    fn col_agg(pt: JitPhysicalType, aop: JitAggregateOp) -> ColumnSpec {
        ColumnSpec {
            encoding: JitEncoding::Plain,
            physical_type: pt,
            role: ColumnRole::Aggregate,
            filter_op: JitFilterOp::None,
            aggregate_op: aop,
        }
    }

    fn spec_filter_agg(
        filter_pt: JitPhysicalType,
        filter_op: JitFilterOp,
        agg_pt: JitPhysicalType,
        agg_op: JitAggregateOp,
    ) -> MultiPipelineSpec {
        MultiPipelineSpec {
            columns: vec![col_filter(filter_pt, filter_op), col_agg(agg_pt, agg_op)],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        }
    }

    fn run(spec: &MultiPipelineSpec, n: u64, cols: &[(&[u8], Option<&[u8]>, i64, i64)]) -> PipelineResult {
        let pipeline = generate_multi_pipeline(spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_ctx(n, &mut result, cols);
        unsafe { execute_multi_pipeline(&pipeline, &ctx); }
        result
    }

    // =========================================================================
    // Phase 1: 2-column filter+aggregate — basic types
    // =========================================================================

    #[test]
    fn test_multi_filter_agg_basic() {
        let col_a = i64b(&[1, 10, 3, 20, 5, 15]);
        let col_b = i64b(&[100, 200, 300, 400, 500, 600]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            6, &[(&col_a, None, 5, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64, r.has_value), (3, 1200, 1));
    }

    #[test]
    fn test_multi_filter_agg_nulls() {
        let col_a = i64b(&[10, 20, 3, 15, 8, 25]);
        let col_b = i64b(&[100, 200, 300, 400, 500, 600]);
        let bm_a = vec![1u8, 0, 1, 1, 0, 1];
        let bm_b = vec![1u8, 1, 0, 1, 1, 1];
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            6, &[(&col_a, Some(&bm_a), 5, 0), (&col_b, Some(&bm_b), 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (3, 1100));
    }

    #[test]
    fn test_multi_filter_i32_agg_i64() {
        let col_a = i32b(&[5, 10, 15, 3, 20]);
        let col_b = i64b(&[100, 200, 50, 300, 150]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int32, JitFilterOp::Ge, JitPhysicalType::Int64, JitAggregateOp::Max),
            5, &[(&col_a, None, 10, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (3, 200));
    }

    #[test]
    fn test_multi_filter_i16() {
        let col_a = i16b(&[1, 10, 3, 20]);
        let col_b = i64b(&[100, 200, 300, 400]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int16, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            4, &[(&col_a, None, 5, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (2, 600));
    }

    #[test]
    fn test_multi_filter_i8() {
        let col_a = i8b(&[1, 10, 3, 20]);
        let col_b = i64b(&[100, 200, 300, 400]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int8, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            4, &[(&col_a, None, 5, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (2, 600));
    }

    // =========================================================================
    // FP types: Double and Float (f32)
    // =========================================================================

    #[test]
    fn test_multi_fp_double_filter_double_agg() {
        let col_a = f64b(&[1.0, 3.0, 2.0, 5.0, 0.5]);
        let col_b = f64b(&[10.0, 20.0, 30.0, 40.0, 50.0]);
        let flo = 2.0f64.to_bits() as i64;
        let r = run(
            &spec_filter_agg(JitPhysicalType::Double, JitFilterOp::Gt, JitPhysicalType::Double, JitAggregateOp::Sum),
            5, &[(&col_a, None, flo, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!(r.count, 2);
        assert!((r.value_f64 - 60.0).abs() < 1e-10);
    }

    #[test]
    fn test_multi_fp_float_filter_float_agg() {
        let col_a = f32b(&[1.0f32, 3.0, 2.0, 5.0, 0.5]);
        let col_b = f32b(&[10.0f32, 20.0, 30.0, 40.0, 50.0]);
        let flo = 2.0f64.to_bits() as i64; // f32 is compared as f64
        let r = run(
            &spec_filter_agg(JitPhysicalType::Float, JitFilterOp::Gt, JitPhysicalType::Float, JitAggregateOp::Sum),
            5, &[(&col_a, None, flo, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!(r.count, 2);
        assert!((r.value_f64 - 60.0).abs() < 1e-5);
    }

    #[test]
    fn test_multi_fp_filter_int_agg() {
        let col_a = f64b(&[1.0, 5.5, 3.0, 10.0, 2.5]);
        let col_b = i64b(&[100, 200, 300, 400, 500]);
        let flo = 3.0f64.to_bits() as i64;
        let r = run(
            &spec_filter_agg(JitPhysicalType::Double, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            5, &[(&col_a, None, flo, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (2, 600));
    }

    #[test]
    fn test_multi_fp_between() {
        let col_a = f64b(&[1.0, 2.5, 3.0, 4.5, 6.0]);
        let col_b = f64b(&[10.0, 20.0, 30.0, 40.0, 50.0]);
        let flo = 2.0f64.to_bits() as i64;
        let fhi = 4.0f64.to_bits() as i64;
        let r = run(
            &spec_filter_agg(JitPhysicalType::Double, JitFilterOp::Between, JitPhysicalType::Double, JitAggregateOp::Sum),
            5, &[(&col_a, None, flo, fhi), (&col_b, None, 0, 0)],
        );
        // Between 2.0..=4.0: rows 1(2.5), 2(3.0) → sum=20+30=50
        assert_eq!(r.count, 2);
        assert!((r.value_f64 - 50.0).abs() < 1e-10);
    }

    #[test]
    fn test_multi_fp_nan_rejected() {
        let col_a = f64b(&[1.0, f64::NAN, 3.0, f64::NAN, 5.0]);
        let col_b = f64b(&[10.0, 20.0, 30.0, 40.0, 50.0]);
        let flo = 0.0f64.to_bits() as i64;
        let r = run(
            &spec_filter_agg(JitPhysicalType::Double, JitFilterOp::Gt, JitPhysicalType::Double, JitAggregateOp::Sum),
            5, &[(&col_a, None, flo, 0), (&col_b, None, 0, 0)],
        );
        // NaN rejected: rows 0(1.0), 2(3.0), 4(5.0) pass
        assert_eq!(r.count, 3);
        assert!((r.value_f64 - 90.0).abs() < 1e-10);
    }

    // =========================================================================
    // Edge cases
    // =========================================================================

    #[test]
    fn test_multi_empty() {
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            0, &[(&[], None, 5, 0), (&[], None, 0, 0)],
        );
        assert_eq!((r.count, r.has_value), (0, 0));
    }

    #[test]
    fn test_multi_single_row() {
        let col_a = i64b(&[10]);
        let col_b = i64b(&[42]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            1, &[(&col_a, None, 5, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (1, 42));
    }

    #[test]
    fn test_multi_all_null() {
        let col_a = i64b(&[10, 20, 30]);
        let col_b = i64b(&[100, 200, 300]);
        let bm = vec![0u8, 0, 0];
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            3, &[(&col_a, Some(&bm), 5, 0), (&col_b, Some(&bm), 0, 0)],
        );
        assert_eq!((r.count, r.has_value), (0, 0));
    }

    #[test]
    fn test_multi_is_not_null_filter() {
        // IsNotNull filter → always passes for non-null rows (no value comparison)
        let col_a = i64b(&[10, 20, 30, 40]);
        let col_b = i64b(&[1, 2, 3, 4]);
        let bm_a = vec![1u8, 0, 1, 0]; // rows 1,3 null
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::IsNotNull, JitPhysicalType::Int64, JitAggregateOp::Sum),
            4, &[(&col_a, Some(&bm_a), 0, 0), (&col_b, None, 0, 0)],
        );
        // Non-null rows: 0, 2 → sum(col_b) = 1 + 3 = 4
        assert_eq!((r.count, r.value_i64), (2, 4));
    }

    #[test]
    fn test_multi_all_pass() {
        let col_a = i64b(&[10, 20, 30]);
        let col_b = i64b(&[1, 2, 3]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            3, &[(&col_a, None, 0, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (3, 6));
    }

    #[test]
    fn test_multi_none_pass() {
        let col_a = i64b(&[1, 2, 3]);
        let col_b = i64b(&[10, 20, 30]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            3, &[(&col_a, None, 100, 0), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.has_value), (0, 0));
    }

    #[test]
    fn test_multi_between_filter() {
        let col_a = i64b(&[1, 5, 3, 10, 7, 2, 8]);
        let col_b = i64b(&[100, 200, 50, 400, 150, 600, 75]);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Between, JitPhysicalType::Int64, JitAggregateOp::Min),
            7, &[(&col_a, None, 3, 8), (&col_b, None, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (4, 50));
    }

    // =========================================================================
    // Phase 2: multi-column AND/OR filters
    // =========================================================================

    #[test]
    fn test_multi_and_filter() {
        let col_a = i64b(&[10, 3, 20, 1, 15]);
        let col_b = i64b(&[100, 200, 600, 300, 400]);
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 5, &[
            (&col_a, None, 5, 0), (&col_b, None, 500, 0), (&col_b, None, 0, 0),
        ]);
        assert_eq!((r.count, r.value_i64), (2, 500));
    }

    #[test]
    fn test_multi_or_filter() {
        let col_a = i64b(&[10, 20, 5, 25, 8]);
        let col_b = i64b(&[100, 300, 150, 500, 400]);
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum),
            ],
            filter_combine: FilterCombine::Or,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 5, &[
            (&col_a, None, 15, 0), (&col_b, None, 200, 0), (&col_b, None, 0, 0),
        ]);
        assert_eq!((r.count, r.value_i64), (4, 1050));
    }

    #[test]
    fn test_multi_3col_filter_agg() {
        let col_a = i64b(&[10, 3, 20, 1, 15, 8]);
        let col_b = i64b(&[100, 200, 600, 300, 400, 50]);
        let col_c = i64b(&[1, 2, 3, 4, 5, 6]);
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 6, &[
            (&col_a, None, 5, 0), (&col_b, None, 500, 0), (&col_c, None, 0, 0),
        ]);
        assert_eq!((r.count, r.value_i64), (3, 12));
    }

    #[test]
    fn test_multi_3col_with_nulls() {
        let col_a = i64b(&[10, 3, 20, 1, 15, 8]);
        let col_b = i64b(&[100, 200, 600, 300, 400, 50]);
        let col_c = i64b(&[1, 2, 3, 4, 5, 6]);
        let bm_a = vec![1u8, 1, 1, 0, 1, 1]; // row 3 null
        let bm_b = vec![1u8, 1, 0, 1, 1, 1]; // row 2 null
        let bm_c = vec![1u8, 1, 1, 1, 1, 1]; // no nulls
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 6, &[
            (&col_a, Some(&bm_a), 5, 0),
            (&col_b, Some(&bm_b), 500, 0),
            (&col_c, Some(&bm_c), 0, 0),
        ]);
        // Non-null in all: rows 0,1,4,5 (row 2 null in B, row 3 null in A)
        // col_A > 5: 0(10)✓ 1(3)✗ 4(15)✓ 5(8)✓
        // col_B < 500: 0(100)✓ 4(400)✓ 5(50)✓
        // AND: rows 0, 4, 5 → col_C sum = 1 + 5 + 6 = 12
        assert_eq!((r.count, r.value_i64), (3, 12));
    }

    // =========================================================================
    // 4+ columns — tests the register-unlimited reload approach
    // =========================================================================

    #[test]
    fn test_multi_4col_3_filters() {
        // 3 filter columns + 1 aggregate
        let col_a = i64b(&[10, 3, 20, 1, 15]);
        let col_b = i64b(&[100, 200, 600, 300, 400]);
        let col_c = i64b(&[5, 8, 2, 9, 7]);
        let col_d = i64b(&[1, 2, 3, 4, 5]);
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),  // col_A > 5
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),  // col_B < 500
                col_filter(JitPhysicalType::Int64, JitFilterOp::Ge),  // col_C >= 5
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum), // sum(col_D)
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 5, &[
            (&col_a, None, 5, 0),
            (&col_b, None, 500, 0),
            (&col_c, None, 5, 0),
            (&col_d, None, 0, 0),
        ]);
        // col_A>5: rows 0,2,4  col_B<500: rows 0,1,3,4  col_C>=5: rows 0,1,3,4
        // AND: rows 0, 4 → col_D sum = 1 + 5 = 6
        assert_eq!((r.count, r.value_i64), (2, 6));
    }

    #[test]
    fn test_multi_5col_4_filters() {
        // 4 filter columns + 1 aggregate — stress test scratch pool
        let n = 8usize;
        let a: Vec<i64> = (0..n as i64).collect();          // 0..7
        let b: Vec<i64> = (0..n as i64).map(|x| x * 10).collect(); // 0,10,20..70
        let c: Vec<i64> = vec![1, 0, 1, 0, 1, 0, 1, 0];    // alternating
        let d: Vec<i64> = vec![5, 5, 5, 5, 15, 15, 15, 15]; // half
        let e: Vec<i64> = vec![100; n];                       // all 100

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Ge),  // a >= 2
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),  // b < 60
                col_filter(JitPhysicalType::Int64, JitFilterOp::Eq),  // c == 1
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),  // d > 10
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Count),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, n as u64, &[
            (&i64b(&a), None, 2, 0),
            (&i64b(&b), None, 60, 0),
            (&i64b(&c), None, 1, 0),
            (&i64b(&d), None, 10, 0),
            (&i64b(&e), None, 0, 0),
        ]);
        // a>=2: 2,3,4,5,6,7  b<60: 0,1,2,3,4,5  c==1: 0,2,4,6  d>10: 4,5,6,7
        // AND: row 4 only → count=1
        assert_eq!(r.count, 1);
    }

    #[test]
    fn test_multi_4col_with_nulls() {
        let a = i64b(&[10, 20, 30, 40]);
        let b = i64b(&[1, 2, 3, 4]);
        let c = i64b(&[100, 200, 300, 400]);
        let d = i64b(&[5, 6, 7, 8]);
        let bm_a = vec![1u8, 0, 1, 1]; // row 1 null
        let bm_b = vec![1u8, 1, 0, 1]; // row 2 null
        let bm_c = vec![1u8, 1, 1, 0]; // row 3 null
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 4, &[
            (&a, Some(&bm_a), 5, 0),
            (&b, Some(&bm_b), 0, 0),
            (&c, Some(&bm_c), 500, 0),
            (&d, None, 0, 0),
        ]);
        // Non-null in all: row 0 only (rows 1,2,3 have a null somewhere)
        // a>5: 10✓  b>0: 1✓  c<500: 100✓ → passes
        // Sum of d: 5
        assert_eq!((r.count, r.value_i64), (1, 5));
    }

    // =========================================================================
    // Oracle-based exhaustive: all filter ops × all aggregate ops
    // =========================================================================

    #[test]
    fn test_multi_filter_agg_all_ops() {
        let fv: Vec<i64> = vec![1, 5, 3, 10, 7, 2, 8, 4, 6, 9];
        let av: Vec<i64> = vec![100, 200, 300, 400, 500, 600, 700, 800, 900, 1000];
        let fa = i64b(&fv);
        let ab = i64b(&av);
        let n = fv.len();
        let flo = 5i64;
        for &fop in &[JitFilterOp::Eq, JitFilterOp::Ne, JitFilterOp::Lt,
                       JitFilterOp::Le, JitFilterOp::Gt, JitFilterOp::Ge] {
            for &aop in &[JitAggregateOp::Count, JitAggregateOp::Sum,
                          JitAggregateOp::Min, JitAggregateOp::Max] {
                let r = run(
                    &spec_filter_agg(JitPhysicalType::Int64, fop, JitPhysicalType::Int64, aop),
                    n as u64, &[(&fa, None, flo, 0), (&ab, None, 0, 0)],
                );
                let (ec, ev, eh) = oracle_multi_int(&fv, &av, fop, flo, aop);
                assert_eq!(r.count, ec, "{fop:?}/{aop:?} count");
                if eh > 0 { assert_eq!(r.value_i64, ev, "{fop:?}/{aop:?} val"); }
                assert_eq!(r.has_value, eh, "{fop:?}/{aop:?} has");
            }
        }
    }

    // =========================================================================
    // Registry + validation
    // =========================================================================

    #[test]
    fn test_multi_registry() {
        let spec = spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum);
        let p1 = super::super::REGISTRY.get_or_generate_multi(&spec).unwrap();
        let p2 = super::super::REGISTRY.get_or_generate_multi(&spec).unwrap();
        assert!(std::sync::Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn test_multi_rejects_wide_types() {
        let spec = spec_filter_agg(JitPhysicalType::Int128, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum);
        assert!(generate_multi_pipeline(&spec).is_err());
    }

    #[test]
    fn test_multi_rejects_empty() {
        let spec = MultiPipelineSpec {
            columns: vec![],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_multi_pipeline(&spec).is_err());
    }

    // =========================================================================
    // Oracle
    // =========================================================================

    fn oracle_multi_int(
        filter_vals: &[i64],
        agg_vals: &[i64],
        fop: JitFilterOp,
        flo: i64,
        aop: JitAggregateOp,
    ) -> (u64, i64, u64) {
        let (mut c, mut a, mut h) = (0u64, 0i64, false);
        for (i, &fv) in filter_vals.iter().enumerate() {
            let pass = match fop {
                JitFilterOp::Eq => fv == flo,
                JitFilterOp::Ne => fv != flo,
                JitFilterOp::Lt => fv < flo,
                JitFilterOp::Le => fv <= flo,
                JitFilterOp::Gt => fv > flo,
                JitFilterOp::Ge => fv >= flo,
                _ => true,
            };
            if pass {
                let av = agg_vals[i];
                match aop {
                    JitAggregateOp::Count => c += 1,
                    JitAggregateOp::Sum => {
                        a += av;
                        c += 1;
                        h = true;
                    }
                    JitAggregateOp::Min => {
                        if !h || av < a {
                            a = av;
                        }
                        c += 1;
                        h = true;
                    }
                    JitAggregateOp::Max => {
                        if !h || av > a {
                            a = av;
                        }
                        c += 1;
                        h = true;
                    }
                }
            }
        }
        (c, a, u64::from(h))
    }

    // =========================================================================
    // FilterAggregate role (filter + aggregate on the same column)
    // =========================================================================

    #[test]
    fn test_multi_filter_aggregate_single_col() {
        // Single column: FilterAggregate — filter AND aggregate on the same data
        let col_a = i64b(&[1, 10, 3, 20, 5]);
        let spec = MultiPipelineSpec {
            columns: vec![ColumnSpec {
                encoding: JitEncoding::Plain,
                physical_type: JitPhysicalType::Int64,
                role: ColumnRole::FilterAggregate,
                filter_op: JitFilterOp::Gt,
                aggregate_op: JitAggregateOp::Sum,
            }],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 5, &[(&col_a, None, 5, 0)]);
        // col_A > 5: rows 1(10), 3(20) → sum = 30
        assert_eq!((r.count, r.value_i64), (2, 30));
    }

    #[test]
    fn test_multi_filter_aggregate_with_extra_filter() {
        // col_A: Filter (Gt), col_B: FilterAggregate (Le + Sum)
        let col_a = i64b(&[10, 3, 20, 1, 15]);
        let col_b = i64b(&[100, 200, 50, 300, 400]);
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Int64,
                    role: ColumnRole::FilterAggregate,
                    filter_op: JitFilterOp::Le,
                    aggregate_op: JitAggregateOp::Sum,
                },
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 5, &[
            (&col_a, None, 5, 0),   // col_A > 5
            (&col_b, None, 300, 0), // col_B <= 300 AND sum(col_B)
        ]);
        // col_A > 5: rows 0(10),2(20),4(15)
        // col_B <= 300: rows 0(100),1(200),2(50),3(300)
        // AND: rows 0(100), 2(50) → sum = 150
        assert_eq!((r.count, r.value_i64), (2, 150));
    }

    // =========================================================================
    // Validation rejection tests
    // =========================================================================

    #[test]
    fn test_multi_rejects_is_null_filter() {
        let spec = MultiPipelineSpec {
            columns: vec![
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Int64,
                    role: ColumnRole::Filter,
                    filter_op: JitFilterOp::IsNull,
                    aggregate_op: JitAggregateOp::Count,
                },
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Count),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_multi_pipeline(&spec).is_err());
    }

    #[test]
    fn test_multi_rejects_filter_none() {
        let spec = MultiPipelineSpec {
            columns: vec![
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Int64,
                    role: ColumnRole::Filter,
                    filter_op: JitFilterOp::None,
                    aggregate_op: JitAggregateOp::Count,
                },
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Count),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_multi_pipeline(&spec).is_err());
    }

    #[test]
    fn test_multi_rejects_no_aggregate() {
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_multi_pipeline(&spec).is_err());
    }

    #[test]
    fn test_multi_rejects_two_aggregates() {
        let spec = MultiPipelineSpec {
            columns: vec![
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Max),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        assert!(generate_multi_pipeline(&spec).is_err());
    }

    #[test]
    fn test_multi_rejects_materialize_without_materialize_col() {
        // Materialize mode with only Aggregate columns → error
        let spec = MultiPipelineSpec {
            columns: vec![col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum)],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        assert!(generate_multi_pipeline(&spec).is_err());
    }

    fn col_mat(pt: JitPhysicalType) -> ColumnSpec {
        ColumnSpec {
            encoding: JitEncoding::Plain,
            physical_type: pt,
            role: ColumnRole::Materialize,
            filter_op: JitFilterOp::None,
            aggregate_op: JitAggregateOp::Count,
        }
    }

    // =========================================================================
    // Materialize mode tests
    // =========================================================================

    #[test]
    fn test_multi_materialize_basic() {
        // Filter col_A > 5, materialize col_B
        let col_a = i64b(&[1, 10, 3, 20, 5, 15]);
        let col_b = i64b(&[100, 200, 300, 400, 500, 600]);
        let output_b = vec![0u8; 6 * 8]; // max 6 i64 values

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_mat(JitPhysicalType::Int64),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let pipeline = generate_multi_pipeline(&spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_ctx(6, &mut result, &[
            (&col_a, None, 5, 0),
            (&col_b, None, output_b.as_ptr() as i64, 0), // filter_lo = output ptr
        ]);
        unsafe { execute_multi_pipeline(&pipeline, &ctx); }

        // col_A > 5: rows 1(10), 3(20), 5(15) → materialize col_B values 200, 400, 600
        assert_eq!(result.count, 3);
        let out: Vec<i64> = output_b.chunks_exact(8).take(3)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(out, vec![200, 400, 600]);
    }

    #[test]
    fn test_multi_materialize_2cols() {
        // Filter col_A > 5, materialize col_B and col_C
        let col_a = i64b(&[1, 10, 3, 20, 5]);
        let col_b = i64b(&[100, 200, 300, 400, 500]);
        let col_c = i32b(&[10, 20, 30, 40, 50]);
        let out_b = vec![0u8; 5 * 8];
        let out_c = vec![0u8; 5 * 4];

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_mat(JitPhysicalType::Int64),
                col_mat(JitPhysicalType::Int32),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let pipeline = generate_multi_pipeline(&spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_ctx(5, &mut result, &[
            (&col_a, None, 5, 0),
            (&col_b, None, out_b.as_ptr() as i64, 0),
            (&col_c, None, out_c.as_ptr() as i64, 0),
        ]);
        unsafe { execute_multi_pipeline(&pipeline, &ctx); }

        // col_A > 5: rows 1(10), 3(20) → count=2
        assert_eq!(result.count, 2);
        let ob: Vec<i64> = out_b.chunks_exact(8).take(2)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(ob, vec![200, 400]);
        let oc: Vec<i32> = out_c.chunks_exact(4).take(2)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(oc, vec![20, 40]);
    }

    #[test]
    fn test_multi_materialize_no_filter() {
        // No filter → materialize all rows
        let col_a = i64b(&[10, 20, 30]);
        let out_a = vec![0u8; 3 * 8];

        let spec = MultiPipelineSpec {
            columns: vec![col_mat(JitPhysicalType::Int64)],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let pipeline = generate_multi_pipeline(&spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_ctx(3, &mut result, &[
            (&col_a, None, out_a.as_ptr() as i64, 0),
        ]);
        unsafe { execute_multi_pipeline(&pipeline, &ctx); }

        assert_eq!(result.count, 3);
        let out: Vec<i64> = out_a.chunks_exact(8).take(3)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(out, vec![10, 20, 30]);
    }

    #[test]
    fn test_multi_materialize_with_nulls() {
        // Filter col_A > 5, materialize col_B, both have bitmaps
        let col_a = i64b(&[10, 20, 3, 15, 8]);
        let col_b = i64b(&[100, 200, 300, 400, 500]);
        let bm_a = vec![1u8, 0, 1, 1, 1]; // row 1 null
        let bm_b = vec![1u8, 1, 1, 0, 1]; // row 3 null
        let out_b = vec![0u8; 5 * 8];

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_mat(JitPhysicalType::Int64),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let pipeline = generate_multi_pipeline(&spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_ctx(5, &mut result, &[
            (&col_a, Some(&bm_a), 5, 0),
            (&col_b, Some(&bm_b), out_b.as_ptr() as i64, 0),
        ]);
        unsafe { execute_multi_pipeline(&pipeline, &ctx); }

        // Non-null in both: rows 0, 2, 4. Of those, col_A > 5: row 0(10), row 4(8).
        // Materialize col_B: 100, 500
        assert_eq!(result.count, 2);
        let out: Vec<i64> = out_b.chunks_exact(8).take(2)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(out, vec![100, 500]);
    }

    #[test]
    fn test_multi_materialize_none_pass() {
        let col_a = i64b(&[1, 2, 3]);
        let out_a = vec![0u8; 3 * 8];

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_mat(JitPhysicalType::Int64),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let pipeline = generate_multi_pipeline(&spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_ctx(3, &mut result, &[
            (&col_a, None, 100, 0), // > 100 → none pass
            (&col_a, None, out_a.as_ptr() as i64, 0),
        ]);
        unsafe { execute_multi_pipeline(&pipeline, &ctx); }

        assert_eq!(result.count, 0);
    }

    #[test]
    fn test_multi_materialize_fp() {
        // Filter col_A (Double) > 2.0, materialize col_B (Double)
        let col_a = f64b(&[1.0, 3.0, 2.0, 5.0]);
        let col_b = f64b(&[10.0, 20.0, 30.0, 40.0]);
        let out_b = vec![0u8; 4 * 8];
        let flo = 2.0f64.to_bits() as i64;

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Double, JitFilterOp::Gt),
                col_mat(JitPhysicalType::Double),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let pipeline = generate_multi_pipeline(&spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_ctx(4, &mut result, &[
            (&col_a, None, flo, 0),
            (&col_b, None, out_b.as_ptr() as i64, 0),
        ]);
        unsafe { execute_multi_pipeline(&pipeline, &ctx); }

        assert_eq!(result.count, 2); // rows 1(3.0), 3(5.0)
        let out: Vec<f64> = out_b.chunks_exact(8).take(2)
            .map(|c| f64::from_le_bytes(c.try_into().unwrap())).collect();
        assert_eq!(out, vec![20.0, 40.0]);
    }

    // =========================================================================
    // 6-8 column stress tests (exercises the register-unlimited design)
    // =========================================================================

    #[test]
    fn test_multi_6col_5_filters() {
        let n = 10usize;
        let a: Vec<i64> = vec![10, 1, 10, 1, 10, 1, 10, 1, 10, 1];
        let b: Vec<i64> = vec![1, 10, 1, 10, 1, 10, 1, 10, 1, 10];
        let c: Vec<i64> = vec![5, 5, 5, 5, 1, 1, 5, 5, 1, 1];
        let d: Vec<i64> = vec![1, 1, 1, 1, 1, 1, 20, 20, 20, 20];
        let e: Vec<i64> = vec![100, 100, 100, 100, 100, 100, 100, 50, 100, 100];
        let f: Vec<i64> = vec![1; n]; // aggregate

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Ge),  // a >= 5
                col_filter(JitPhysicalType::Int64, JitFilterOp::Lt),  // b < 5
                col_filter(JitPhysicalType::Int64, JitFilterOp::Ge),  // c >= 5
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),  // d > 10
                col_filter(JitPhysicalType::Int64, JitFilterOp::Ge),  // e >= 100
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Count),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, n as u64, &[
            (&i64b(&a), None, 5, 0), (&i64b(&b), None, 5, 0),
            (&i64b(&c), None, 5, 0), (&i64b(&d), None, 10, 0),
            (&i64b(&e), None, 100, 0), (&i64b(&f), None, 0, 0),
        ]);
        // a>=5: 0,2,4,6,8  b<5: 0,2,4,6,8  c>=5: 0,1,2,3,6,7
        // d>10: 6,7,8,9  e>=100: 0,1,2,3,4,5,6,8,9
        // AND: row 6 only
        assert_eq!(r.count, 1);
    }

    #[test]
    fn test_multi_8col_max_columns() {
        // Test at MAX_PIPELINE_COLUMNS — 7 filters + 1 aggregate
        let n = 4usize;
        // Row 0: all filters pass. Row 1-3: at least one fails.
        let vals: Vec<Vec<i64>> = (0..8).map(|col| {
            (0..n as i64).map(|row| if row == 0 { 10 } else { col }).collect()
        }).collect();
        let bytes: Vec<Vec<u8>> = vals.iter().map(|v| i64b(v)).collect();

        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt), // > 5
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Count),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let col_data: Vec<(&[u8], Option<&[u8]>, i64, i64)> =
            bytes.iter().map(|b| (b.as_slice(), None, 5i64, 0i64)).collect();
        let r = run(&spec, n as u64, &col_data);
        // Only row 0 has 10 in all columns → passes all 7 filters
        assert_eq!(r.count, 1);
    }

    #[test]
    fn test_multi_6col_with_nulls() {
        // 5 filters + 1 aggregate, some with bitmaps, some without
        let n = 6usize;
        let a = i64b(&[10, 10, 10, 10, 10, 10]); // all pass filter
        let b = i64b(&[10, 10, 10, 10, 10, 10]);
        let c = i64b(&[10, 10, 10, 10, 10, 10]);
        let d = i64b(&[10, 10, 10, 10, 10, 10]);
        let e = i64b(&[10, 10, 10, 10, 10, 10]);
        let f = i64b(&[1, 1, 1, 1, 1, 1]); // aggregate
        // Each bitmap nulls out a different row
        let bm0 = vec![0u8, 1, 1, 1, 1, 1]; // row 0 null
        let bm1 = vec![1u8, 0, 1, 1, 1, 1]; // row 1 null
        // col 2: no bitmap (None) — never null
        let bm3 = vec![1u8, 1, 1, 0, 1, 1]; // row 3 null
        // col 4: no bitmap
        // agg: no bitmap
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Count),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, n as u64, &[
            (&a, Some(&bm0), 5, 0),
            (&b, Some(&bm1), 5, 0),
            (&c, None, 5, 0),        // no bitmap
            (&d, Some(&bm3), 5, 0),
            (&e, None, 5, 0),        // no bitmap
            (&f, None, 0, 0),
        ]);
        // Rows 0,1,3 nulled out. Rows 2,4,5 are non-null in all columns.
        // All values are 10 > 5, so filter passes for non-null rows.
        assert_eq!(r.count, 3);
    }

    // =========================================================================
    // FP stress: multiple FP filter columns, Between
    // =========================================================================

    #[test]
    fn test_multi_3_fp_filters_between() {
        // 2 FP Between filters + 1 FP simple filter + 1 Int aggregate
        let a = f64b(&[5.0, 1.0, 3.0, 7.0, 4.0]);
        let b = f64b(&[10.0, 50.0, 20.0, 30.0, 40.0]);
        let c = f64b(&[100.0, 200.0, 150.0, 50.0, 300.0]);
        let d = i64b(&[1, 2, 3, 4, 5]);
        let flo_a = 2.0f64.to_bits() as i64;
        let fhi_a = 6.0f64.to_bits() as i64;
        let flo_b = 15.0f64.to_bits() as i64;
        let fhi_b = 45.0f64.to_bits() as i64;
        let flo_c = 100.0f64.to_bits() as i64;

        let spec = MultiPipelineSpec {
            columns: vec![
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Double,
                    role: ColumnRole::Filter,
                    filter_op: JitFilterOp::Between,
                    aggregate_op: JitAggregateOp::Count,
                },
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Double,
                    role: ColumnRole::Filter,
                    filter_op: JitFilterOp::Between,
                    aggregate_op: JitAggregateOp::Count,
                },
                col_filter(JitPhysicalType::Double, JitFilterOp::Ge),
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum),
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 5, &[
            (&a, None, flo_a, fhi_a), // a Between 2.0 and 6.0
            (&b, None, flo_b, fhi_b), // b Between 15.0 and 45.0
            (&c, None, flo_c, 0),     // c >= 100.0
            (&d, None, 0, 0),
        ]);
        // a in [2,6]: rows 0(5),2(3),4(4)  b in [15,45]: rows 2(20),3(30),4(40)
        // c >= 100: rows 0(100),1(200),2(150),4(300)
        // AND: rows 2, 4 → sum(d) = 3 + 5 = 8
        assert_eq!((r.count, r.value_i64), (2, 8));
    }

    #[test]
    fn test_multi_bitmap_on_non_first_col() {
        // Column 0 has NO bitmap, column 1 has a bitmap
        let col_a = i64b(&[10, 20, 30]);
        let col_b = i64b(&[1, 2, 3]);
        let bm_b = vec![1u8, 0, 1]; // row 1 null in col B
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Gt, JitPhysicalType::Int64, JitAggregateOp::Sum),
            3, &[(&col_a, None, 5, 0), (&col_b, Some(&bm_b), 0, 0)],
        );
        // All of col_A > 5. Col B row 1 is null → skip.
        // Non-null passing rows: 0, 2 → sum = 1 + 3 = 4
        assert_eq!((r.count, r.value_i64), (2, 4));
    }

    // =========================================================================
    // Stress tests — larger row counts
    // =========================================================================

    #[test]
    fn test_multi_5col_or_mixed_types() {
        // 4 filters (mixed types) + 1 aggregate, OR combination
        let a = i64b(&[10, 1, 20, 1, 15, 1, 30, 1]); // Int64
        let b = i32b(&[1, 100, 1, 100, 1, 100, 1, 100]); // Int32
        let c = f64b(&[0.0, 0.0, 5.0, 0.0, 0.0, 0.0, 7.0, 0.0]); // Double
        let d = i64b(&[0, 0, 0, 0, 0, 0, 0, 99]); // Int64
        let e = i64b(&[1, 1, 1, 1, 1, 1, 1, 1]); // aggregate
        let flo_c = 3.0f64.to_bits() as i64;
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),   // a > 5
                col_filter(JitPhysicalType::Int32, JitFilterOp::Gt),   // b > 50
                col_filter(JitPhysicalType::Double, JitFilterOp::Gt),  // c > 3.0
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),   // d > 50
                col_agg(JitPhysicalType::Int64, JitAggregateOp::Count),
            ],
            filter_combine: FilterCombine::Or,
            output_mode: JitOutputMode::Aggregate,
        };
        let r = run(&spec, 8, &[
            (&a, None, 5, 0), (&b, None, 50, 0),
            (&c, None, flo_c, 0), (&d, None, 50, 0),
            (&e, None, 0, 0),
        ]);
        // a>5: rows 0,2,4,6  b>50: rows 1,3,5,7  c>3.0: rows 2,6  d>50: row 7(99)
        // OR: all 8 rows pass (every row has at least one passing filter)
        assert_eq!(r.count, 8);
    }

    #[test]
    fn test_multi_large_row_count() {
        // 1000 rows, 2 columns
        let n = 1000usize;
        let filter_vals: Vec<i64> = (0..n as i64).collect();
        let agg_vals: Vec<i64> = vec![1; n];
        let fb = i64b(&filter_vals);
        let ab = i64b(&agg_vals);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Ge, JitPhysicalType::Int64, JitAggregateOp::Count),
            n as u64, &[(&fb, None, 500, 0), (&ab, None, 0, 0)],
        );
        // filter >= 500: rows 500..999 = 500 rows
        assert_eq!(r.count, 500);
    }

    #[test]
    fn test_multi_large_row_count_with_nulls() {
        let n = 1000usize;
        let filter_vals: Vec<i64> = (0..n as i64).collect();
        let agg_vals: Vec<i64> = vec![1; n];
        let bm: Vec<u8> = (0..n).map(|i| if i % 3 == 0 { 0 } else { 1 }).collect();
        let fb = i64b(&filter_vals);
        let ab = i64b(&agg_vals);
        let r = run(
            &spec_filter_agg(JitPhysicalType::Int64, JitFilterOp::Ge, JitPhysicalType::Int64, JitAggregateOp::Count),
            n as u64, &[(&fb, Some(&bm), 500, 0), (&ab, Some(&bm), 0, 0)],
        );
        // Non-null: rows where i%3 != 0 (667 rows). Of those, filter >= 500.
        // Rows 500..999 that are non-null: i%3!=0 → 500 * 2/3 ≈ 333-334
        let expected: u64 = (500..1000).filter(|i| i % 3 != 0).count() as u64;
        assert_eq!(r.count, expected);
    }

    // =========================================================================
    // Page kernel tests (decode_row_group_jit foundation)
    // =========================================================================

    fn make_page_ctx(
        row_count: u64,
        result: *mut PipelineResult,
        col_data: &[(&[u8], i64, i64)],
    ) -> MultiPageContext {
        let mut ctx = MultiPageContext {
            row_count,
            result_ptr: result as *mut u8,
            def_bitmap: std::ptr::null(),
            columns: unsafe { std::mem::zeroed() },
        };
        for (i, (vals, flo, fhi)) in col_data.iter().enumerate() {
            ctx.columns[i] = PageColumnSlot {
                values_data: vals.as_ptr(),
                filter_lo: *flo,
                filter_hi: *fhi,
            };
        }
        ctx
    }

    fn run_page(
        spec: &MultiPipelineSpec,
        n: u64,
        cols: &[(&[u8], i64, i64)],
    ) -> PipelineResult {
        let kernel = generate_page_kernel(spec).unwrap();
        let mut result = PipelineResult::new();
        let ctx = make_page_ctx(n, &mut result, cols);
        unsafe { execute_page_kernel(&kernel, &ctx); }
        result
    }

    #[test]
    fn test_page_kernel_sum_basic() {
        // All non-null rows, no filter, sum.
        let vals = i64b(&[1, 2, 3, 4, 5]);
        let r = run_page(
            &MultiPipelineSpec {
                columns: vec![col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum)],
                filter_combine: FilterCombine::And,
                output_mode: JitOutputMode::Aggregate,
            },
            5,
            &[(&vals, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64, r.has_value), (5, 15, 1));
    }

    #[test]
    fn test_page_kernel_accumulator_carries() {
        // Two batches: first batch sums [1,2,3] = 6, second batch sums [4,5] = 9.
        // The accumulator must carry state from the first invocation.
        let spec = MultiPipelineSpec {
            columns: vec![col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum)],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let kernel = generate_page_kernel(&spec).unwrap();
        let mut result = PipelineResult::new();

        // Batch 1: [1, 2, 3]
        let vals1 = i64b(&[1, 2, 3]);
        let ctx1 = make_page_ctx(3, &mut result, &[(&vals1, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx1); }
        assert_eq!((result.count, result.value_i64), (3, 6));

        // Batch 2: [4, 5] — result carries forward
        let vals2 = i64b(&[4, 5]);
        let ctx2 = make_page_ctx(2, &mut result, &[(&vals2, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx2); }
        assert_eq!((result.count, result.value_i64), (5, 15));
    }

    #[test]
    fn test_page_kernel_min_carries() {
        let spec = MultiPipelineSpec {
            columns: vec![col_agg(JitPhysicalType::Int64, JitAggregateOp::Min)],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let kernel = generate_page_kernel(&spec).unwrap();
        let mut result = PipelineResult::new();

        // Batch 1: [10, 5, 8] → min=5
        let vals1 = i64b(&[10, 5, 8]);
        let ctx1 = make_page_ctx(3, &mut result, &[(&vals1, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx1); }
        assert_eq!((result.count, result.value_i64, result.has_value), (3, 5, 1));

        // Batch 2: [3, 20] → min should be 3 (lower than prev 5)
        let vals2 = i64b(&[3, 20]);
        let ctx2 = make_page_ctx(2, &mut result, &[(&vals2, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx2); }
        assert_eq!((result.count, result.value_i64), (5, 3));
    }

    #[test]
    fn test_page_kernel_max_carries() {
        let spec = MultiPipelineSpec {
            columns: vec![col_agg(JitPhysicalType::Int64, JitAggregateOp::Max)],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let kernel = generate_page_kernel(&spec).unwrap();
        let mut result = PipelineResult::new();

        // Batch 1: [3, 7, 5] → max=7
        let vals1 = i64b(&[3, 7, 5]);
        let ctx1 = make_page_ctx(3, &mut result, &[(&vals1, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx1); }
        assert_eq!(result.value_i64, 7);

        // Batch 2: [10, 1] → max should be 10
        let vals2 = i64b(&[10, 1]);
        let ctx2 = make_page_ctx(2, &mut result, &[(&vals2, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx2); }
        assert_eq!(result.value_i64, 10);
    }

    #[test]
    fn test_page_kernel_filter_sum() {
        // Filter col > 5, aggregate sum.
        let filter_vals = i64b(&[1, 10, 3, 20, 5, 15]);
        let agg_vals = i64b(&[100, 200, 300, 400, 500, 600]);
        let r = run_page(
            &spec_filter_agg(
                JitPhysicalType::Int64, JitFilterOp::Gt,
                JitPhysicalType::Int64, JitAggregateOp::Sum,
            ),
            6,
            &[(&filter_vals, 5, 0), (&agg_vals, 0, 0)],
        );
        // Rows passing (>5): indices 1(10), 3(20), 5(15) → sum=200+400+600=1200
        assert_eq!((r.count, r.value_i64, r.has_value), (3, 1200, 1));
    }

    #[test]
    fn test_page_kernel_filter_carries() {
        // Two batches with filter, accumulator carries.
        let spec = spec_filter_agg(
            JitPhysicalType::Int64, JitFilterOp::Gt,
            JitPhysicalType::Int64, JitAggregateOp::Sum,
        );
        let kernel = generate_page_kernel(&spec).unwrap();
        let mut result = PipelineResult::new();

        // Batch 1: filter=[10,1,20], agg=[100,200,300], filter>5 → rows 0,2 → sum=400
        let f1 = i64b(&[10, 1, 20]);
        let a1 = i64b(&[100, 200, 300]);
        let ctx1 = make_page_ctx(3, &mut result, &[(&f1, 5, 0), (&a1, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx1); }
        assert_eq!((result.count, result.value_i64), (2, 400));

        // Batch 2: filter=[30], agg=[500], filter>5 → row 0 → sum=400+500=900
        let f2 = i64b(&[30]);
        let a2 = i64b(&[500]);
        let ctx2 = make_page_ctx(1, &mut result, &[(&f2, 5, 0), (&a2, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx2); }
        assert_eq!((result.count, result.value_i64), (3, 900));
    }

    #[test]
    fn test_page_kernel_empty_batch() {
        let r = run_page(
            &MultiPipelineSpec {
                columns: vec![col_agg(JitPhysicalType::Int64, JitAggregateOp::Sum)],
                filter_combine: FilterCombine::And,
                output_mode: JitOutputMode::Aggregate,
            },
            0,
            &[(&[], 0, 0)],
        );
        assert_eq!((r.count, r.has_value), (0, 0));
    }

    #[test]
    fn test_page_kernel_f64_sum() {
        let vals = f64b(&[1.5, 2.5, 3.0]);
        let r = run_page(
            &MultiPipelineSpec {
                columns: vec![col_agg(JitPhysicalType::Double, JitAggregateOp::Sum)],
                filter_combine: FilterCombine::And,
                output_mode: JitOutputMode::Aggregate,
            },
            3,
            &[(&vals, 0, 0)],
        );
        assert_eq!(r.count, 3);
        assert!((r.value_f64 - 7.0).abs() < 1e-10);
    }

    #[test]
    fn test_page_kernel_f64_carries() {
        let spec = MultiPipelineSpec {
            columns: vec![col_agg(JitPhysicalType::Double, JitAggregateOp::Sum)],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Aggregate,
        };
        let kernel = generate_page_kernel(&spec).unwrap();
        let mut result = PipelineResult::new();

        let vals1 = f64b(&[1.0, 2.0]);
        let ctx1 = make_page_ctx(2, &mut result, &[(&vals1, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx1); }
        assert!((result.value_f64 - 3.0).abs() < 1e-10);

        let vals2 = f64b(&[4.0, 5.0]);
        let ctx2 = make_page_ctx(2, &mut result, &[(&vals2, 0, 0)]);
        unsafe { execute_page_kernel(&kernel, &ctx2); }
        assert!((result.value_f64 - 12.0).abs() < 1e-10);
        assert_eq!(result.count, 4);
    }

    #[test]
    fn test_page_kernel_f32_min() {
        let vals = f32b(&[3.0f32, 1.5, 2.0]);
        let r = run_page(
            &MultiPipelineSpec {
                columns: vec![col_agg(JitPhysicalType::Float, JitAggregateOp::Min)],
                filter_combine: FilterCombine::And,
                output_mode: JitOutputMode::Aggregate,
            },
            3,
            &[(&vals, 0, 0)],
        );
        assert_eq!(r.count, 3);
        assert!((r.value_f64 - 1.5).abs() < 1e-10);
    }

    #[test]
    fn test_page_kernel_i32_filter() {
        let filter_vals = i32b(&[5, 10, 15, 3, 20]);
        let agg_vals = i64b(&[100, 200, 50, 300, 150]);
        let r = run_page(
            &spec_filter_agg(
                JitPhysicalType::Int32, JitFilterOp::Ge,
                JitPhysicalType::Int64, JitAggregateOp::Max,
            ),
            5,
            &[(&filter_vals, 10, 0), (&agg_vals, 0, 0)],
        );
        assert_eq!((r.count, r.value_i64), (3, 200));
    }

    #[test]
    fn test_page_kernel_between_filter() {
        let filter_vals = i64b(&[1, 5, 10, 15, 20]);
        let agg_vals = i64b(&[10, 20, 30, 40, 50]);
        let r = run_page(
            &spec_filter_agg(
                JitPhysicalType::Int64, JitFilterOp::Between,
                JitPhysicalType::Int64, JitAggregateOp::Sum,
            ),
            5,
            &[(&filter_vals, 5, 15), (&agg_vals, 0, 0)],
        );
        // Between 5..=15: rows 1(5), 2(10), 3(15) → sum=20+30+40=90
        assert_eq!((r.count, r.value_i64), (3, 90));
    }

    #[test]
    fn test_page_kernel_count() {
        let vals = i64b(&[10, 20, 30]);
        let r = run_page(
            &MultiPipelineSpec {
                columns: vec![col_agg(JitPhysicalType::Int64, JitAggregateOp::Count)],
                filter_combine: FilterCombine::And,
                output_mode: JitOutputMode::Aggregate,
            },
            3,
            &[(&vals, 0, 0)],
        );
        assert_eq!(r.count, 3);
    }

    #[test]
    fn test_page_kernel_materialize_basic() {
        // Materialize all rows (no filter).
        let vals = i64b(&[10, 20, 30, 40, 50]);
        let mut output = vec![0u8; 5 * 8];
        let mut result = PipelineResult::new();
        let spec = MultiPipelineSpec {
            columns: vec![ColumnSpec {
                encoding: JitEncoding::Plain,
                physical_type: JitPhysicalType::Int64,
                role: ColumnRole::Materialize,
                filter_op: JitFilterOp::None,
                aggregate_op: JitAggregateOp::Count,
            }],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let kernel = generate_page_kernel(&spec).unwrap();
        let ctx = make_page_ctx(
            5, &mut result,
            &[(&vals, output.as_mut_ptr() as i64, 0)],
        );
        unsafe { execute_page_kernel(&kernel, &ctx); }
        assert_eq!(result.count, 5);
        let output_vals: Vec<i64> = output.chunks(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(output_vals, vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn test_page_kernel_materialize_with_filter() {
        // Filter > 20, materialize values.
        let filter_vals = i64b(&[10, 30, 5, 40, 15]);
        let mat_vals = i64b(&[100, 200, 300, 400, 500]);
        let mut output = vec![0u8; 5 * 8];
        let mut result = PipelineResult::new();
        let spec = MultiPipelineSpec {
            columns: vec![
                col_filter(JitPhysicalType::Int64, JitFilterOp::Gt),
                ColumnSpec {
                    encoding: JitEncoding::Plain,
                    physical_type: JitPhysicalType::Int64,
                    role: ColumnRole::Materialize,
                    filter_op: JitFilterOp::None,
                    aggregate_op: JitAggregateOp::Count,
                },
            ],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let kernel = generate_page_kernel(&spec).unwrap();
        let ctx = make_page_ctx(
            5, &mut result,
            &[(&filter_vals, 20, 0), (&mat_vals, output.as_mut_ptr() as i64, 0)],
        );
        unsafe { execute_page_kernel(&kernel, &ctx); }
        // Rows passing (>20): 1(30), 3(40) → mat values: 200, 400
        assert_eq!(result.count, 2);
        let output_vals: Vec<i64> = output[..16].chunks(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        assert_eq!(output_vals, vec![200, 400]);
    }

    #[test]
    fn test_page_kernel_materialize_carries() {
        // Two batches, count carries across invocations.
        let spec = MultiPipelineSpec {
            columns: vec![ColumnSpec {
                encoding: JitEncoding::Plain,
                physical_type: JitPhysicalType::Int64,
                role: ColumnRole::Materialize,
                filter_op: JitFilterOp::None,
                aggregate_op: JitAggregateOp::Count,
            }],
            filter_combine: FilterCombine::And,
            output_mode: JitOutputMode::Materialize,
        };
        let kernel = generate_page_kernel(&spec).unwrap();
        let mut result = PipelineResult::new();
        let mut output = vec![0u8; 5 * 8];

        // Batch 1: [10, 20, 30] → output[0..24]
        let vals1 = i64b(&[10, 20, 30]);
        let ctx1 = make_page_ctx(
            3, &mut result,
            &[(&vals1, output.as_mut_ptr() as i64, 0)],
        );
        unsafe { execute_page_kernel(&kernel, &ctx1); }
        assert_eq!(result.count, 3);

        // Batch 2: [40, 50] → output[24..40], cursor is managed by the driver
        // (in real usage the driver advances the cursor; here we test count carries)
        let vals2 = i64b(&[40, 50]);
        let ctx2 = make_page_ctx(
            2, &mut result,
            &[(&vals2, output.as_mut_ptr().wrapping_add(24) as i64, 0)],
        );
        unsafe { execute_page_kernel(&kernel, &ctx2); }
        assert_eq!(result.count, 5);
    }
}
