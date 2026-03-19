//! JNI entry points for the parquet JIT pipeline.

use std::mem::{offset_of, size_of};
use std::sync::Arc;

use jni::objects::JClass;
use jni::JNIEnv;

use super::{
    CompiledPipeline, JitAggregateOp, JitEncoding, JitFilterOp, JitPhysicalType, PipelineResult,
    PipelineSpec, REGISTRY,
};

fn encoding_from_int(v: i32) -> Option<JitEncoding> {
    match v {
        0 => Some(JitEncoding::Plain),
        1 => Some(JitEncoding::RleDictionary),
        2 => Some(JitEncoding::DeltaBinaryPacked),
        3 => Some(JitEncoding::DeltaLengthByteArray),
        4 => Some(JitEncoding::ByteStreamSplit),
        _ => None,
    }
}

fn physical_type_from_int(v: i32) -> Option<JitPhysicalType> {
    match v {
        0 => Some(JitPhysicalType::Int32),
        1 => Some(JitPhysicalType::Int64),
        2 => Some(JitPhysicalType::Float),
        3 => Some(JitPhysicalType::Double),
        4 => Some(JitPhysicalType::Int8),
        5 => Some(JitPhysicalType::Int16),
        6 => Some(JitPhysicalType::Int128),
        7 => Some(JitPhysicalType::Int256),
        _ => None,
    }
}

fn filter_op_from_int(v: i32) -> Option<JitFilterOp> {
    match v {
        0 => Some(JitFilterOp::None),
        1 => Some(JitFilterOp::Eq),
        2 => Some(JitFilterOp::Ne),
        3 => Some(JitFilterOp::Lt),
        4 => Some(JitFilterOp::Le),
        5 => Some(JitFilterOp::Gt),
        6 => Some(JitFilterOp::Ge),
        7 => Some(JitFilterOp::Between),
        8 => Some(JitFilterOp::IsNull),
        9 => Some(JitFilterOp::IsNotNull),
        _ => None,
    }
}

fn aggregate_op_from_int(v: i32) -> Option<JitAggregateOp> {
    match v {
        0 => Some(JitAggregateOp::Count),
        1 => Some(JitAggregateOp::Sum),
        2 => Some(JitAggregateOp::Min),
        3 => Some(JitAggregateOp::Max),
        _ => None,
    }
}

/// Generate (or retrieve cached) a JIT pipeline for the given spec.
/// Returns an opaque handle (pointer to `Arc<CompiledPipeline>`) or 0 on error.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_generatePipeline(
    _env: JNIEnv,
    _class: JClass,
    encoding: i32,
    physical_type: i32,
    filter_op: i32,
    aggregate_op: i32,
) -> i64 {
    let Some(encoding) = encoding_from_int(encoding) else {
        return 0;
    };
    let Some(physical_type) = physical_type_from_int(physical_type) else {
        return 0;
    };
    let Some(filter_op) = filter_op_from_int(filter_op) else {
        return 0;
    };
    let Some(aggregate_op) = aggregate_op_from_int(aggregate_op) else {
        return 0;
    };

    let spec = PipelineSpec {
        encoding,
        physical_type,
        filter_op,
        aggregate_op,
        output_mode: super::JitOutputMode::Aggregate,
    };

    match REGISTRY.get_or_generate(&spec) {
        Ok(pipeline) => Arc::into_raw(pipeline) as i64,
        Err(_) => 0,
    }
}

/// Execute a previously generated pipeline on raw page values.
///
/// Returns 0 on success, nonzero on error.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_executePipeline(
    _env: JNIEnv,
    _class: JClass,
    pipeline_ptr: i64,
    values_data: i64,
    null_bitmap: i64,
    row_count: i32,
    filter_lo: i64,
    filter_hi: i64,
    result_ptr: i64,
) -> i32 {
    if pipeline_ptr == 0 || values_data == 0 || result_ptr == 0 {
        return -1;
    }

    let pipeline = unsafe { &*(pipeline_ptr as *const CompiledPipeline) };

    unsafe {
        (pipeline.fn_ptr())(
            values_data as *const u8,
            if null_bitmap == 0 {
                std::ptr::null()
            } else {
                null_bitmap as *const u8
            },
            row_count as u32,
            filter_lo,
            filter_hi,
            result_ptr as *mut PipelineResult,
        )
    }
}

/// Release a pipeline handle obtained from `generatePipeline`.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_freePipeline(
    _env: JNIEnv,
    _class: JClass,
    pipeline_ptr: i64,
) {
    if pipeline_ptr == 0 {
        return;
    }
    // Reconstruct the Arc and drop it (decrements refcount).
    unsafe {
        Arc::from_raw(pipeline_ptr as *const CompiledPipeline);
    }
}

// -----------------------------------------------------------------------
// Multi-column pipeline JNI entry points
// -----------------------------------------------------------------------

use super::multi::{
    ColumnRole, ColumnSlot, ColumnSpec, CompiledMultiPipeline, FilterCombine,
    MultiPipelineContext, MultiPipelineSpec, MAX_PIPELINE_COLUMNS,
};

fn column_role_from_int(v: i32) -> Option<ColumnRole> {
    match v {
        0 => Some(ColumnRole::Filter),
        1 => Some(ColumnRole::Aggregate),
        2 => Some(ColumnRole::Materialize),
        3 => Some(ColumnRole::FilterAggregate),
        _ => None,
    }
}

/// Generate a multi-column pipeline.
///
/// `col_specs_ptr` points to a flat array of `(encoding, physical_type, role,
/// filter_op, aggregate_op)` tuples as i32 quintets (5 × col_count ints).
///
/// Returns an opaque handle or 0 on error.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_generateMultiPipeline(
    _env: JNIEnv,
    _class: JClass,
    col_count: i32,
    col_specs_ptr: i64,
    filter_combine: i32,
    output_mode: i32,
) -> i64 {
    if col_count <= 0 || col_count as usize > MAX_PIPELINE_COLUMNS || col_specs_ptr == 0 {
        return 0;
    }
    let filter_combine = match filter_combine {
        0 => FilterCombine::And,
        1 => FilterCombine::Or,
        _ => return 0,
    };
    let output_mode = match output_mode {
        0 => super::JitOutputMode::Aggregate,
        1 => super::JitOutputMode::Materialize,
        _ => return 0,
    };

    let n = col_count as usize;
    let specs_raw = unsafe { std::slice::from_raw_parts(col_specs_ptr as *const i32, n * 5) };
    let mut columns = Vec::with_capacity(n);
    for i in 0..n {
        let base = i * 5;
        let Some(encoding) = encoding_from_int(specs_raw[base]) else { return 0 };
        let Some(physical_type) = physical_type_from_int(specs_raw[base + 1]) else { return 0 };
        let Some(role) = column_role_from_int(specs_raw[base + 2]) else { return 0 };
        let Some(filter_op) = filter_op_from_int(specs_raw[base + 3]) else { return 0 };
        let Some(aggregate_op) = aggregate_op_from_int(specs_raw[base + 4]) else { return 0 };
        columns.push(ColumnSpec {
            encoding,
            physical_type,
            role,
            filter_op,
            aggregate_op,
        });
    }

    let spec = MultiPipelineSpec {
        columns,
        filter_combine,
        output_mode,
    };

    match super::REGISTRY.get_or_generate_multi(&spec) {
        Ok(pipeline) => Arc::into_raw(pipeline) as i64,
        Err(_) => 0,
    }
}

/// Execute a previously generated multi-column pipeline.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_executeMultiPipeline(
    _env: JNIEnv,
    _class: JClass,
    pipeline_ptr: i64,
    ctx_ptr: i64,
) -> i32 {
    if pipeline_ptr == 0 || ctx_ptr == 0 {
        return -1;
    }
    let pipeline = unsafe { &*(pipeline_ptr as *const CompiledMultiPipeline) };
    let ctx = unsafe { &*(ctx_ptr as *const MultiPipelineContext) };
    unsafe { super::multi::execute_multi_pipeline(pipeline, ctx) }
}

/// Release a multi-column pipeline handle.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_freeMultiPipeline(
    _env: JNIEnv,
    _class: JClass,
    pipeline_ptr: i64,
) {
    if pipeline_ptr == 0 {
        return;
    }
    unsafe {
        Arc::from_raw(pipeline_ptr as *const CompiledMultiPipeline);
    }
}

/// Size of MultiPipelineContext struct.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_multiPipelineContextSize(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    size_of::<MultiPipelineContext>() as i64
}

/// Size of a single ColumnSlot.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnSlotSize(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    size_of::<ColumnSlot>() as i64
}

// -----------------------------------------------------------------------
// Page kernel JNI entry points (for decode_row_group_jit)
// -----------------------------------------------------------------------

use super::multi::{
    CompiledPageKernel, MultiPageContext, PageColumnSlot,
};

/// Generate a page-level JIT kernel.
///
/// Same spec format as `generateMultiPipeline`. Returns an opaque handle or 0.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_generatePageKernel(
    _env: JNIEnv,
    _class: JClass,
    col_count: i32,
    col_specs_ptr: i64,
    filter_combine: i32,
    output_mode: i32,
) -> i64 {
    if col_count <= 0 || col_count as usize > MAX_PIPELINE_COLUMNS || col_specs_ptr == 0 {
        return 0;
    }
    let filter_combine = match filter_combine {
        0 => FilterCombine::And,
        1 => FilterCombine::Or,
        _ => return 0,
    };
    let output_mode = match output_mode {
        0 => super::JitOutputMode::Aggregate,
        1 => super::JitOutputMode::Materialize,
        _ => return 0,
    };

    let n = col_count as usize;
    let specs_raw = unsafe { std::slice::from_raw_parts(col_specs_ptr as *const i32, n * 5) };
    let mut columns = Vec::with_capacity(n);
    for i in 0..n {
        let base = i * 5;
        let Some(encoding) = encoding_from_int(specs_raw[base]) else { return 0 };
        let Some(physical_type) = physical_type_from_int(specs_raw[base + 1]) else { return 0 };
        let Some(role) = column_role_from_int(specs_raw[base + 2]) else { return 0 };
        let Some(filter_op) = filter_op_from_int(specs_raw[base + 3]) else { return 0 };
        let Some(aggregate_op) = aggregate_op_from_int(specs_raw[base + 4]) else { return 0 };
        columns.push(ColumnSpec {
            encoding,
            physical_type,
            role,
            filter_op,
            aggregate_op,
        });
    }

    let spec = MultiPipelineSpec {
        columns,
        filter_combine,
        output_mode,
    };

    match super::REGISTRY.get_or_generate_page_kernel(&spec) {
        Ok(kernel) => Arc::into_raw(kernel) as i64,
        Err(_) => 0,
    }
}

/// Execute a page-level JIT kernel.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_executePageKernel(
    _env: JNIEnv,
    _class: JClass,
    kernel_ptr: i64,
    ctx_ptr: i64,
) -> i32 {
    if kernel_ptr == 0 || ctx_ptr == 0 {
        return -1;
    }
    let kernel = unsafe { &*(kernel_ptr as *const CompiledPageKernel) };
    let ctx = unsafe { &*(ctx_ptr as *const MultiPageContext) };
    unsafe { super::multi::execute_page_kernel(kernel, ctx) }
}

/// Release a page kernel handle.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_freePageKernel(
    _env: JNIEnv,
    _class: JClass,
    kernel_ptr: i64,
) {
    if kernel_ptr == 0 {
        return;
    }
    unsafe {
        Arc::from_raw(kernel_ptr as *const CompiledPageKernel);
    }
}

/// Size of MultiPageContext struct.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_pageContextSize(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    size_of::<MultiPageContext>() as i64
}

/// Size of a single PageColumnSlot.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_pageColumnSlotSize(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    size_of::<PageColumnSlot>() as i64
}

// -----------------------------------------------------------------------
// PipelineResult layout offsets (used by Java to read fields via Unsafe)
// -----------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_pipelineResultSize(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    size_of::<PipelineResult>() as i64
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_pipelineResultCountOffset(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    offset_of!(PipelineResult, count) as i64
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_pipelineResultValueI64Offset(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    offset_of!(PipelineResult, value_i64) as i64
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_pipelineResultValueF64Offset(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    offset_of!(PipelineResult, value_f64) as i64
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_pipelineResultHasValueOffset(
    _env: JNIEnv,
    _class: JClass,
) -> i64 {
    offset_of!(PipelineResult, has_value) as i64
}
