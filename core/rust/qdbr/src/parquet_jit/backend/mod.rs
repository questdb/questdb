//! IR backend — compiles IrFunction to native machine code.

#[cfg(target_arch = "aarch64")]
pub mod aarch64;

use super::ir::IrFunction;
use super::{CompiledPipeline, JitError, PipelineSpec};

pub fn compile(ir: &IrFunction, spec: &PipelineSpec) -> Result<CompiledPipeline, JitError> {
    #[cfg(target_arch = "aarch64")]
    return aarch64::compile(ir, spec);

    #[cfg(not(target_arch = "aarch64"))]
    return Err(JitError::UnsupportedCombination(
        "IR backend not yet available for this architecture".into(),
    ));
}

pub fn compile_multi(
    ir: &IrFunction,
    spec: &super::multi::MultiPipelineSpec,
) -> Result<super::multi::CompiledMultiPipeline, JitError> {
    #[cfg(target_arch = "aarch64")]
    return aarch64::compile_multi(ir, spec);

    #[cfg(not(target_arch = "aarch64"))]
    return Err(JitError::UnsupportedCombination(
        "Multi-column IR backend not available for this architecture".into(),
    ));
}

pub fn compile_page_kernel(
    ir: &IrFunction,
    spec: &super::multi::MultiPipelineSpec,
) -> Result<super::multi::CompiledPageKernel, JitError> {
    #[cfg(target_arch = "aarch64")]
    return aarch64::compile_page_kernel(ir, spec);

    #[cfg(not(target_arch = "aarch64"))]
    return Err(JitError::UnsupportedCombination(
        "Page kernel IR backend not available for this architecture".into(),
    ));
}
