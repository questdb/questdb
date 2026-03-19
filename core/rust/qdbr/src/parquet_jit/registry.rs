//! Thread-safe cache for compiled JIT pipelines.
//!
//! Each unique `PipelineSpec` produces a single `CompiledPipeline` that is
//! shared (via `Arc`) across all callers.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use super::multi::{
    generate_multi_pipeline, generate_page_kernel, CompiledMultiPipeline, CompiledPageKernel,
    MultiPipelineSpec,
};
use super::{generate_pipeline, CompiledPipeline, JitError, PipelineSpec};

pub struct PipelineRegistry {
    cache: Mutex<HashMap<PipelineSpec, Arc<CompiledPipeline>>>,
    multi_cache: Mutex<HashMap<MultiPipelineSpec, Arc<CompiledMultiPipeline>>>,
    page_kernel_cache: Mutex<HashMap<MultiPipelineSpec, Arc<CompiledPageKernel>>>,
}

impl PipelineRegistry {
    fn new() -> Self {
        PipelineRegistry {
            cache: Mutex::new(HashMap::new()),
            multi_cache: Mutex::new(HashMap::new()),
            page_kernel_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_or_generate(
        &self,
        spec: &PipelineSpec,
    ) -> Result<Arc<CompiledPipeline>, JitError> {
        let mut cache = self.cache.lock().unwrap();
        if let Some(pipeline) = cache.get(spec) {
            return Ok(Arc::clone(pipeline));
        }
        let pipeline = Arc::new(generate_pipeline(spec)?);
        cache.insert(*spec, Arc::clone(&pipeline));
        Ok(pipeline)
    }

    pub fn get_or_generate_multi(
        &self,
        spec: &MultiPipelineSpec,
    ) -> Result<Arc<CompiledMultiPipeline>, JitError> {
        let mut cache = self.multi_cache.lock().unwrap();
        if let Some(pipeline) = cache.get(spec) {
            return Ok(Arc::clone(pipeline));
        }
        let pipeline = Arc::new(generate_multi_pipeline(spec)?);
        cache.insert(spec.clone(), Arc::clone(&pipeline));
        Ok(pipeline)
    }

    pub fn get_or_generate_page_kernel(
        &self,
        spec: &MultiPipelineSpec,
    ) -> Result<Arc<CompiledPageKernel>, JitError> {
        let mut cache = self.page_kernel_cache.lock().unwrap();
        if let Some(kernel) = cache.get(spec) {
            return Ok(Arc::clone(kernel));
        }
        let kernel = Arc::new(generate_page_kernel(spec)?);
        cache.insert(spec.clone(), Arc::clone(&kernel));
        Ok(kernel)
    }
}

pub static REGISTRY: LazyLock<PipelineRegistry> = LazyLock::new(PipelineRegistry::new);
