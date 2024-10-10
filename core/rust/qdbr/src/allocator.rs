/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
use jni::objects::JClass;
use jni::JNIEnv;
use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone, Copy)]
pub struct QdbWatermarkAllocator {
    rss_mem_limit: *const AtomicUsize,
    rss_mem_used: *mut AtomicUsize,
    tagged_used: *mut AtomicUsize,
    malloc_count: *mut AtomicUsize,
}

const RSS_ORDERING: Ordering = Ordering::SeqCst;
const COUNTER_ORDERING: Ordering = Ordering::AcqRel;

impl QdbWatermarkAllocator {
    pub fn new(
        rss_mem_limit: *const AtomicUsize,
        rss_mem_used: *mut AtomicUsize,
        tagged_used: *mut AtomicUsize,
        malloc_count: *mut AtomicUsize,
    ) -> Self {
        Self {
            rss_mem_limit,
            rss_mem_used,
            tagged_used,
            malloc_count,
        }
    }

    fn rss_mem_limit(&self) -> &AtomicUsize {
        unsafe { &*self.rss_mem_limit }
    }

    fn rss_mem_used(&self) -> &AtomicUsize {
        unsafe { &*self.rss_mem_used }
    }

    fn tagged_used(&self) -> &AtomicUsize {
        unsafe { &*self.tagged_used }
    }

    fn malloc_count(&self) -> &AtomicUsize {
        unsafe { &*self.malloc_count }
    }

    fn check_alloc_limit(&self, layout: Layout) -> Result<(), AllocError> {
        let rss_mem_limit = self.rss_mem_limit().load(RSS_ORDERING);
        if rss_mem_limit > 0 {
            let rss_mem_used = self.rss_mem_used().load(RSS_ORDERING);
            let new_rss_mem_used = rss_mem_used + layout.size();
            if new_rss_mem_used > rss_mem_limit {
                return Err(AllocError);
            }
        }
        Ok(())
    }

    fn add_memory_alloc(&self, layout: Layout) {
        let size = layout.size();
        self.tagged_used().fetch_add(size, COUNTER_ORDERING);
        self.rss_mem_used().fetch_add(size, COUNTER_ORDERING);
        self.malloc_count().fetch_add(1, COUNTER_ORDERING);
    }

    fn sub_memory_alloc(&self, layout: Layout) {
        let size = layout.size();
        self.tagged_used().fetch_sub(size, COUNTER_ORDERING);
        self.rss_mem_used().fetch_sub(size, COUNTER_ORDERING);
        self.malloc_count().fetch_sub(1, COUNTER_ORDERING);
    }
}

unsafe impl Allocator for QdbWatermarkAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.check_alloc_limit(layout)?;
        let allocated = Global.allocate(layout)?;
        self.add_memory_alloc(layout);
        Ok(allocated)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        Global.deallocate(ptr, layout);
        self.sub_memory_alloc(layout);
    }
}

#[allow(dead_code)] // TODO(amunra): remove once in use
#[cfg(test)]
pub struct QdbTestAllocator;

#[cfg(test)]
unsafe impl Allocator for QdbTestAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        Global.deallocate(ptr, layout)
    }
}

#[allow(dead_code)] // TODO(amunra): remove once in use
#[cfg(not(test))]
pub type QdbAllocator = QdbWatermarkAllocator;

#[allow(dead_code)] // TODO(amunra): remove once in use
#[cfg(test)]
pub type QdbAllocator = QdbTestAllocator;

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Unsafe_QdbAllocator_create(
    _env: JNIEnv,
    _class: JClass,
    rss_mem_limit: *const AtomicUsize,
    rss_mem_used: *mut AtomicUsize,
    mem_used: *mut AtomicUsize,
    tagged_used: *mut AtomicUsize,
) -> *mut QdbWatermarkAllocator {
    Box::into_raw(Box::new(QdbWatermarkAllocator::new(
        rss_mem_limit,
        rss_mem_used,
        mem_used,
        tagged_used,
    )))
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Unsafe_QdbAllocator_destroy(
    _env: JNIEnv,
    _class: JClass,
    allocator: *mut QdbWatermarkAllocator,
) {
    if allocator.is_null() {
        panic!("allocator pointer is null");
    }

    drop(unsafe { Box::from_raw(allocator) })
}
