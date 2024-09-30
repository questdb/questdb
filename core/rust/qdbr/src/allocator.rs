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
use std::alloc::{AllocError, Allocator, Global, Layout};
use std::fmt::{Display, Formatter};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

// We use a thread local to store additional allocation error information.
// We do this since we can't pass it back via the Allocator trait's return type.
thread_local! {
    static ALLOC_ERROR: std::cell::RefCell<Option<AllocFailure>> = std::cell::RefCell::new(None);
}

#[derive(Debug, Copy, Clone)]
pub enum AllocFailure {
    /// The underlying allocator failed to allocate memory.
    OutOfMemory {
        /// What was requested.
        layout: Layout,
    },

    /// A memory limit would have been exceeded.
    MemoryLimitExceeded {
        /// What was requested.
        layout: Layout,

        /// The memory tag that was used for the allocation request.
        memory_tag: i32,

        /// The memory limit at the time of the breach.
        rss_mem_limit: usize,

        /// The memory used at the time of the breach.
        rss_mem_used: usize,
    },
}

impl Display for AllocFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocFailure::OutOfMemory { layout } => {
                write!(f, "out of memory when allocating {} (alignment: {})", layout.size(), layout.align())
            }
            AllocFailure::MemoryLimitExceeded {
                layout,
                memory_tag,
                rss_mem_limit,
                rss_mem_used,
            } => write!(
                f,
                "memory limit exceeded when allocating {} with tag {} (alignment: {}, rss used: {}, rss limit: {})",
                layout.size(), memory_tag, layout.align(), rss_mem_used, rss_mem_limit
            ),
        }
    }
}

impl std::error::Error for AllocFailure {}

/// Custom allocator that fails once a memory limit watermark is reached.
/// It also tracks memory usage for a specific memory tag.
/// See `Unsafe.java` for the Java side of this.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct TaggedWatermarkAllocator {
    /// Resident set size memory limit. Can be updated. Set to 0 for no explicit limit.
    rss_mem_limit: *const AtomicUsize,

    /// Resident set size memory used. Updated on each allocation and deallocation, also from Java.
    /// This is regardless of the memory tag used.
    rss_mem_used: *mut AtomicUsize,

    /// The total memory used for the specific memory tag.
    tagged_used: *mut AtomicUsize,

    /// The total number of calls to `malloc`.
    malloc_count: *mut AtomicUsize,

    /// The number of free calls to `malloc`.
    free_count: *mut AtomicUsize,

    /// See `MemoryTag.java` for possible values.
    memory_tag: i32,
}

const RSS_ORDERING: Ordering = Ordering::SeqCst;
const COUNTER_ORDERING: Ordering = Ordering::AcqRel;

impl TaggedWatermarkAllocator {
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

    fn free_count(&self) -> &AtomicUsize {
        unsafe { &*self.free_count }
    }

    fn check_alloc_limit(&self, layout: Layout) -> Result<(), AllocFailure> {
        let rss_mem_limit = self.rss_mem_limit().load(RSS_ORDERING);
        if rss_mem_limit > 0 {
            let rss_mem_used = self.rss_mem_used().load(RSS_ORDERING);
            let new_rss_mem_used = rss_mem_used + layout.size();
            if new_rss_mem_used > rss_mem_limit {
                return Err(AllocFailure::MemoryLimitExceeded {
                    memory_tag: self.memory_tag,
                    layout,
                    rss_mem_limit,
                    rss_mem_used,
                });
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
        self.free_count().fetch_add(1, COUNTER_ORDERING);
    }
}

unsafe impl Allocator for TaggedWatermarkAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.check_alloc_limit(layout).map_err(|failure| {
            ALLOC_ERROR.with(|error| {
                *error.borrow_mut() = Some(failure);
            });
            AllocError
        })?;
        let allocated = Global.allocate(layout).map_err(|error| {
            ALLOC_ERROR.with(|error| {
                *error.borrow_mut() = Some(AllocFailure::OutOfMemory { layout });
            });
            error
        })?;
        self.add_memory_alloc(layout);
        Ok(allocated)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        Global.deallocate(ptr, layout);
        self.sub_memory_alloc(layout);
    }
}

/// Takes (and clears) the last allocation error that occurred.
/// This may be `None` if no error occurred, or if the error was cleared.
/// This operates on top of a thread-local and augments the (lack of) details
/// provided by the `AllocError` type.
pub fn take_last_alloc_error() -> Option<AllocFailure> {
    ALLOC_ERROR.with(|error| error.borrow_mut().take())
}

#[allow(dead_code)] // TODO(amunra): remove once in use
#[cfg(test)]
#[derive(Clone, Copy)]
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

#[cfg(not(test))]
pub type QdbAllocator = TaggedWatermarkAllocator;

#[allow(dead_code)] // TODO(amunra): remove once in use
#[cfg(test)]
pub type QdbAllocator = QdbTestAllocator;

pub type AcVec<T> = alloc_checked::vec::Vec<T, QdbAllocator>;
