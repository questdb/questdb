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
    static ALLOC_ERROR: std::cell::RefCell<Option<AllocFailure>> = const { std::cell::RefCell::new(None) };
}

/// Takes (and clears) the last allocation error that occurred.
/// This may be `None` if no error occurred, or if the error was cleared.
/// This operates on top of a thread-local and augments the (lack of) details
/// provided by the `AllocError` type.
pub fn take_last_alloc_error() -> Option<AllocFailure> {
    ALLOC_ERROR.with(|error| error.borrow_mut().take())
}

#[derive(Debug, Copy, Clone)]
pub enum AllocFailure {
    /// The underlying allocator failed to allocate memory.
    OutOfMemory {
        /// What was requested.
        requested_size: usize,
    },

    /// A memory limit would have been exceeded.
    MemoryLimitExceeded {
        /// What was requested.
        requested_size: usize,

        /// The memory tag that was used for the allocation request.
        memory_tag: i32,

        /// The memory limit at the time of the breach.
        rss_mem_limit: usize,

        /// The memory used at the time of the breach.
        rss_mem_used: usize,
    },
}

fn save_oom_err(alloc_error: AllocError, requested_size: usize) -> AllocError {
    ALLOC_ERROR.with(|error| {
        *error.borrow_mut() = Some(AllocFailure::OutOfMemory { requested_size });
    });
    alloc_error
}

impl Display for AllocFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AllocFailure::OutOfMemory { requested_size } => {
                write!(f, "out of memory when allocating {}", requested_size)
            }
            AllocFailure::MemoryLimitExceeded {
                requested_size,
                memory_tag,
                rss_mem_limit,
                rss_mem_used,
            } => write!(
                f,
                "memory limit exceeded when allocating {} with tag {} (rss used: {}, rss limit: {})",
                requested_size, memory_tag, rss_mem_used, rss_mem_limit
            ),
        }
    }
}

impl std::error::Error for AllocFailure {}

#[repr(C, packed)]
struct MemTracking {
    /// Resident set size memory used. Updated on each allocation, reallocation and deallocation,
    /// also from Java. This is regardless of the memory tag used.
    rss_mem_used: &'static AtomicUsize,

    /// Resident set size memory limit. Can be updated. Set to 0 for no explicit limit.
    rss_mem_limit: &'static AtomicUsize,

    /// The total number of allocation calls.
    malloc_count: &'static AtomicUsize,

    /// The total number of reallocation (grow or shrink) calls.
    realloc_count: &'static AtomicUsize,

    /// The total number of free calls.
    free_count: &'static AtomicUsize,

    /// Tracking non-rss memory, such as mmap.
    _non_rss_mem_used: &'static AtomicUsize,
}

/// Custom allocator that fails once a memory limit watermark is reached.
/// It also tracks memory usage for a specific memory tag.
/// See `Unsafe.java` for the Java side of this.
#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct QdbAllocator {
    /// Global counters
    mem_tracking: &'static MemTracking,

    /// The total memory used for the specific memory tag.
    tagged_used: &'static AtomicUsize,

    /// Memory category. See `MemoryTag.java` for possible values.
    memory_tag: i32,
}

const RSS_ORDERING: Ordering = Ordering::SeqCst;
const COUNTER_ORDERING: Ordering = Ordering::AcqRel;

impl QdbAllocator {
    fn rss_mem_used(&self) -> &AtomicUsize {
        &*(*self.mem_tracking).rss_mem_used
    }

    fn rss_mem_limit(&self) -> &AtomicUsize {
        &*(*self.mem_tracking).rss_mem_limit
    }

    fn malloc_count(&self) -> &AtomicUsize {
        &*(*self.mem_tracking).malloc_count
    }

    fn realloc_count(&self) -> &AtomicUsize {
        &*(*self.mem_tracking).realloc_count
    }

    fn free_count(&self) -> &AtomicUsize {
        &*(*self.mem_tracking).free_count
    }

    fn tagged_used(&self) -> &AtomicUsize {
        &*self.tagged_used
    }

    fn check_alloc_limit(&self, requested_size: usize) -> Result<(), AllocError> {
        let rss_mem_limit = self.rss_mem_limit().load(RSS_ORDERING);
        if rss_mem_limit > 0 {
            let rss_mem_used = self.rss_mem_used().load(RSS_ORDERING);
            let new_rss_mem_used = rss_mem_used + requested_size;
            if new_rss_mem_used > rss_mem_limit {
                ALLOC_ERROR.with(|error| {
                    // eprintln!("    ---> failed! {:?}", failure);
                    *error.borrow_mut() = Some(AllocFailure::MemoryLimitExceeded {
                        memory_tag: self.memory_tag,
                        requested_size,
                        rss_mem_limit,
                        rss_mem_used,
                    });
                });
                return Err(AllocError);
            }
        }
        Ok(())
    }

    fn track_allocate(&self, malloced_size: usize) {
        self.tagged_used()
            .fetch_add(malloced_size, COUNTER_ORDERING);
        self.rss_mem_used()
            .fetch_add(malloced_size, COUNTER_ORDERING);
        self.malloc_count().fetch_add(1, COUNTER_ORDERING);
    }

    fn track_grow(&self, delta: usize) {
        self.tagged_used().fetch_add(delta, COUNTER_ORDERING);
        self.rss_mem_used().fetch_add(delta, COUNTER_ORDERING);
        self.realloc_count().fetch_add(1, COUNTER_ORDERING);
    }

    fn track_shrink(&self, delta: usize) {
        self.tagged_used().fetch_sub(delta, COUNTER_ORDERING);
        self.rss_mem_used().fetch_sub(delta, COUNTER_ORDERING);
        self.realloc_count().fetch_add(1, COUNTER_ORDERING);
    }

    fn track_deallocate(&self, freed_size: usize) {
        self.tagged_used().fetch_sub(freed_size, COUNTER_ORDERING);
        self.rss_mem_used().fetch_sub(freed_size, COUNTER_ORDERING);
        self.free_count().fetch_add(1, COUNTER_ORDERING);
    }
}

unsafe impl Allocator for QdbAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.check_alloc_limit(layout.size())?;
        let allocated = Global
            .allocate(layout)
            .map_err(|error| save_oom_err(error, layout.size()))?;
        self.track_allocate(layout.size());
        Ok(allocated)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.check_alloc_limit(layout.size())?;
        let allocated = Global
            .allocate_zeroed(layout)
            .map_err(|error| save_oom_err(error, layout.size()))?;
        self.track_allocate(layout.size());
        Ok(allocated)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        Global.deallocate(ptr, layout);
        self.track_deallocate(layout.size());
    }

    unsafe fn grow(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        assert!(new_layout.size() > old_layout.size());
        let delta = new_layout.size() - old_layout.size();
        self.check_alloc_limit(delta)?;
        let allocated = Global
            .grow(ptr, old_layout, new_layout)
            .map_err(|error| save_oom_err(error, delta))?;
        self.track_grow(delta);
        Ok(allocated)
    }

    unsafe fn grow_zeroed(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        assert!(new_layout.size() > old_layout.size());
        let delta = new_layout.size() - old_layout.size();
        self.check_alloc_limit(delta)?;
        let allocated = Global
            .grow_zeroed(ptr, old_layout, new_layout)
            .map_err(|error| save_oom_err(error, delta))?;
        self.track_grow(delta);
        Ok(allocated)
    }

    unsafe fn shrink(
        &self,
        ptr: NonNull<u8>,
        old_layout: Layout,
        new_layout: Layout,
    ) -> Result<NonNull<[u8]>, AllocError> {
        assert!(new_layout.size() < old_layout.size());
        let delta = old_layout.size() - new_layout.size();
        let allocated = Global
            .shrink(ptr, old_layout, new_layout)
            .map_err(|error| save_oom_err(error, delta))?;
        self.track_shrink(delta);
        Ok(allocated)
    }
}

pub type AcVec<T> = alloc_checked::vec::Vec<T, QdbAllocator>;

#[cfg(test)]
pub static RSS_MEM_USED: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub static RSS_MEM_LIMIT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub static MALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub static REALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub static FREE_COUNT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub static NON_RSS_MEM_USED: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub static TAGGED_USED: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static TEST_MEM_TRACKING: MemTracking = MemTracking {
    rss_mem_used: &RSS_MEM_USED,
    rss_mem_limit: &RSS_MEM_LIMIT,
    malloc_count: &MALLOC_COUNT,
    realloc_count: &REALLOC_COUNT,
    free_count: &FREE_COUNT,
    _non_rss_mem_used: &NON_RSS_MEM_USED,
};

#[cfg(test)]
pub static TEST_ALLOCATOR: QdbAllocator = QdbAllocator {
    mem_tracking: &TEST_MEM_TRACKING,
    tagged_used: &TAGGED_USED,
    memory_tag: 64,
};

#[cfg(test)]
pub struct TestAllocatorLimitGuard {}

#[cfg(test)]
impl TestAllocatorLimitGuard {
    pub fn new(limit: usize) -> Self {
        RSS_MEM_LIMIT.store(limit, RSS_ORDERING);
        Self {}
    }
}

#[cfg(test)]
impl Drop for TestAllocatorLimitGuard {
    fn drop(&mut self) {
        RSS_MEM_LIMIT.store(0, RSS_ORDERING);
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::Allocator;
    use std::ptr::NonNull;
    use std::sync::atomic::AtomicUsize;
    use crate::allocator::{take_last_alloc_error, AllocFailure, MemTracking, QdbAllocator, TestAllocatorLimitGuard, RSS_MEM_LIMIT, RSS_MEM_USED, TEST_ALLOCATOR};

    #[test]
    fn test_size_assumptions() {
        // We rely on these sizes in `Unsafe.java`.
        assert_eq!(size_of::<&'static AtomicUsize>(), size_of::<*const AtomicUsize>());
        assert_eq!(size_of::<&'static AtomicUsize>(), 8);

        assert_eq!(size_of::<&'static MemTracking>(), size_of::<*const MemTracking>());
        assert_eq!(size_of::<&'static MemTracking>(), 8);

        assert_eq!(size_of::<MemTracking>(), 6 * 8);
        assert_eq!(size_of::<QdbAllocator>(), 8 + 8 + 4);
    }

    #[test]
    fn test_alloc_fail() {
        let allocator = TEST_ALLOCATOR;
        let too_large = 1024 * 1024 * 1024 * 1024 * 1024;  // 1024 TB
        let layout = std::alloc::Layout::from_size_align(too_large, 8).unwrap();
        let result = allocator.allocate(layout);
        assert!(result.is_err());
        let last_err = take_last_alloc_error().unwrap();
        assert!(matches!(last_err, AllocFailure::OutOfMemory { .. }));
    }

    #[test]
    fn test_alloc_with_limit() {
        let allocator = TEST_ALLOCATOR;
        // Limit the memory to 1KiB.
        let limit_guard = TestAllocatorLimitGuard::new(1024);

        assert_eq!(RSS_MEM_USED.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(TEST_ALLOCATOR.tagged_used().load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(RSS_MEM_LIMIT.load(std::sync::atomic::Ordering::SeqCst), 1024);

        // Ask for 64 bytes, should be fine.
        {
            let layout = std::alloc::Layout::from_size_align(64, 8).unwrap();
            let allocation = allocator.allocate(layout).unwrap();

            // Free said allocation.
            let allocation = NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
            unsafe { allocator.deallocate(allocation, layout) };
        }


        // Now allocate in excess of the limit, should fail.
        {
            let layout = std::alloc::Layout::from_size_align(2048, 8).unwrap();
            let result = allocator.allocate(layout);
            assert!(result.is_err());
            let last_err = take_last_alloc_error().unwrap();
            assert!(matches!(last_err, AllocFailure::MemoryLimitExceeded {
                requested_size: 2048,
                memory_tag: 64,
                rss_mem_limit: 1024,
                rss_mem_used: 0,
            }));
        }

        drop(limit_guard);
        assert_eq!(RSS_MEM_LIMIT.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}