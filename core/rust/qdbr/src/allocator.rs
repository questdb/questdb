/*******************************************************************************
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
use std::alloc::{AllocError, Allocator, Global, Layout};
use std::cell::RefCell;
use std::fmt::{Display, Formatter};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use std::sync::Arc;

// We use a thread local to store additional allocation error information.
// We do this since we can't pass it back via the Allocator trait's return type.
thread_local! {
    static ALLOC_ERROR: RefCell<Option<AllocFailure>> = const { RefCell::new(None) };
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
                write!(f, "out of memory when allocating {requested_size}")
            }
            AllocFailure::MemoryLimitExceeded {
                requested_size,
                memory_tag,
                rss_mem_limit,
                rss_mem_used,
            } => write!(
                f,
                "memory limit exceeded when allocating {requested_size} with tag {memory_tag} (rss used: {rss_mem_used}, rss limit: {rss_mem_limit})",
            ),
        }
    }
}

impl std::error::Error for AllocFailure {}

#[repr(C)]
pub struct MemTracking {
    /// Resident set size memory used. Updated on each allocation, reallocation and deallocation,
    /// also from Java. This is regardless of the memory tag used.
    rss_mem_used: AtomicUsize,

    /// Resident set size memory limit. Can be updated. Set to 0 for no explicit limit.
    rss_mem_limit: AtomicUsize,

    /// The total number of allocation calls.
    malloc_count: AtomicUsize,

    /// The total number of reallocation (grow or shrink) calls.
    realloc_count: AtomicUsize,

    /// The total number of free calls.
    free_count: AtomicUsize,

    /// Tracking non-rss memory, such as mmap.
    _non_rss_mem_used: AtomicUsize,
}

/// Custom allocator that fails once a memory limit watermark is reached.
/// It also tracks memory usage for a specific memory tag.
/// See `Unsafe.java` for the Java side of this.
#[derive(Clone)]
#[cfg(not(test))]
#[repr(C, packed)]
pub struct QdbAllocator {
    /// Global counters
    pub mem_tracking: *const MemTracking,

    /// The total memory used for the specific memory tag.
    pub tagged_used: *const AtomicUsize,

    /// Memory category. See `MemoryTag.java` for possible values.
    memory_tag: i32,
}

#[cfg(test)]
#[derive(Clone)]
pub struct QdbAllocator {
    mem_tracking: Arc<MemTracking>,
    tagged_used: Arc<AtomicUsize>,
    memory_tag: i32,
}

const RSS_ORDERING: Ordering = Ordering::SeqCst;
const COUNTER_ORDERING: Ordering = Ordering::AcqRel;

#[cfg(not(test))]
impl QdbAllocator {
    fn rss_mem_used(&self) -> &AtomicUsize {
        unsafe { &(*self.mem_tracking).rss_mem_used }
    }

    fn rss_mem_limit(&self) -> &AtomicUsize {
        unsafe { &(*self.mem_tracking).rss_mem_limit }
    }

    fn malloc_count(&self) -> &AtomicUsize {
        unsafe { &(*self.mem_tracking).malloc_count }
    }

    fn realloc_count(&self) -> &AtomicUsize {
        unsafe { &(*self.mem_tracking).realloc_count }
    }

    fn free_count(&self) -> &AtomicUsize {
        unsafe { &(*self.mem_tracking).free_count }
    }

    fn tagged_used(&self) -> &AtomicUsize {
        unsafe { &*self.tagged_used }
    }
}

#[cfg(test)]
impl QdbAllocator {
    fn rss_mem_used(&self) -> &AtomicUsize {
        &self.mem_tracking.rss_mem_used
    }

    fn rss_mem_limit(&self) -> &AtomicUsize {
        &self.mem_tracking.rss_mem_limit
    }

    fn malloc_count(&self) -> &AtomicUsize {
        &self.mem_tracking.malloc_count
    }

    fn realloc_count(&self) -> &AtomicUsize {
        &self.mem_tracking.realloc_count
    }

    fn free_count(&self) -> &AtomicUsize {
        &self.mem_tracking.free_count
    }

    fn tagged_used(&self) -> &AtomicUsize {
        &self.tagged_used
    }
}

impl QdbAllocator {
    fn check_alloc_limit(&self, requested_size: usize) -> Result<(), AllocError> {
        let rss_mem_limit = self.rss_mem_limit().load(RSS_ORDERING);
        if rss_mem_limit > 0 {
            let rss_mem_used = self.rss_mem_used().load(RSS_ORDERING);
            let new_rss_mem_used = rss_mem_used + requested_size;
            if new_rss_mem_used > rss_mem_limit {
                ALLOC_ERROR.with(|error| {
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
        self.track_allocate(allocated.len());
        Ok(allocated)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.check_alloc_limit(layout.size())?;
        let allocated = Global
            .allocate_zeroed(layout)
            .map_err(|error| save_oom_err(error, layout.size()))?;
        self.track_allocate(allocated.len());
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
        let true_delta = allocated.len() - old_layout.size();
        self.track_grow(true_delta);
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
        let true_delta = allocated.len() - old_layout.size();
        self.track_grow(true_delta);
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
        let true_delta = old_layout.size() - allocated.len();
        self.track_shrink(true_delta);
        Ok(allocated)
    }
}

pub type AcVec<T> = alloc_checked::vec::Vec<T, QdbAllocator>;

#[cfg(test)]
pub struct TestAllocatorState {
    mem_tracking: Arc<MemTracking>,
    tagged_used: Arc<AtomicUsize>,
}

#[cfg(test)]
impl TestAllocatorState {
    pub fn new() -> Self {
        let mem_tracking = Arc::new(MemTracking {
            rss_mem_used: AtomicUsize::new(0),
            rss_mem_limit: AtomicUsize::new(0),
            malloc_count: AtomicUsize::new(0),
            realloc_count: AtomicUsize::new(0),
            free_count: AtomicUsize::new(0),
            _non_rss_mem_used: AtomicUsize::new(0),
        });

        let tagged_used = Arc::new(AtomicUsize::new(0));

        Self { mem_tracking, tagged_used }
    }

    pub fn allocator(&self) -> QdbAllocator {
        QdbAllocator {
            mem_tracking: self.mem_tracking.clone(),
            tagged_used: self.tagged_used.clone(),
            memory_tag: 65,
        }
    }

    pub fn rss_mem_used(&self) -> usize {
        self.mem_tracking.rss_mem_used.load(Ordering::SeqCst)
    }

    pub fn rss_mem_limit(&self) -> usize {
        self.mem_tracking.rss_mem_limit.load(Ordering::SeqCst)
    }

    pub fn set_mem_rss_limit(&self, limit: usize) {
        self.mem_tracking
            .rss_mem_limit
            .store(limit, Ordering::SeqCst);
    }

    pub fn malloc_count(&self) -> usize {
        self.mem_tracking.malloc_count.load(Ordering::SeqCst)
    }

    pub fn realloc_count(&self) -> usize {
        self.mem_tracking.realloc_count.load(Ordering::SeqCst)
    }

    pub fn free_count(&self) -> usize {
        self.mem_tracking.free_count.load(Ordering::SeqCst)
    }

    pub fn tagged_used(&self) -> usize {
        self.tagged_used.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use crate::allocator::{take_last_alloc_error, AllocFailure, TestAllocatorState};
    use rand::Rng;
    use std::alloc::Allocator;
    use std::ptr::NonNull;
    use std::sync::{Arc, Barrier};

    #[test]
    fn test_mem_tracking_size() {
        assert_eq!(
            size_of::<crate::allocator::MemTracking>(),
            6 * size_of::<u64>()
        );
    }

    #[test]
    fn test_alloc_fail() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let too_large = 1024 * 1024 * 1024 * 1024 * 1024; // 1024 TB
        let layout = std::alloc::Layout::from_size_align(too_large, 16).unwrap();
        let result = allocator.allocate(layout);
        assert!(result.is_err());
        let last_err = take_last_alloc_error().unwrap();
        assert!(matches!(last_err, AllocFailure::OutOfMemory { .. }));
    }

    #[test]
    fn test_alloc_with_limit() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // Limit the memory to 1KiB.
        tas.set_mem_rss_limit(1024);

        assert_eq!(tas.rss_mem_used(), 0);
        assert_eq!(tas.tagged_used(), 0);
        assert_eq!(tas.rss_mem_limit(), 1024);

        // Ask for 64 bytes, should be fine.
        {
            let layout = std::alloc::Layout::from_size_align(64, 16).unwrap();
            let allocation = allocator.allocate(layout).unwrap();
            assert_eq!(tas.malloc_count(), 1);
            assert_eq!(tas.realloc_count(), 0);
            assert_eq!(tas.free_count(), 0);
            assert_eq!(tas.tagged_used(), 64);

            // Free said allocation.
            let allocation = NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
            unsafe { allocator.deallocate(allocation, layout) };
            assert_eq!(tas.malloc_count(), 1);
            assert_eq!(tas.realloc_count(), 0);
            assert_eq!(tas.free_count(), 1);
            assert_eq!(tas.tagged_used(), 0);
        }

        // Now allocate in excess of the limit, should fail.
        {
            let layout = std::alloc::Layout::from_size_align(2048, 16).unwrap();
            let result = allocator.allocate(layout);
            assert!(result.is_err());
            let last_err = take_last_alloc_error().unwrap();
            assert!(matches!(
                last_err,
                AllocFailure::MemoryLimitExceeded {
                    requested_size: 2048,
                    memory_tag: 65,
                    rss_mem_limit: 1024,
                    rss_mem_used: 0,
                }
            ));
            assert_eq!(tas.malloc_count(), 1);
            assert_eq!(tas.realloc_count(), 0);
            assert_eq!(tas.free_count(), 1);
        }
    }

    #[test]
    fn test_grow() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        // Allocate 64 bytes.
        let layout1 = std::alloc::Layout::from_size_align(64, 16).unwrap();
        let allocation1 = allocator.allocate(layout1).unwrap();
        assert_eq!(tas.rss_mem_used(), 64);
        assert_eq!(tas.tagged_used(), 64);
        assert_eq!(tas.malloc_count(), 1);
        assert_eq!(tas.realloc_count(), 0);
        assert_eq!(tas.free_count(), 0);

        // Grow the allocation to 128 bytes.
        let allocation1 = NonNull::new(allocation1.as_ptr() as *mut u8).unwrap();
        let layout2 = std::alloc::Layout::from_size_align(128, 16).unwrap();
        let allocation2 = unsafe { allocator.grow(allocation1, layout1, layout2).unwrap() };
        assert_eq!(tas.rss_mem_used(), 128);
        assert_eq!(tas.tagged_used(), 128);
        assert_eq!(tas.malloc_count(), 1);
        assert_eq!(tas.realloc_count(), 1);
        assert_eq!(tas.free_count(), 0);

        // Shrink the allocation to 96 bytes.
        let allocation2 = NonNull::new(allocation2.as_ptr() as *mut u8).unwrap();
        let layout3 = std::alloc::Layout::from_size_align(96, 16).unwrap();
        let allocation3 = unsafe { allocator.shrink(allocation2, layout2, layout3).unwrap() };
        assert_eq!(tas.rss_mem_used(), 96);
        assert_eq!(tas.tagged_used(), 96);
        assert_eq!(tas.malloc_count(), 1);
        assert_eq!(tas.realloc_count(), 2);
        assert_eq!(tas.free_count(), 0);

        // Free
        let allocation3 = NonNull::new(allocation3.as_ptr() as *mut u8).unwrap();
        unsafe { allocator.deallocate(allocation3, layout3) };
        assert_eq!(tas.rss_mem_used(), 0);
        assert_eq!(tas.tagged_used(), 0);
        assert_eq!(tas.malloc_count(), 1);
        assert_eq!(tas.realloc_count(), 2);
        assert_eq!(tas.free_count(), 1);
    }

    #[test]
    fn test_parallel() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let thread_count = 64;
        let barrier = Arc::new(Barrier::new(thread_count));
        let mut threads = Vec::with_capacity(thread_count);
        let alignments = [8, 16, 32, 64, 128];

        #[derive(Clone, Copy)]
        enum Op {
            Allocate,
            AllocateZeroed,
            Grow,
            GrowZeroed,
            Shrink,
            Deallocate,
        }

        let ops = [
            Op::Allocate,
            Op::AllocateZeroed,
            Op::Grow,
            Op::GrowZeroed,
            Op::Shrink,
            Op::Deallocate,
        ];

        for _ in 0..thread_count {
            let mut alloc_count = 0;
            let mut realloc_count = 0;
            let allocator = allocator.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                let mut allocations = Vec::with_capacity(2048);
                let mut rng = rand::rng();
                barrier.wait();
                let iterations = 10_000usize;
                // Keep on allocating and deallocating randomly.
                for _ in 0..iterations {
                    let op_index = rng.random_range(0..ops.len());
                    let op = ops[op_index];
                    match op {
                        Op::Allocate => {
                            let size = rng.random_range(1..=1024);
                            let alignment_index = rng.random_range(0..=4);
                            let alignment = alignments[alignment_index];
                            let layout = std::alloc::Layout::from_size_align(size, alignment)
                                .expect("layout creation failed");
                            let allocation = allocator.allocate(layout).unwrap();
                            allocations.push((allocation, layout));
                            alloc_count += 1;
                        }
                        Op::AllocateZeroed => {
                            let size = rng.random_range(1..=1024);
                            let alignment_index = rng.random_range(0..=4);
                            let alignment = alignments[alignment_index];
                            let layout = std::alloc::Layout::from_size_align(size, alignment)
                                .expect("layout creation failed");
                            let allocation = allocator.allocate_zeroed(layout).unwrap();
                            allocations.push((allocation, layout));
                            alloc_count += 1;
                        }
                        Op::Grow => {
                            // grow one at random, if any
                            if !allocations.is_empty() {
                                let grow_index = rng.random_range(0..allocations.len());
                                let (allocation, layout) = allocations[grow_index];
                                let allocation =
                                    NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
                                let new_size = layout.size() + rng.random_range(1..=1024);
                                let new_layout =
                                    std::alloc::Layout::from_size_align(new_size, layout.align())
                                        .expect("layout creation failed");
                                let allocation = unsafe {
                                    allocator.grow(allocation, layout, new_layout).unwrap()
                                };
                                allocations[grow_index] = (allocation, new_layout);
                                realloc_count += 1;
                            }
                        }
                        Op::GrowZeroed => {
                            // grow one at random, if any
                            if !allocations.is_empty() {
                                let grow_index = rng.random_range(0..allocations.len());
                                let (allocation, layout) = allocations[grow_index];
                                let allocation =
                                    NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
                                let new_size = layout.size() + rng.random_range(1..=1024);
                                let new_layout =
                                    std::alloc::Layout::from_size_align(new_size, layout.align())
                                        .expect("layout creation failed");
                                let allocation = unsafe {
                                    allocator
                                        .grow_zeroed(allocation, layout, new_layout)
                                        .unwrap()
                                };
                                allocations[grow_index] = (allocation, new_layout);
                                realloc_count += 1;
                            }
                        }
                        Op::Shrink => {
                            // shrink one at random, if any
                            if !allocations.is_empty() {
                                let shrink_index = rng.random_range(0..allocations.len());
                                let (allocation, layout) = allocations[shrink_index];
                                let allocation =
                                    NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
                                if layout.size() > 1 {
                                    let shrink_by = rng.random_range(1..layout.size());
                                    assert!(shrink_by < layout.size());
                                    let new_size = layout.size() - shrink_by;
                                    let new_layout = std::alloc::Layout::from_size_align(
                                        new_size,
                                        layout.align(),
                                    )
                                    .expect("layout creation failed");
                                    let allocation = unsafe {
                                        allocator.shrink(allocation, layout, new_layout).unwrap()
                                    };
                                    allocations[shrink_index] = (allocation, new_layout);
                                    realloc_count += 1;
                                }
                            }
                        }
                        Op::Deallocate => {
                            // dealloc one at random, if any
                            if !allocations.is_empty() {
                                let pop_index = rng.random_range(0..allocations.len());
                                let (allocation, layout) = allocations.remove(pop_index);
                                let allocation =
                                    NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
                                unsafe { allocator.deallocate(allocation, layout) };
                            }
                        }
                    }
                }

                // Deallocate all remaining allocations.
                for (allocation, layout) in allocations {
                    let allocation = NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
                    unsafe { allocator.deallocate(allocation, layout) };
                }

                (alloc_count, realloc_count)
            }));
        }

        let (total_allocs, total_reallocs) = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .fold((0, 0), |(acc_allocs, acc_reallocs), (allocs, reallocs)| {
                (acc_allocs + allocs, acc_reallocs + reallocs)
            });

        assert_eq!(tas.rss_mem_used(), 0);
        assert_eq!(tas.tagged_used(), 0);
        assert_eq!(tas.malloc_count(), total_allocs);
        assert_eq!(tas.realloc_count(), total_reallocs);
        assert_eq!(tas.free_count(), total_allocs);
    }
}
