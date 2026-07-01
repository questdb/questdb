/*+*****************************************************************************
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

use qdb_core::memory_tracker::MemoryTracker;

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

/// Identifies which limit was breached when an allocation is rejected.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum AllocScope {
    /// The global RSS memory limit, shared by every allocation.
    Global,
    /// The per-workload memory tracker bound to this allocator.
    Query,
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

        /// Which scope tripped the breach: global or per-query.
        scope: AllocScope,

        /// The memory limit at the time of the breach (corresponds to `scope`).
        rss_mem_limit: usize,

        /// The memory used at the time of the breach (corresponds to `scope`).
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
                scope,
                rss_mem_limit,
                rss_mem_used,
            } => {
                let scope_label = match scope {
                    AllocScope::Global => "global",
                    AllocScope::Query => "query",
                };
                write!(
                    f,
                    "{scope_label} memory limit exceeded when allocating {requested_size} with tag {memory_tag} (used: {rss_mem_used}, limit: {rss_mem_limit})",
                )
            }
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

impl Default for MemTracking {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTracking {
    pub const fn new() -> Self {
        Self {
            rss_mem_used: AtomicUsize::new(0),
            rss_mem_limit: AtomicUsize::new(0),
            malloc_count: AtomicUsize::new(0),
            realloc_count: AtomicUsize::new(0),
            free_count: AtomicUsize::new(0),
            _non_rss_mem_used: AtomicUsize::new(0),
        }
    }
}

/// Custom allocator that fails once a memory limit watermark is reached.
/// It also tracks memory usage for a specific memory tag.
/// See `Unsafe.java` for the Java side of this.
#[derive(Clone)]
#[cfg(not(test))]
#[repr(C, packed)]
pub struct QdbAllocator {
    /// Global counters.
    pub mem_tracking: *const MemTracking,

    /// Per-workload counters. `null` when the allocator is not bound to a
    /// per-query memory tracker (the OSS default for table-pool, log, and
    /// other process-lifetime allocations).
    pub memory_tracker: *const MemoryTracker,

    /// The total memory used for the specific memory tag.
    pub tagged_used: *const AtomicUsize,

    /// Memory category. See `MemoryTag.java` for possible values.
    memory_tag: i32,
}

#[cfg(test)]
#[derive(Clone)]
pub struct QdbAllocator {
    mem_tracking: Arc<MemTracking>,
    memory_tracker: Option<Arc<MemoryTracker>>,
    tagged_used: Arc<AtomicUsize>,
    memory_tag: i32,
}

const RSS_ORDERING: Ordering = Ordering::SeqCst;
const COUNTER_ORDERING: Ordering = Ordering::AcqRel;
const MIN_ALLOC_ALIGNMENT: usize = std::mem::align_of::<u128>();

#[cfg(not(test))]
impl QdbAllocator {
    pub fn new(
        mem_tracking: *const MemTracking,
        memory_tracker: *const MemoryTracker,
        tagged_used: *const AtomicUsize,
        memory_tag: i32,
    ) -> Self {
        Self {
            mem_tracking,
            memory_tracker,
            tagged_used,
            memory_tag,
        }
    }

    fn rss_mem_used(&self) -> &AtomicUsize {
        unsafe { &(*self.mem_tracking).rss_mem_used }
    }

    fn rss_mem_limit(&self) -> &AtomicUsize {
        unsafe { &(*self.mem_tracking).rss_mem_limit }
    }

    fn memory_tracker(&self) -> Option<&MemoryTracker> {
        if self.memory_tracker.is_null() {
            None
        } else {
            Some(unsafe { &*self.memory_tracker })
        }
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

    fn memory_tracker(&self) -> Option<&MemoryTracker> {
        self.memory_tracker.as_deref()
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
    fn aligned_layout(layout: Layout) -> Result<Layout, AllocError> {
        let alignment = layout.align().max(MIN_ALLOC_ALIGNMENT);
        // Raising the alignment lowers the size ceiling Layout permits, so a
        // request within MIN_ALLOC_ALIGNMENT bytes of isize::MAX is rejected
        // here. Report it as an allocation failure instead of panicking; a
        // panic in JNI-called Rust aborts the JVM.
        Layout::from_size_align(layout.size(), alignment)
            .map_err(|_| save_oom_err(AllocError, layout.size()))
    }

    fn check_alloc_limit(&self, requested_size: usize) -> Result<(), AllocError> {
        // Global RSS limit first so its diagnostic wins when both are breached.
        let rss_mem_limit = self.rss_mem_limit().load(RSS_ORDERING);
        if rss_mem_limit > 0 {
            let rss_mem_used = self.rss_mem_used().load(RSS_ORDERING);
            let new_rss_mem_used = rss_mem_used.saturating_add(requested_size);
            if new_rss_mem_used > rss_mem_limit {
                ALLOC_ERROR.with(|error| {
                    *error.borrow_mut() = Some(AllocFailure::MemoryLimitExceeded {
                        memory_tag: self.memory_tag,
                        requested_size,
                        scope: AllocScope::Global,
                        rss_mem_limit,
                        rss_mem_used,
                    });
                });
                return Err(AllocError);
            }
        }
        // Per-workload limit, only when a tracker is bound. Resolve the tracker once
        // rather than null-checking it twice for its two counter words.
        if let Some(tracker) = self.memory_tracker() {
            let limit = tracker.limit();
            if limit > 0 {
                let used = tracker.used();
                if used.saturating_add(requested_size) > limit {
                    ALLOC_ERROR.with(|error| {
                        *error.borrow_mut() = Some(AllocFailure::MemoryLimitExceeded {
                            memory_tag: self.memory_tag,
                            requested_size,
                            scope: AllocScope::Query,
                            rss_mem_limit: limit,
                            rss_mem_used: used,
                        });
                    });
                    return Err(AllocError);
                }
            }
        }
        Ok(())
    }

    /// Charges the requested `layout.size()`, not the slice length the allocator
    /// returned. `deallocate` only ever sees `layout.size()` (the true length is
    /// not recoverable at free time), so tracking the requested size on both
    /// sides keeps the counters exactly balanced. An allocator that returns
    /// excess capacity would otherwise drift `tracker_used` upward forever, and
    /// a drifted counter makes every later `check_alloc_limit` spuriously fail.
    /// `check_alloc_limit` already gates on the requested size, so this also
    /// keeps the limit check and the counter consistent. With std `Global`, which
    /// returns exactly `layout.size()`, the two are identical anyway.
    fn track_allocate(&self, requested_size: usize) {
        self.tagged_used()
            .fetch_add(requested_size, COUNTER_ORDERING);
        self.rss_mem_used()
            .fetch_add(requested_size, COUNTER_ORDERING);
        if let Some(tracker) = self.memory_tracker() {
            tracker.charge_unchecked(requested_size);
        }
        self.malloc_count().fetch_add(1, COUNTER_ORDERING);
    }

    fn track_grow(&self, delta: usize) {
        self.tagged_used().fetch_add(delta, COUNTER_ORDERING);
        self.rss_mem_used().fetch_add(delta, COUNTER_ORDERING);
        if let Some(tracker) = self.memory_tracker() {
            tracker.charge_unchecked(delta);
        }
        self.realloc_count().fetch_add(1, COUNTER_ORDERING);
    }

    fn track_shrink(&self, delta: usize) {
        self.tagged_used().fetch_sub(delta, COUNTER_ORDERING);
        self.rss_mem_used().fetch_sub(delta, COUNTER_ORDERING);
        if let Some(tracker) = self.memory_tracker() {
            let prev = tracker.credit(delta);
            debug_assert!(
                prev >= delta,
                "per-query memory underflow on shrink: used={prev}, delta={delta}"
            );
        }
        self.realloc_count().fetch_add(1, COUNTER_ORDERING);
    }

    fn track_deallocate(&self, freed_size: usize) {
        self.tagged_used().fetch_sub(freed_size, COUNTER_ORDERING);
        self.rss_mem_used().fetch_sub(freed_size, COUNTER_ORDERING);
        if let Some(tracker) = self.memory_tracker() {
            let prev = tracker.credit(freed_size);
            debug_assert!(
                prev >= freed_size,
                "per-query memory underflow on deallocate: used={prev}, delta={freed_size}"
            );
        }
        self.free_count().fetch_add(1, COUNTER_ORDERING);
    }

    /// Charges `bytes` of externally-allocated native memory against the
    /// per-query tracker after checking the configured limits. Returns an
    /// error (recording the breach via `take_last_alloc_error`, exactly as a
    /// rejected `allocate` does) when the per-query or global limit would be
    /// crossed.
    ///
    /// Used for native memory QuestDB allocates outside this allocator yet
    /// still wants the per-query limit to bound -- notably the Parquet
    /// VarcharSlice decode `page_buffers`, whose payload bytes are backed by
    /// the system allocator rather than an `AcVec`. Only the per-query counter
    /// is adjusted; the RSS and tagged counters are deliberately left untouched
    /// because the backing allocation is already invisible to them. Every
    /// successful charge must be balanced by an equal `credit_tracked` once the
    /// bytes are released, or the per-query counter drifts upward.
    pub fn charge_tracked(&self, bytes: usize) -> Result<(), AllocError> {
        if bytes == 0 {
            return Ok(());
        }
        self.check_alloc_limit(bytes)?;
        if let Some(tracker) = self.memory_tracker() {
            tracker.charge_unchecked(bytes);
        }
        Ok(())
    }

    /// Releases `bytes` previously reserved with `charge_tracked`, clamping the
    /// per-query counter at zero on the impossible underflow (see
    /// [`MemoryTracker::credit`]). The RSS and tagged counters are left
    /// untouched, mirroring `charge_tracked`.
    pub fn credit_tracked(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        if let Some(tracker) = self.memory_tracker() {
            tracker.credit(bytes);
        }
    }
}

unsafe impl Allocator for QdbAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.check_alloc_limit(layout.size())?;
        let allocated = Global
            .allocate(Self::aligned_layout(layout)?)
            .map_err(|error| save_oom_err(error, layout.size()))?;
        self.track_allocate(layout.size());
        Ok(allocated)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        self.check_alloc_limit(layout.size())?;
        let allocated = Global
            .allocate_zeroed(Self::aligned_layout(layout)?)
            .map_err(|error| save_oom_err(error, layout.size()))?;
        self.track_allocate(layout.size());
        Ok(allocated)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // A live block was accepted by allocate(), so aligned_layout cannot fail
        // here. Gate on Ok regardless so an impossible failure leaks the block
        // rather than desyncing the counters or aborting the JVM.
        if let Ok(aligned) = Self::aligned_layout(layout) {
            Global.deallocate(ptr, aligned);
            self.track_deallocate(layout.size());
        }
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
            .grow(
                ptr,
                Self::aligned_layout(old_layout)?,
                Self::aligned_layout(new_layout)?,
            )
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
            .grow_zeroed(
                ptr,
                Self::aligned_layout(old_layout)?,
                Self::aligned_layout(new_layout)?,
            )
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
            .shrink(
                ptr,
                Self::aligned_layout(old_layout)?,
                Self::aligned_layout(new_layout)?,
            )
            .map_err(|error| save_oom_err(error, delta))?;
        self.track_shrink(delta);
        Ok(allocated)
    }
}

pub type AcVec<T> = alloc_checked::vec::Vec<T, QdbAllocator>;

#[cfg(test)]
pub struct TestAllocatorState {
    mem_tracking: Arc<MemTracking>,
    memory_tracker: Option<Arc<MemoryTracker>>,
    tagged_used: Arc<AtomicUsize>,
}

#[cfg(test)]
impl Default for TestAllocatorState {
    fn default() -> Self {
        Self::new()
    }
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

        Self { mem_tracking, memory_tracker: None, tagged_used }
    }

    /// Attaches a per-query memory tracker to subsequent `allocator()` calls.
    pub fn with_memory_tracker(mut self) -> Self {
        self.memory_tracker = Some(Arc::new(MemoryTracker::new()));
        self
    }

    pub fn allocator(&self) -> QdbAllocator {
        QdbAllocator {
            mem_tracking: self.mem_tracking.clone(),
            memory_tracker: self.memory_tracker.clone(),
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

    pub fn tracker_used(&self) -> usize {
        self.memory_tracker.as_ref().map(|t| t.used()).unwrap_or(0)
    }

    pub fn tracker_limit(&self) -> usize {
        self.memory_tracker.as_ref().map(|t| t.limit()).unwrap_or(0)
    }

    pub fn set_tracker_limit(&self, limit: usize) {
        if let Some(t) = self.memory_tracker.as_ref() {
            t.set_limit(limit);
        }
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
    use crate::allocator::{take_last_alloc_error, AllocFailure, AllocScope, TestAllocatorState};
    use rand::rngs::StdRng;
    use rand::{RngExt, SeedableRng};
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
        const TOO_LARGE: usize = 1024 * 1024 * 1024 * 1024 * 1024; // 1024 TB
        take_last_alloc_error();

        #[cfg(miri)]
        {
            // Miri aborts on impossible host allocations before `Global.allocate`
            // can report a recoverable `AllocError`, so verify the recorded OOM
            // path directly.
            super::save_oom_err(std::alloc::AllocError, TOO_LARGE);
        }

        #[cfg(not(miri))]
        {
            let tas = TestAllocatorState::new();
            let allocator = tas.allocator();
            let layout = std::alloc::Layout::from_size_align(TOO_LARGE, 16).unwrap();
            let result = allocator.allocate(layout);
            assert!(result.is_err());
        }

        let last_err = take_last_alloc_error().unwrap();
        assert!(matches!(
            last_err,
            AllocFailure::OutOfMemory { requested_size } if requested_size == TOO_LARGE
        ));
    }

    #[test]
    fn test_alloc_rejects_layout_that_overflows_min_alignment() {
        // A layout valid at align 1 whose size, raised to the 16-byte minimum
        // alignment, would exceed isize::MAX. aligned_layout must surface this
        // as an allocation failure instead of panicking (which would abort the
        // JVM under JNI). The request is rejected before any real allocation.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        take_last_alloc_error();

        let layout = std::alloc::Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        let result = allocator.allocate(layout);
        assert!(result.is_err());

        let last_err = take_last_alloc_error().unwrap();
        assert!(matches!(
            last_err,
            AllocFailure::OutOfMemory { requested_size } if requested_size == isize::MAX as usize
        ));
        assert_eq!(tas.malloc_count(), 0);
        assert_eq!(tas.rss_mem_used(), 0);
    }

    #[test]
    fn test_grow_rejects_layout_that_overflows_min_alignment() {
        // The same overflow as the allocate case, but on the grow path: a
        // failed grow must reject gracefully and leave the original block
        // intact for the caller to keep using or free.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let layout = std::alloc::Layout::from_size_align(64, 1).unwrap();
        let allocation = allocator.allocate(layout).unwrap();
        let ptr = NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
        assert_eq!(tas.tagged_used(), 64);
        take_last_alloc_error();

        let new_layout = std::alloc::Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        let result = unsafe { allocator.grow(ptr, layout, new_layout) };
        assert!(result.is_err());
        assert!(matches!(
            take_last_alloc_error().unwrap(),
            AllocFailure::OutOfMemory { .. }
        ));

        // The original block is untouched and still ours to free.
        assert_eq!(tas.tagged_used(), 64);
        unsafe { allocator.deallocate(ptr, layout) };
        assert_eq!(tas.tagged_used(), 0);
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
                    scope: AllocScope::Global,
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
    fn test_alloc_with_per_query_limit() {
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();

        // No global RSS limit, only the per-query one.
        tas.set_tracker_limit(1024);

        assert_eq!(tas.tracker_limit(), 1024);
        assert_eq!(tas.tracker_used(), 0);
        assert_eq!(tas.rss_mem_used(), 0);

        // 64 bytes: both counters update.
        {
            let layout = std::alloc::Layout::from_size_align(64, 16).unwrap();
            let allocation = allocator.allocate(layout).unwrap();
            assert_eq!(tas.tagged_used(), 64);
            assert_eq!(tas.rss_mem_used(), 64);
            assert_eq!(tas.tracker_used(), 64);

            let allocation = NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
            unsafe { allocator.deallocate(allocation, layout) };
            assert_eq!(tas.tracker_used(), 0);
            assert_eq!(tas.rss_mem_used(), 0);
        }

        // Over the per-query limit: discriminator must be `Query`.
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
                    scope: AllocScope::Query,
                    rss_mem_limit: 1024,
                    rss_mem_used: 0,
                }
            ));
            // Counters are not advanced on a rejected allocation.
            assert_eq!(tas.tracker_used(), 0);
            assert_eq!(tas.rss_mem_used(), 0);
        }
    }

    #[test]
    fn test_alloc_with_both_limits_global_trips_first() {
        // Both limits set; the global one trips before the per-query one,
        // so the discriminator must say `Global`.
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();
        tas.set_mem_rss_limit(512);
        tas.set_tracker_limit(8192);

        let layout = std::alloc::Layout::from_size_align(1024, 16).unwrap();
        let result = allocator.allocate(layout);
        assert!(result.is_err());
        let last_err = take_last_alloc_error().unwrap();
        assert!(matches!(
            last_err,
            AllocFailure::MemoryLimitExceeded { scope: AllocScope::Global, rss_mem_limit: 512, .. }
        ));
    }

    #[test]
    fn test_alloc_at_per_query_limit_succeeds() {
        // Exactly at the limit boundary: must succeed.
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();
        tas.set_tracker_limit(1024);

        let layout = std::alloc::Layout::from_size_align(1024, 16).unwrap();
        let allocation = allocator.allocate(layout).unwrap();
        // The allocator may give slightly more than requested due to alignment;
        // tracker_used reflects what we actually got back.
        assert!(tas.tracker_used() >= 1024);

        let allocation = NonNull::new(allocation.as_ptr() as *mut u8).unwrap();
        unsafe { allocator.deallocate(allocation, layout) };
        assert_eq!(tas.tracker_used(), 0);
    }

    #[test]
    fn test_grow_with_per_query_limit() {
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();
        tas.set_tracker_limit(256);

        // 64 bytes, fits.
        let layout1 = std::alloc::Layout::from_size_align(64, 16).unwrap();
        let allocation1 = allocator.allocate(layout1).unwrap();
        let used_after_alloc = tas.tracker_used();
        assert!((64..=128).contains(&used_after_alloc));

        // Grow to 128 bytes, still fits.
        let allocation1 = NonNull::new(allocation1.as_ptr() as *mut u8).unwrap();
        let layout2 = std::alloc::Layout::from_size_align(128, 16).unwrap();
        let allocation2 = unsafe { allocator.grow(allocation1, layout1, layout2).unwrap() };
        let used_after_grow = tas.tracker_used();
        assert!(used_after_grow >= 128);

        // Shrink to 96 bytes, per-query counter decreases.
        let allocation2 = NonNull::new(allocation2.as_ptr() as *mut u8).unwrap();
        let layout3 = std::alloc::Layout::from_size_align(96, 16).unwrap();
        let allocation3 = unsafe { allocator.shrink(allocation2, layout2, layout3).unwrap() };
        assert!(tas.tracker_used() < used_after_grow);

        // Try to grow well past the limit: must reject and leave the block intact.
        let allocation3 = NonNull::new(allocation3.as_ptr() as *mut u8).unwrap();
        let layout_huge = std::alloc::Layout::from_size_align(4096, 16).unwrap();
        let used_before_breach = tas.tracker_used();
        let rss_before_breach = tas.rss_mem_used();
        let result = unsafe { allocator.grow(allocation3, layout3, layout_huge) };
        assert!(result.is_err());
        let last_err = take_last_alloc_error().unwrap();
        assert!(matches!(
            last_err,
            AllocFailure::MemoryLimitExceeded { scope: AllocScope::Query, .. }
        ));
        // Counters unchanged on rejected grow.
        assert_eq!(tas.tracker_used(), used_before_breach);
        assert_eq!(tas.rss_mem_used(), rss_before_breach);

        // Original allocation is still valid: the caller still owns it.
        unsafe { allocator.deallocate(allocation3, layout3) };
        assert_eq!(tas.tracker_used(), 0);
        assert_eq!(tas.rss_mem_used(), 0);
    }

    #[test]
    fn test_charge_tracked_only_touches_per_query_counter() {
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();
        tas.set_tracker_limit(1024);

        // A charge adjusts only the per-query counter; RSS and tagged counters
        // stay put because the backing bytes are allocated outside this allocator.
        allocator.charge_tracked(512).unwrap();
        assert_eq!(tas.tracker_used(), 512);
        assert_eq!(tas.rss_mem_used(), 0);
        assert_eq!(tas.tagged_used(), 0);

        // Crossing the limit is rejected and leaves the counter unchanged.
        let result = allocator.charge_tracked(1024);
        assert!(result.is_err());
        assert!(matches!(
            take_last_alloc_error().unwrap(),
            AllocFailure::MemoryLimitExceeded { scope: AllocScope::Query, .. }
        ));
        assert_eq!(tas.tracker_used(), 512);

        // A credit releases the charge symmetrically.
        allocator.credit_tracked(512);
        assert_eq!(tas.tracker_used(), 0);

        // Zero-byte charge/credit are no-ops on every counter.
        allocator.charge_tracked(0).unwrap();
        allocator.credit_tracked(0);
        assert_eq!(tas.tracker_used(), 0);
        assert_eq!(tas.rss_mem_used(), 0);
    }

    #[test]
    fn test_charge_tracked_without_tracker_checks_global_limit() {
        // No per-query tracker bound: charge_tracked still honors the global RSS
        // limit and degrades to a no-op on the (absent) per-query counter.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        tas.set_mem_rss_limit(1024);

        // The global counter is not mutated, so the check compares the request
        // against the bare limit; 2048 > 1024 is rejected.
        let result = allocator.charge_tracked(2048);
        assert!(result.is_err());
        assert!(matches!(
            take_last_alloc_error().unwrap(),
            AllocFailure::MemoryLimitExceeded { scope: AllocScope::Global, .. }
        ));

        // Under the global limit: accepted, and no counter moves (no tracker).
        allocator.charge_tracked(512).unwrap();
        assert_eq!(tas.rss_mem_used(), 0);
        allocator.credit_tracked(512);
    }

    #[test]
    fn test_min_alloc_alignment() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let layout = std::alloc::Layout::from_size_align(1, 1).unwrap();
        let allocation = allocator.allocate(layout).unwrap();
        let ptr = allocation.as_ptr() as *mut u8;
        assert_eq!((ptr as usize) % super::MIN_ALLOC_ALIGNMENT, 0);

        let allocation = NonNull::new(ptr).unwrap();
        unsafe { allocator.deallocate(allocation, layout) };
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
        // Attach a per-query tracker so the concurrent run also exercises the
        // tracker_used counter (its fetch_add/fetch_sub on every alloc and free).
        // The limit stays 0 (unlimited), so no allocation is rejected.
        let tas = TestAllocatorState::new().with_memory_tracker();
        let allocator = tas.allocator();

        #[cfg(miri)]
        let (thread_count, allocations_capacity, iterations) = (4, 64, 128usize);
        #[cfg(not(miri))]
        let (thread_count, allocations_capacity, iterations) = (64, 2048, 10_000usize);

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

        for thread_idx in 0..thread_count {
            let mut alloc_count = 0;
            let mut realloc_count = 0;
            let allocator = allocator.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                let mut allocations = Vec::with_capacity(allocations_capacity);
                let mut rng = StdRng::seed_from_u64(thread_idx as u64 + 1);
                barrier.wait();
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
        assert_eq!(tas.tracker_used(), 0);
        assert_eq!(tas.malloc_count(), total_allocs);
        assert_eq!(tas.realloc_count(), total_reallocs);
        assert_eq!(tas.free_count(), total_allocs);
    }
}
