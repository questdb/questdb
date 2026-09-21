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

//! Native-memory accounting shared with Java by data layout. Plain OSS
//! trackers publish every charge synchronously. Resource Group trackers batch
//! a signed delta in const-initialized OS-thread-local state and publish at an
//! adaptive threshold or an explicit execution boundary.

use std::cell::Cell;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

const GATE_ORDERING: Ordering = Ordering::SeqCst;
const RESOURCE_MEMORY_MAGIC: usize = 0x5144_4252_474D_454D;
const RMW_ORDERING: Ordering = Ordering::AcqRel;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Breach {
    pub limit: usize,
    pub scope: MemoryScope,
    pub used: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MemoryScope {
    /// The Resource Group tracker binding is invalid.
    Configuration,
    /// The global RSS memory limit, shared by every allocation.
    Global,
    /// The Resource Group aggregate memory limit.
    Group,
    /// The managed-query process safety limit.
    Process,
    /// The per-workload memory tracker.
    Query,
}

#[derive(Default)]
#[repr(C)]
struct MemoryNode {
    used: AtomicIsize,
    limit: AtomicUsize,
}

/// The full 64-byte Java tracker block. The first two words remain the OSS
/// `{used, limit}` ABI. Published Resource Group counters are signed because a
/// free may be published by a different carrier before the allocating
/// carrier's positive delta.
#[derive(Default)]
#[repr(C)]
pub struct MemoryTracker {
    node: MemoryNode,
    resource_magic: AtomicUsize,
    resource_threshold: AtomicUsize,
    resource_group: AtomicUsize,
    resource_process: AtomicUsize,
    resource_context_count: AtomicIsize,
    resource_generation: AtomicUsize,
}

const _: () = assert!(
    size_of::<MemoryTracker>() == 64
        && std::mem::offset_of!(MemoryTracker, node) == 0
        && std::mem::offset_of!(MemoryNode, used) == 0
        && std::mem::offset_of!(MemoryNode, limit) == 8
        && std::mem::offset_of!(MemoryTracker, resource_magic) == 16
        && std::mem::offset_of!(MemoryTracker, resource_threshold) == 24
        && std::mem::offset_of!(MemoryTracker, resource_group) == 32
        && std::mem::offset_of!(MemoryTracker, resource_process) == 40
        && std::mem::offset_of!(MemoryTracker, resource_context_count) == 48
        && std::mem::offset_of!(MemoryTracker, resource_generation) == 56
);

#[derive(Clone, Copy)]
struct ResourceMemoryThreadState {
    delta: isize,
    generation: usize,
    group_address: usize,
    process_address: usize,
    tracker_address: usize,
}

impl ResourceMemoryThreadState {
    const EMPTY: Self = Self {
        delta: 0,
        generation: 0,
        group_address: 0,
        process_address: 0,
        tracker_address: 0,
    };

    fn bind(
        &mut self,
        tracker: &MemoryTracker,
        generation: usize,
        group_address: usize,
        process_address: usize,
    ) -> Result<(), Breach> {
        let tracker_address = tracker as *const MemoryTracker as usize;
        if self.tracker_address == tracker_address && self.generation == generation {
            return Ok(());
        }
        self.detach();
        if !tracker.binding_valid(generation, group_address, process_address) {
            return Err(tracker.configuration_breach());
        }
        self.generation = generation;
        self.group_address = group_address;
        self.process_address = process_address;
        self.tracker_address = tracker_address;
        tracker.resource_context_count.fetch_add(1, RMW_ORDERING);
        if !tracker.binding_valid(generation, group_address, process_address) {
            tracker.resource_context_count.fetch_sub(1, RMW_ORDERING);
            self.clear();
            return Err(tracker.configuration_breach());
        }
        Ok(())
    }

    fn charge(
        &mut self,
        tracker: &MemoryTracker,
        generation: usize,
        group_address: usize,
        process_address: usize,
        bytes: usize,
        threshold: usize,
    ) -> Result<(), Breach> {
        self.bind(tracker, generation, group_address, process_address)?;
        let bytes = isize::try_from(bytes).map_err(|_| tracker.configuration_breach())?;
        let previous = self.delta;
        let next = previous
            .checked_add(bytes)
            .ok_or_else(|| tracker.configuration_breach())?;
        if next >= threshold as isize {
            match tracker.publish_enforced_delta(generation, group_address, process_address, next) {
                Ok(()) => self.delta = 0,
                Err(breach) => {
                    self.delta = previous;
                    return Err(breach);
                }
            }
        } else {
            self.delta = next;
        }
        Ok(())
    }

    fn clear(&mut self) {
        *self = Self::EMPTY;
    }

    fn credit(
        &mut self,
        tracker: &MemoryTracker,
        generation: usize,
        group_address: usize,
        process_address: usize,
        bytes: usize,
        threshold: usize,
    ) -> Result<(), Breach> {
        self.bind(tracker, generation, group_address, process_address)?;
        let bytes = isize::try_from(bytes).map_err(|_| tracker.configuration_breach())?;
        let next = self
            .delta
            .checked_sub(bytes)
            .ok_or_else(|| tracker.configuration_breach())?;
        if next <= -(threshold as isize) {
            tracker.publish_boundary_delta(generation, group_address, process_address, next);
            self.delta = 0;
        } else {
            self.delta = next;
        }
        Ok(())
    }

    fn detach(&mut self) {
        if self.tracker_address == 0 {
            return;
        }
        // Tracker blocks stay mapped until engine shutdown, after carrier
        // threads have joined. Generation validation prevents late publication
        // into a recycled binding.
        let tracker = unsafe { &*(self.tracker_address as *const MemoryTracker) };
        if tracker.binding_valid(self.generation, self.group_address, self.process_address) {
            self.publish();
            let previous = tracker.resource_context_count.fetch_sub(1, RMW_ORDERING);
            debug_assert!(previous >= 1, "Resource Group context-count underflow");
        }
        self.clear();
    }

    fn detach_if(&mut self, tracker_address: usize, generation: usize) {
        if self.tracker_address == tracker_address && self.generation == generation {
            self.detach();
        }
    }

    fn publish(&mut self) {
        if self.delta == 0 || self.tracker_address == 0 {
            return;
        }
        let tracker = unsafe { &*(self.tracker_address as *const MemoryTracker) };
        tracker.publish_boundary_delta(
            self.generation,
            self.group_address,
            self.process_address,
            self.delta,
        );
        self.delta = 0;
    }
}

thread_local! {
    static RESOURCE_MEMORY_THREAD_STATE: Cell<ResourceMemoryThreadState> =
        const { Cell::new(ResourceMemoryThreadState::EMPTY) };
}

pub fn detach_thread_local() {
    with_thread_state(ResourceMemoryThreadState::detach);
}

pub fn detach_thread_local_if(tracker_address: usize, generation: usize) {
    with_thread_state(|state| state.detach_if(tracker_address, generation));
}

pub fn publish_thread_local() {
    with_thread_state(ResourceMemoryThreadState::publish);
}

fn with_thread_state<R>(f: impl FnOnce(&mut ResourceMemoryThreadState) -> R) -> R {
    RESOURCE_MEMORY_THREAD_STATE.with(|cell| {
        let mut state = cell.get();
        let result = f(&mut state);
        cell.set(state);
        result
    })
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Credits a charge on the regular allocator path. Resource Group credits
    /// accumulate in the thread-local delta and are dropped, under a debug
    /// assertion, when the binding is no longer valid.
    pub fn credit(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        if self.resource_magic.load(GATE_ORDERING) != RESOURCE_MEMORY_MAGIC {
            Self::credit_exact(&self.node, bytes);
            return;
        }
        let result = self.resource_binding().and_then(
            |(generation, group_address, process_address, threshold)| {
                with_thread_state(|state| {
                    state.credit(
                        self,
                        generation,
                        group_address,
                        process_address,
                        bytes,
                        threshold,
                    )
                })
            },
        );
        debug_assert!(result.is_ok(), "invalid Resource Group credit: {result:?}");
    }

    /// Immediate counterpart used by coarse, cross-thread Enterprise leases.
    /// Those leases outlive an allocator call and cannot retain TLS ownership.
    pub fn credit_immediate(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        if self.resource_magic.load(GATE_ORDERING) != RESOURCE_MEMORY_MAGIC {
            Self::credit_exact(&self.node, bytes);
            return;
        }
        if let Ok((generation, group_address, process_address, _)) = self.resource_binding() {
            let Ok(delta) = isize::try_from(bytes).map(|value| -value) else {
                return;
            };
            self.publish_boundary_delta(generation, group_address, process_address, delta);
        }
    }

    pub fn limit(&self) -> usize {
        self.node.limit.load(GATE_ORDERING)
    }

    pub fn set_limit(&self, limit: usize) {
        self.node.limit.store(limit, GATE_ORDERING);
    }

    pub fn try_charge(&self, bytes: usize) -> Result<(), Breach> {
        if bytes == 0 {
            return Ok(());
        }
        if self.resource_magic.load(GATE_ORDERING) != RESOURCE_MEMORY_MAGIC {
            return self.try_charge_plain(bytes);
        }
        let (generation, group_address, process_address, threshold) = self.resource_binding()?;
        with_thread_state(|state| {
            state.charge(
                self,
                generation,
                group_address,
                process_address,
                bytes,
                threshold,
            )
        })
    }

    /// Immediate counterpart used by qdb-ent's long-lived cold-read leases.
    pub fn try_charge_immediate(&self, bytes: usize) -> Result<(), Breach> {
        if bytes == 0 {
            return Ok(());
        }
        if self.resource_magic.load(GATE_ORDERING) != RESOURCE_MEMORY_MAGIC {
            return self.try_charge_plain(bytes);
        }
        let (generation, group_address, process_address, _) = self.resource_binding()?;
        let delta = isize::try_from(bytes).map_err(|_| self.configuration_breach())?;
        self.publish_enforced_delta(generation, group_address, process_address, delta)
    }

    pub fn used(&self) -> usize {
        Self::non_negative(self.node.used.load(GATE_ORDERING))
    }

    fn binding_valid(
        &self,
        generation: usize,
        group_address: usize,
        process_address: usize,
    ) -> bool {
        self.resource_magic.load(GATE_ORDERING) == RESOURCE_MEMORY_MAGIC
            && self.resource_generation.load(GATE_ORDERING) == generation
            && self.resource_group.load(GATE_ORDERING) == group_address
            && self.resource_process.load(GATE_ORDERING) == process_address
    }

    fn configuration_breach(&self) -> Breach {
        Breach {
            limit: self.limit(),
            scope: MemoryScope::Configuration,
            used: self.used(),
        }
    }

    fn credit_exact(node: &MemoryNode, bytes: usize) {
        let Ok(bytes) = isize::try_from(bytes) else {
            return;
        };
        let previous = node.used.fetch_sub(bytes, RMW_ORDERING);
        if previous < bytes {
            node.used.fetch_add(bytes - previous.max(0), RMW_ORDERING);
        }
    }

    fn non_negative(value: isize) -> usize {
        value.max(0) as usize
    }

    /// Applies an unenforced delta to the query, process and group counters.
    /// A credit may reach the counters before the matching charge, so the
    /// counters are signed and no limit is checked here.
    fn publish_boundary_delta(
        &self,
        generation: usize,
        group_address: usize,
        process_address: usize,
        delta: isize,
    ) {
        if delta == 0 || !self.binding_valid(generation, group_address, process_address) {
            return;
        }
        let (group, process) = unsafe {
            (
                &*(group_address as *const MemoryNode),
                &*(process_address as *const MemoryNode),
            )
        };
        self.node.used.fetch_add(delta, RMW_ORDERING);
        process.used.fetch_add(delta, RMW_ORDERING);
        group.used.fetch_add(delta, RMW_ORDERING);
    }

    fn publish_enforced_delta(
        &self,
        generation: usize,
        group_address: usize,
        process_address: usize,
        delta: isize,
    ) -> Result<(), Breach> {
        if delta <= 0 || !self.binding_valid(generation, group_address, process_address) {
            return Err(self.configuration_breach());
        }
        let (group, process) = unsafe {
            (
                &*(group_address as *const MemoryNode),
                &*(process_address as *const MemoryNode),
            )
        };
        let bytes = delta as usize;
        Self::reserve_node(&self.node, bytes, MemoryScope::Query)?;
        if let Err(breach) = Self::reserve_node(process, bytes, MemoryScope::Process) {
            self.node.used.fetch_sub(delta, RMW_ORDERING);
            return Err(breach);
        }
        if let Err(breach) = Self::reserve_node(group, bytes, MemoryScope::Group) {
            process.used.fetch_sub(delta, RMW_ORDERING);
            self.node.used.fetch_sub(delta, RMW_ORDERING);
            return Err(breach);
        }
        Ok(())
    }

    fn reserve_node(node: &MemoryNode, bytes: usize, scope: MemoryScope) -> Result<(), Breach> {
        let bytes = isize::try_from(bytes).map_err(|_| Breach {
            limit: node.limit.load(GATE_ORDERING),
            scope,
            used: Self::non_negative(node.used.load(GATE_ORDERING)),
        })?;
        let limit = node.limit.load(GATE_ORDERING);
        loop {
            let used = node.used.load(GATE_ORDERING);
            let Some(next) = used.checked_add(bytes) else {
                return Err(Breach {
                    limit,
                    scope,
                    used: Self::non_negative(used),
                });
            };
            if limit > 0 && next > limit as isize {
                return Err(Breach {
                    limit,
                    scope,
                    used: Self::non_negative(used),
                });
            }
            if node
                .used
                .compare_exchange_weak(used, next, RMW_ORDERING, GATE_ORDERING)
                .is_ok()
            {
                return Ok(());
            }
        }
    }

    fn resource_binding(&self) -> Result<(usize, usize, usize, usize), Breach> {
        let generation = self.resource_generation.load(GATE_ORDERING);
        let group_address = self.resource_group.load(GATE_ORDERING);
        let process_address = self.resource_process.load(GATE_ORDERING);
        let threshold = self.resource_threshold.load(GATE_ORDERING);
        if generation == 0 || group_address == 0 || process_address == 0 || threshold == 0 {
            return Err(self.configuration_breach());
        }
        Ok((generation, group_address, process_address, threshold))
    }

    fn try_charge_plain(&self, bytes: usize) -> Result<(), Breach> {
        let bytes = isize::try_from(bytes).map_err(|_| Breach {
            limit: self.limit(),
            scope: MemoryScope::Query,
            used: self.used(),
        })?;
        let limit = self.limit();
        let used = self.used();
        if limit > 0 && used.saturating_add(bytes as usize) > limit {
            return Err(Breach {
                limit,
                scope: MemoryScope::Query,
                used,
            });
        }
        self.node.used.fetch_add(bytes, RMW_ORDERING);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    const RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES: usize = 64 * 1024;

    fn bind_resource_nodes(tracker: &MemoryTracker, group: &MemoryNode, process: &MemoryNode) {
        let threshold = unpublished_threshold(tracker, group, process);
        tracker
            .resource_group
            .store(group as *const MemoryNode as usize, GATE_ORDERING);
        tracker
            .resource_process
            .store(process as *const MemoryNode as usize, GATE_ORDERING);
        tracker.resource_context_count.store(0, GATE_ORDERING);
        tracker.resource_generation.store(1, GATE_ORDERING);
        tracker.resource_threshold.store(threshold, GATE_ORDERING);
        tracker
            .resource_magic
            .store(RESOURCE_MEMORY_MAGIC, GATE_ORDERING);
    }

    fn node(limit: usize) -> MemoryNode {
        MemoryNode {
            used: AtomicIsize::new(0),
            limit: AtomicUsize::new(limit),
        }
    }

    fn unpublished_threshold(
        tracker: &MemoryTracker,
        group: &MemoryNode,
        process: &MemoryNode,
    ) -> usize {
        let narrowest = [
            tracker.limit(),
            group.limit.load(GATE_ORDERING),
            process.limit.load(GATE_ORDERING),
        ]
        .into_iter()
        .filter(|limit| *limit > 0)
        .min()
        .unwrap_or(usize::MAX);
        if narrowest == usize::MAX {
            return RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES;
        }
        (narrowest / 1024).clamp(1, RESOURCE_MEMORY_MAX_UNPUBLISHED_BYTES)
    }

    #[test]
    fn boundary_publish_may_exceed_limit_by_bounded_thread_deltas() {
        let tracker = Arc::new(MemoryTracker::new());
        let group = Arc::new(node(100));
        let process = Arc::new(node(100));
        tracker.set_limit(100);
        bind_resource_nodes(&tracker, &group, &process);
        // limit/1024 yields a one-byte threshold for tiny limits, so install a
        // test-only 64-byte threshold to exercise the documented N*T bound.
        tracker.resource_threshold.store(64, GATE_ORDERING);
        let charged = Arc::new(Barrier::new(5));
        let publish = Arc::new(Barrier::new(5));
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let tracker = Arc::clone(&tracker);
                let charged = Arc::clone(&charged);
                let publish = Arc::clone(&publish);
                scope.spawn(move || {
                    tracker.try_charge(63).unwrap();
                    charged.wait();
                    publish.wait();
                    detach_thread_local();
                });
            }
            charged.wait();
            assert_eq!(tracker.used(), 0);
            publish.wait();
        });
        assert_eq!(tracker.used(), 252);
        assert_eq!(group.used.load(GATE_ORDERING), 252);
        assert_eq!(process.used.load(GATE_ORDERING), 252);
    }

    #[test]
    fn cross_thread_credit_can_publish_before_charge_and_converges_to_zero() {
        let tracker = Arc::new(MemoryTracker::new());
        let group = Arc::new(node(128 * 1024 * 1024));
        let process = Arc::new(node(128 * 1024 * 1024));
        tracker.set_limit(128 * 1024 * 1024);
        bind_resource_nodes(&tracker, &group, &process);
        let charged = Arc::new(Barrier::new(2));
        let credited = Arc::new(Barrier::new(2));
        std::thread::scope(|scope| {
            let tracker_a = Arc::clone(&tracker);
            let charged_a = Arc::clone(&charged);
            let credited_a = Arc::clone(&credited);
            scope.spawn(move || {
                tracker_a.try_charge(32).unwrap();
                charged_a.wait();
                credited_a.wait();
                detach_thread_local();
            });
            let tracker_b = Arc::clone(&tracker);
            scope.spawn(move || {
                charged.wait();
                tracker_b.credit(32);
                detach_thread_local();
                assert_eq!(tracker_b.node.used.load(GATE_ORDERING), -32);
                credited.wait();
            });
        });
        assert_eq!(tracker.used(), 0);
        assert_eq!(group.used.load(GATE_ORDERING), 0);
        assert_eq!(process.used.load(GATE_ORDERING), 0);
    }

    #[test]
    fn locally_accumulated_charge_publishes_at_threshold() {
        let tracker = MemoryTracker::new();
        let group = node(128 * 1024 * 1024);
        let process = node(128 * 1024 * 1024);
        tracker.set_limit(128 * 1024 * 1024);
        bind_resource_nodes(&tracker, &group, &process);
        tracker.try_charge(32 * 1024).unwrap();
        assert_eq!(tracker.used(), 0);
        tracker.try_charge(32 * 1024).unwrap();
        assert_eq!(tracker.used(), 64 * 1024);
        tracker.credit(64 * 1024);
        assert_eq!(tracker.used(), 0);
        detach_thread_local();
        assert_eq!(tracker.resource_context_count.load(GATE_ORDERING), 0);
    }

    #[test]
    fn immediate_resource_charge_enforces_and_balances() {
        let tracker = MemoryTracker::new();
        let group = node(10);
        let process = node(100);
        tracker.set_limit(100);
        bind_resource_nodes(&tracker, &group, &process);
        tracker.try_charge_immediate(8).unwrap();
        assert_eq!(tracker.used(), 8);
        assert_eq!(
            tracker.try_charge_immediate(3),
            Err(Breach {
                limit: 10,
                scope: MemoryScope::Group,
                used: 8,
            })
        );
        tracker.credit_immediate(8);
        assert_eq!(tracker.used(), 0);
        assert_eq!(group.used.load(GATE_ORDERING), 0);
        assert_eq!(process.used.load(GATE_ORDERING), 0);
    }

    #[test]
    fn invalid_resource_binding_fails_without_publishing() {
        let tracker = MemoryTracker::new();
        tracker.resource_threshold.store(64, GATE_ORDERING);
        tracker.resource_generation.store(1, GATE_ORDERING);
        tracker
            .resource_magic
            .store(RESOURCE_MEMORY_MAGIC, GATE_ORDERING);
        assert_eq!(
            tracker.try_charge(1),
            Err(Breach {
                limit: 0,
                scope: MemoryScope::Configuration,
                used: 0,
            })
        );
    }

    #[test]
    fn plain_tracker_charge_credit_through_raw_overlay() {
        let tracker = MemoryTracker::new();
        tracker.set_limit(1024);
        let address = &tracker as *const MemoryTracker as usize;
        let view = unsafe { &*(address as *const MemoryTracker) };
        view.try_charge(512).unwrap();
        assert_eq!(tracker.used(), 512);
        view.credit(512);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn plain_tracker_limit_is_exact() {
        let tracker = MemoryTracker::new();
        tracker.set_limit(1024);
        tracker.try_charge(512).unwrap();
        tracker.try_charge(512).unwrap();
        assert_eq!(
            tracker.try_charge(1),
            Err(Breach {
                limit: 1024,
                scope: MemoryScope::Query,
                used: 1024,
            })
        );
        tracker.credit(1024);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn zero_byte_operations_are_noops() {
        let tracker = MemoryTracker::new();
        tracker.set_limit(8);
        tracker.try_charge(8).unwrap();
        tracker.try_charge(0).unwrap();
        tracker.credit(0);
        assert_eq!(tracker.used(), 8);
    }
}
