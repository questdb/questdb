//! Write-Ahead Log (WAL) lock manager.
//!
//! This module provides a lock manager for handling concurrent access to WAL
//! directories by multiple writers and purge jobs.
//! It ensures that writers and purge jobs do not interfere with each other
//! by managing locks on a per-WAL (table/wal-id) basis.
//!
//! Note:
//! It is expected for at most one writer and one purge job to be active
//! for a given WAL at any time. Multiple writers or purge jobs for the same
//! WAL are not supported and will result in errors.
//!
//! WAL inner structure:
//! WALs directories contains multiple segments that contains the actual data.
//! WAL segments are identified by a segment id (SegmentId) which is a
//! non-negative integer.
//! The folder structure looks like this:
//! ```text
//! /table_name/wal[wal_id]/[segment_id]
//! ```
//!
//! WAL writer behavior:
//! When a WAL writer starts, it acquires a new WalId, locks the WAL for
//! writing and starts writing to segments starting from segment id 0.
//! When the writer advances to a new segment, it updates the `min_segment_id`
//! in the lock manager.
//!
//! WAL purge job behavior:
//! When a WAL purge job starts, it iterates over all WAL directories.
//! For each WAL, it tries to acquire a purge lock which returns the max
//! segment id that can be deleted or None if no writer is active.
//! The purge job has now exclusive access to all segments with id less than
//! or equal to the returned segment id and no writer may lock these segments
//! until the purge job releases the lock.
//!
//! We need WAL writers to be able to lock/unlock/lock WALs for replication
//! to work properly. As we do not know whether a WAL writer is closed on the
//! primary node, the replica locks the WAL when it needs to update a table,
//! downloads the WAL segments, finish its work and then unlocks the WAL.
//!
//! Implementation details:
//! The lock manager maintains a mapping of (table_dir_name, wal_id) to the wal
//! state.
//! The wal state can be one of the following:
//! - PurgeExclusive: A purge job has exclusive access to the WAL. No writer is
//! active and if a writer tries to acquire a lock, it will block until the
//! purge is done.
//! - WriterShared: A writer has shared access to the WAL. It has exclusive
//! access to all segments with id greater than or equal to `min_segment_id`.
//! A purge job may try to acquire a lock, in which case the state will be
//! upgraded to WriterAndPurge.
//! - WriterAndPurge: Both a writer and a purge job have access to the WAL.
//! The writer has exclusive access to all segments with id greater than or
//! equal to `min_segment_id` and the purge job has exclusive access to all
//! other segments.

use std::{
    error::Error,
    fmt::Display,
    sync::{Arc, Condvar, Mutex, atomic::AtomicU32},
};

use dashmap::Entry;

use crate::types::{IdNumber, SegmentId, WalId};

// Unique identifier to a table within an instance.
// Note that this is NOT the same as `TableToken` ids.
// This id is unique and stable for the lifetime of the WalLock instance,
// while `TableToken` ids are not guaranteed to be unique.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct TableId(u32);

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct WalLockKey(TableId, WalId);

type NextWriterLock = Arc<(Mutex<bool>, Condvar)>;

// States of a specific WAL entry.
#[derive(Debug)]
pub enum WalEntry {
    // Wal purge job have an exclusive lock on the WAL and can delete it completely.
    PurgeExclusive {
        // A writer may want to acquire a lock after the purge is done.
        // In that case, the purge job must update the state to `WriterShared` and
        // set this field to wake up the waiting writer on unlock.
        next_writer: Option<(NextWriterLock, SegmentId)>,
    },
    // Writer have a lock on the WAL and have an exclusive access to all segments greater than
    // or equal to `min_segment_id`.
    WriterShared {
        min_segment_id: SegmentId,
    },
    // Both writer and purge have locks on the WAL. Writer has exclusive access to all segments
    // greater than or equal to `min_segment_id` and purge has exclusive access to all segments
    // less than `min_segment_id`.
    WriterAndPurge {
        min_segment_id: SegmentId,
    },
}

// Lock manager for WAL directories.
//
// This implementation assumes that no 2 writers or 2 purge jobs will try to write to the same table+wal_id
// concurrently.
// For the Wal purge job, this is ensured by the job being sequential and thus restricted to a single thread.
// For the writers, this is ensured by allocating a new WalId for each new writer.
// Not respecting this assumption will lead to errors.
#[derive(Debug, Default)]
pub struct WalLock {
    // Locks for each table+wal_id combination.
    entries: dashmap::DashMap<WalLockKey, WalEntry>,

    // TableDirName -> Table id
    // We cannot rely on `TableToken` table ids as they are not really unique, instead
    // we maintain our own mapping because table dir names are guaranteed to be unique.
    table_ids: dashmap::DashMap<String, TableId>,

    // Next table id to assign.
    next_table_id: AtomicU32,
}

impl WalLock {
    pub fn new() -> Self {
        Self {
            entries: dashmap::DashMap::new(),
            table_ids: dashmap::DashMap::new(),
            next_table_id: AtomicU32::new(0),
        }
    }

    // Get or create a TableId for the given table directory name.
    fn get_table_id(&self, table_dir_name: &str) -> TableId {
        if let Some(table_id) = self.table_ids.get(table_dir_name) {
            return *table_id;
        }

        match self.table_ids.entry(table_dir_name.to_string()) {
            Entry::Occupied(occ) => *occ.get(),
            Entry::Vacant(vac) => {
                let id = self
                    .next_table_id
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let table_id = TableId(id);
                vac.insert(table_id);
                table_id
            }
        }
    }

    // Acquire a writer lock for the given table and wal id.
    // If a purge job is currently holding an exclusive lock on the WAL,
    // this function will block until the purge is done.
    // If another writer already holds a lock on the WAL, an error is returned.
    pub fn lock_for_write(
        &self,
        table_dir_name: &str,
        wal_id: WalId,
        min_segment_id: SegmentId,
    ) -> Result<(), WriterLockError> {
        let table_id = self.get_table_id(table_dir_name);
        let key = WalLockKey(table_id, wal_id);
        let mut shared: Option<Arc<(Mutex<bool>, Condvar)>> = None;

        {
            let entry = self.entries.entry(key);
            match entry {
                Entry::Occupied(mut occ) => {
                    let wal_entry = occ.get_mut();
                    match wal_entry {
                        WalEntry::PurgeExclusive { next_writer } => {
                            match next_writer {
                                Some(_) => {
                                    // There is already a writer waiting for the purge to finish.
                                    // We cannot have 2 writers at the same time.
                                    return Err(WriterLockError::WriterAlreadyExists(
                                        table_dir_name.to_string(),
                                        wal_id,
                                    ));
                                }
                                None => {
                                    let s = Arc::new((Mutex::new(false), Condvar::new()));
                                    shared = Some(s.clone());

                                    // No writer is waiting yet, set ourselves as the next writer.
                                    *next_writer = Some((s, min_segment_id));
                                }
                            }
                        }
                        WalEntry::WriterShared { .. } | WalEntry::WriterAndPurge { .. } => {
                            // There is already a writer holding the lock.
                            return Err(WriterLockError::WriterAlreadyExists(
                                table_dir_name.to_string(),
                                wal_id,
                            ));
                        }
                    }
                }
                Entry::Vacant(vac) => {
                    vac.insert(WalEntry::WriterShared { min_segment_id });
                }
            };
        }

        if let Some(shared) = shared {
            // We need to wait for the purge to finish.
            let (lock, cvar) = &*shared;
            let _started = cvar
                .wait_while(lock.lock().unwrap(), |started| !*started)
                .unwrap();
        }

        Ok(())
    }

    // Release a writer lock for the given table and wal id.
    pub fn unlock_write(
        &self,
        table_dir_name: &str,
        wal_id: WalId,
    ) -> Result<(), WriterUnlockError> {
        let table_id = self.get_table_id(table_dir_name);
        let key = WalLockKey(table_id, wal_id);
        let entry = self.entries.entry(key);
        match entry {
            Entry::Occupied(mut occ) => {
                let wal_entry = occ.get_mut();
                match wal_entry {
                    WalEntry::WriterShared { .. } => {
                        // No purge is waiting, just remove the entry.
                        occ.remove();
                    }
                    WalEntry::WriterAndPurge { .. } => {
                        // A purge is taking place, update the state to `PurgeExclusive`
                        *wal_entry = WalEntry::PurgeExclusive { next_writer: None };
                    }
                    WalEntry::PurgeExclusive { .. } => {
                        // This should not happen, as a writer cannot hold a lock
                        // when the state is `PurgeExclusive`.
                        return Err(WriterUnlockError::InvalidState(
                            table_dir_name.to_string(),
                            wal_id,
                        ));
                    }
                }
            }
            Entry::Vacant(_) => {
                // This should not happen, as we should have an entry for the writer.
                return Err(WriterUnlockError::NotFound(
                    table_dir_name.to_string(),
                    wal_id,
                ));
            }
        }
        Ok(())
    }

    // Acquire a purge lock for the given table and wal id.
    // If a writer is currently holding a lock on the WAL,
    // this function will return the max segment id that the purge can delete up to.
    pub fn lock_for_purge(
        &self,
        table_dir_name: &str,
        wal_id: WalId,
    ) -> Result<Option<SegmentId>, PurgeLockError> {
        let table_id = self.get_table_id(table_dir_name);
        let key = WalLockKey(table_id, wal_id);
        let mut min_segment_id: Option<SegmentId> = None;

        {
            let entry = self.entries.entry(key);
            match entry {
                Entry::Occupied(mut occ) => {
                    let wal_entry = occ.get_mut();
                    match wal_entry {
                        WalEntry::WriterShared {
                            min_segment_id: writer_min,
                        } => {
                            // A writer is holding a lock, update the state to `WriterAndPurge`
                            min_segment_id = Some(*writer_min);
                            *wal_entry = WalEntry::WriterAndPurge {
                                min_segment_id: *writer_min,
                            };
                        }
                        WalEntry::WriterAndPurge { .. } | WalEntry::PurgeExclusive { .. } => {
                            // A purge is already holding a lock.
                            return Err(PurgeLockError::PurgeAlreadyExists(
                                table_dir_name.to_string(),
                                wal_id,
                            ));
                        }
                    }
                }
                Entry::Vacant(vac) => {
                    vac.insert(WalEntry::PurgeExclusive { next_writer: None });
                }
            };
        }

        Ok(min_segment_id)
    }

    // Release a purge lock for the given table and wal id.
    pub fn unlock_purge(
        &self,
        table_dir_name: &str,
        wal_id: WalId,
    ) -> Result<(), PurgeUnlockError> {
        let table_id = self.get_table_id(table_dir_name);
        let key = WalLockKey(table_id, wal_id);
        let entry = self.entries.entry(key);
        match entry {
            Entry::Occupied(mut occ) => {
                let wal_entry = occ.get_mut();
                match wal_entry {
                    WalEntry::PurgeExclusive { next_writer } => {
                        match next_writer {
                            Some((shared, next_segment_id)) => {
                                // A writer is waiting, wake it up and update the state to `WriterShared`
                                // It's fine to update the state after notifying the writer, we cannot have
                                // multiple threads reading the same entry at the same time.

                                {
                                    let (lock, cvar) = &**shared;
                                    let mut started = lock.lock().unwrap();
                                    *started = true;
                                    cvar.notify_one();
                                }

                                *wal_entry = WalEntry::WriterShared {
                                    min_segment_id: *next_segment_id,
                                };
                            }
                            None => {
                                // No writer is waiting, just remove the entry.
                                occ.remove();
                            }
                        }
                    }
                    WalEntry::WriterAndPurge { min_segment_id } => {
                        // A writer is holding a lock, downgrade our state to `WriterShared`
                        *wal_entry = WalEntry::WriterShared {
                            min_segment_id: *min_segment_id,
                        }
                    }
                    WalEntry::WriterShared { .. } => {
                        // This should not happen, as a purge cannot hold a lock
                        // when the state is `WriterShared`.
                        return Err(PurgeUnlockError::InvalidState(
                            table_dir_name.to_string(),
                            wal_id,
                        ));
                    }
                }
            }
            Entry::Vacant(_) => {
                // This should not happen, as we should have an entry for the purge.
                return Err(PurgeUnlockError::NotFound(
                    table_dir_name.to_string(),
                    wal_id,
                ));
            }
        }
        Ok(())
    }

    // Check if a specific segment is locked by a writer for the given table and wal id.
    pub fn is_segment_locked(
        &self,
        table_dir_name: &str,
        wal_id: WalId,
        segment_id: SegmentId,
    ) -> bool {
        let table_id = self.get_table_id(table_dir_name);
        let key = WalLockKey(table_id, wal_id);
        if let Some(wal_entry) = self.entries.get(&key) {
            match &*wal_entry {
                WalEntry::WriterShared { min_segment_id }
                | WalEntry::WriterAndPurge { min_segment_id } => segment_id >= *min_segment_id,
                WalEntry::PurgeExclusive { .. } => false,
            }
        } else {
            false
        }
    }

    // Check if there is any lock (writer or purge) for the given table and wal id.
    pub fn is_locked(&self, table_dir_name: &str, wal_id: WalId) -> bool {
        let table_id = self.get_table_id(table_dir_name);
        let key = WalLockKey(table_id, wal_id);
        self.entries.contains_key(&key)
    }

    // Updates the min segment id for a writer lock.
    // Returns an error if the new segment ID is less than the current one.
    // Returns Ok(()) if the update was successful or if the WAL entry doesn't exist.
    pub fn update_writer_min_segment_id(
        &self,
        table_dir_name: &str,
        wal_id: WalId,
        new_min_segment_id: SegmentId,
    ) -> Result<(), UpdateSegmentError> {
        let table_id = self.get_table_id(table_dir_name);
        let key = WalLockKey(table_id, wal_id);
        if let Some(mut wal_entry) = self.entries.get_mut(&key) {
            match &mut *wal_entry {
                WalEntry::WriterShared { min_segment_id }
                | WalEntry::WriterAndPurge { min_segment_id } => {
                    if new_min_segment_id < *min_segment_id {
                        return Err(UpdateSegmentError::SegmentIdDecreased {
                            table_dir_name: table_dir_name.to_string(),
                            wal_id,
                            current: *min_segment_id,
                            new: new_min_segment_id,
                        });
                    }
                    *min_segment_id = new_min_segment_id;
                }
                _ => {}
            }
        }
        Ok(())
    }

    // Cleans up all locks.
    pub fn clear(&self) {
        self.entries.clear();
        self.table_ids.clear();
        self.next_table_id
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    // Remove a table id mapping.
    // The caller must ensure that there are no locks for the given table.
    pub fn clear_table(&self, table_dir_name: &str) {
        self.table_ids.remove(table_dir_name);
    }
}

#[derive(Debug, Clone)]
pub enum WriterLockError {
    WriterAlreadyExists(String, WalId),
}

impl Display for WriterLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterLockError::WriterAlreadyExists(table_id, wal_id) => {
                write!(
                    f,
                    "cannot acquire writer lock: already exists for table_dir_name={} wal_id={}",
                    table_id,
                    wal_id.value()
                )
            }
        }
    }
}

impl Error for WriterLockError {}

#[derive(Debug, Clone)]
pub enum WriterUnlockError {
    InvalidState(String, WalId),
    NotFound(String, WalId),
}

impl Display for WriterUnlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriterUnlockError::InvalidState(table_id, wal_id) => {
                write!(
                    f,
                    "cannot release writer lock: invalid state (purge exclusive) for table_dir_name={} wal_id={}",
                    table_id,
                    wal_id.value()
                )
            }
            WriterUnlockError::NotFound(table_id, wal_id) => {
                write!(
                    f,
                    "cannot release writer lock: not found for table_dir_name={} wal_id={}",
                    table_id,
                    wal_id.value()
                )
            }
        }
    }
}

impl Error for WriterUnlockError {}

#[derive(Debug, Clone)]
pub enum PurgeLockError {
    PurgeAlreadyExists(String, WalId),
}

impl Display for PurgeLockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PurgeLockError::PurgeAlreadyExists(table_id, wal_id) => {
                write!(
                    f,
                    "cannot acquire purge lock: already exists for table_dir_name={} wal_id={}",
                    table_id,
                    wal_id.value()
                )
            }
        }
    }
}

impl Error for PurgeLockError {}

#[derive(Debug, Clone)]
pub enum PurgeUnlockError {
    InvalidState(String, WalId),
    NotFound(String, WalId),
}

impl Display for PurgeUnlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PurgeUnlockError::InvalidState(table_id, wal_id) => {
                write!(
                    f,
                    "cannot release purge lock: invalid state (writer shared) for table_dir_name={} wal_id={}",
                    table_id,
                    wal_id.value()
                )
            }
            PurgeUnlockError::NotFound(table_id, wal_id) => {
                write!(
                    f,
                    "cannot release purge lock: not found for table_dir_name={} wal_id={}",
                    table_id,
                    wal_id.value()
                )
            }
        }
    }
}

impl Error for PurgeUnlockError {}

#[derive(Debug, Clone)]
pub enum UpdateSegmentError {
    SegmentIdDecreased {
        table_dir_name: String,
        wal_id: WalId,
        current: SegmentId,
        new: SegmentId,
    },
}

impl Display for UpdateSegmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateSegmentError::SegmentIdDecreased {
                table_dir_name,
                wal_id,
                current,
                new,
            } => {
                write!(
                    f,
                    "new minSegmentId must be >= current: table_dir_name={} wal_id={} current={} new={}",
                    table_dir_name,
                    wal_id.value(),
                    current.value(),
                    new.value()
                )
            }
        }
    }
}

impl Error for UpdateSegmentError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    fn wal_id(id: u32) -> WalId {
        WalId::new(id)
    }

    fn seg_id(id: i32) -> SegmentId {
        SegmentId::new(id)
    }

    #[test]
    fn test_writer_lock_unlock() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();

        assert!(lock_manager.is_locked("1", wal_id(1)));
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(0)));
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(1)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();

        assert!(!lock_manager.is_locked("1", wal_id(1)));
    }

    #[test]
    fn test_writer_lock_with_high_min_segment() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(5))
            .unwrap();

        assert!(lock_manager.is_locked("1", wal_id(1)));
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(4)));
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(5)));
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(6)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_purge_lock_unlock() {
        let lock_manager = WalLock::new();
        let max_purgeable_segment = lock_manager.lock_for_purge("1", wal_id(1)).unwrap();

        assert!(max_purgeable_segment.is_none());
        assert!(lock_manager.is_locked("1", wal_id(1)));

        lock_manager.unlock_purge("1", wal_id(1)).unwrap();

        assert!(!lock_manager.is_locked("1", wal_id(1)));
    }

    #[test]
    fn test_double_writer_lock_fails() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();

        let result = lock_manager.lock_for_write("1", wal_id(1), seg_id(5));
        assert!(matches!(
            result,
            Err(WriterLockError::WriterAlreadyExists(_, _))
        ));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_double_purge_lock_fails() {
        let lock_manager = WalLock::new();
        lock_manager.lock_for_purge("1", wal_id(1)).unwrap();

        let result = lock_manager.lock_for_purge("1", wal_id(1));
        assert!(matches!(
            result,
            Err(PurgeLockError::PurgeAlreadyExists(_, _))
        ));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot acquire purge lock: already exists"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));

        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_purge_lock_on_writer_purge_state_fails() {
        let lock_manager = WalLock::new();
        // Writer locks first
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();
        // Purge attaches (now in WriterAndPurge state)
        lock_manager.lock_for_purge("1", wal_id(1)).unwrap();

        // Another purge tries to attach - should fail
        let result = lock_manager.lock_for_purge("1", wal_id(1));
        assert!(matches!(
            result,
            Err(PurgeLockError::PurgeAlreadyExists(_, _))
        ));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot acquire purge lock: already exists"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));

        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_writer_then_purge_shared_mode() {
        let lock_manager = WalLock::new();
        // Writer locks first with minSegment = 5
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(5))
            .unwrap();

        // Purge attaches, should return minSegment - 1 = 4
        let max_purgeable_segment = lock_manager.lock_for_purge("1", wal_id(1)).unwrap();
        assert_eq!(max_purgeable_segment, Some(seg_id(5)));

        // Both are sharing the lock
        assert!(lock_manager.is_locked("1", wal_id(1)));

        // Purge unlocks first, writer should still hold the lock
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
        assert!(lock_manager.is_locked("1", wal_id(1)));

        // Writer unlocks, lock should be released
        lock_manager.unlock_write("1", wal_id(1)).unwrap();
        assert!(!lock_manager.is_locked("1", wal_id(1)));
    }

    #[test]
    fn test_writer_then_purge_writer_unlocks_first() {
        let lock_manager = WalLock::new();
        // Writer locks first with minSegment = 10
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(10))
            .unwrap();

        // Purge attaches
        let max_purgeable_segment = lock_manager.lock_for_purge("1", wal_id(1)).unwrap();
        assert_eq!(max_purgeable_segment, Some(seg_id(10)));

        // Writer unlocks first, purge should still hold the lock
        lock_manager.unlock_write("1", wal_id(1)).unwrap();
        assert!(lock_manager.is_locked("1", wal_id(1)));

        // Purge unlocks, lock should be released
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
        assert!(!lock_manager.is_locked("1", wal_id(1)));
    }

    #[test]
    fn test_purge_then_writer_blocks_until_purge_unlocks() {
        let lock_manager = Arc::new(WalLock::new());

        // Purge locks first
        let max_purgeable_segment = lock_manager.lock_for_purge("1", wal_id(1)).unwrap();
        assert!(max_purgeable_segment.is_none());

        let writer_acquired = Arc::new(AtomicBool::new(false));
        let writer_started = Arc::new(AtomicBool::new(false));

        let lock_manager_clone = Arc::clone(&lock_manager);
        let writer_acquired_clone = Arc::clone(&writer_acquired);
        let writer_started_clone = Arc::clone(&writer_started);

        // Writer tries to lock in another thread, should block
        let writer_thread = thread::spawn(move || {
            writer_started_clone.store(true, Ordering::SeqCst);
            lock_manager_clone
                .lock_for_write("1", wal_id(1), seg_id(3))
                .unwrap();
            writer_acquired_clone.store(true, Ordering::SeqCst);
        });

        // Wait for writer thread to start
        while !writer_started.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(100)); // Give writer time to block

        // Writer should still be blocked
        assert!(!writer_acquired.load(Ordering::SeqCst));

        // Purge unlocks
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();

        // Writer should now acquire the lock
        writer_thread.join().unwrap();
        assert!(writer_acquired.load(Ordering::SeqCst));
        assert!(lock_manager.is_locked("1", wal_id(1)));

        // Clean up
        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_purge_then_writer_then_purge_unlocks_first() {
        let lock_manager = Arc::new(WalLock::new());

        // Purge locks first (exclusive)
        lock_manager.lock_for_purge("1", wal_id(1)).unwrap();

        let writer_acquired = Arc::new(AtomicBool::new(false));

        let lock_manager_clone = Arc::clone(&lock_manager);
        let writer_acquired_clone = Arc::clone(&writer_acquired);

        // Writer tries to lock, will block
        let writer_thread = thread::spawn(move || {
            lock_manager_clone
                .lock_for_write("1", wal_id(1), seg_id(7))
                .unwrap();
            writer_acquired_clone.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(100));
        assert!(!writer_acquired.load(Ordering::SeqCst));

        // Purge unlocks, writer should acquire with its minSegment
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();

        writer_thread.join().unwrap();

        // Verify segment tracking
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(7)));
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(6)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_multiple_tables() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();
        lock_manager
            .lock_for_write("2", wal_id(1), seg_id(0))
            .unwrap();
        lock_manager.lock_for_purge("3", wal_id(1)).unwrap();

        assert!(lock_manager.is_locked("1", wal_id(1)));
        assert!(lock_manager.is_locked("2", wal_id(1)));
        assert!(lock_manager.is_locked("3", wal_id(1)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
        assert!(!lock_manager.is_locked("1", wal_id(1)));
        assert!(lock_manager.is_locked("2", wal_id(1)));
        assert!(lock_manager.is_locked("3", wal_id(1)));

        lock_manager.unlock_write("2", wal_id(1)).unwrap();
        lock_manager.unlock_purge("3", wal_id(1)).unwrap();
    }

    #[test]
    fn test_multiple_wals_same_table() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();
        lock_manager
            .lock_for_write("1", wal_id(2), seg_id(5))
            .unwrap();
        lock_manager.lock_for_purge("1", wal_id(3)).unwrap();

        assert!(lock_manager.is_locked("1", wal_id(1)));
        assert!(lock_manager.is_locked("1", wal_id(2)));
        assert!(lock_manager.is_locked("1", wal_id(3)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
        assert!(!lock_manager.is_locked("1", wal_id(1)));
        assert!(lock_manager.is_locked("1", wal_id(2)));

        lock_manager.unlock_write("1", wal_id(2)).unwrap();
        lock_manager.unlock_purge("1", wal_id(3)).unwrap();
    }

    #[test]
    fn test_update_writer_min_segment_id_increase() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();

        // Initially all segments from 0 are locked
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(0)));

        // Advance minSegmentId to 5
        lock_manager
            .update_writer_min_segment_id("1", wal_id(1), seg_id(5))
            .unwrap();

        // Now only segments >= 5 are locked
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(4)));
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(5)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_update_writer_min_segment_id_affects_purge_lock() {
        let lock_manager = WalLock::new();
        // Writer locks with minSegment = 5
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(5))
            .unwrap();

        // Purge attaches, max purgeable = 5 (the min_segment_id)
        let max_purgeable1 = lock_manager.lock_for_purge("1", wal_id(1)).unwrap();
        assert_eq!(max_purgeable1, Some(seg_id(5)));

        // Writer advances to segment 10
        lock_manager
            .update_writer_min_segment_id("1", wal_id(1), seg_id(10))
            .unwrap();

        // Unlock purge and re-lock to see updated value
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
        let max_purgeable2 = lock_manager.lock_for_purge("1", wal_id(1)).unwrap();
        assert_eq!(max_purgeable2, Some(seg_id(10)));

        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_update_writer_min_segment_id_on_non_existent_wal() {
        let lock_manager = WalLock::new();
        // Should be a no-op, not error
        lock_manager
            .update_writer_min_segment_id("1", wal_id(1), seg_id(10))
            .unwrap();

        // WAL should not be locked
        assert!(!lock_manager.is_locked("1", wal_id(1)));
    }

    #[test]
    fn test_unlock_purge_on_active_writer_only_fails() {
        let lock_manager = WalLock::new();
        // Only writer locks (no purge)
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();

        // Try to unlock purge when only writer has the lock - should fail
        let result = lock_manager.unlock_purge("1", wal_id(1));
        assert!(matches!(result, Err(PurgeUnlockError::InvalidState(_, _))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot release purge lock: invalid state"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));

        // Clean up
        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_unlock_writer_on_purge_exclusive_fails() {
        let lock_manager = WalLock::new();
        // Only purge locks (no writer)
        lock_manager.lock_for_purge("1", wal_id(1)).unwrap();

        // Try to unlock writer when only purge has the lock - should fail
        let result = lock_manager.unlock_write("1", wal_id(1));
        assert!(matches!(result, Err(WriterUnlockError::InvalidState(_, _))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot release writer lock: invalid state"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));

        // Clean up
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_unlock_writer_on_non_existent_wal_fails() {
        let lock_manager = WalLock::new();
        // Try to unlock writer on a non-existent entry - should fail
        let result = lock_manager.unlock_write("1", wal_id(1));
        assert!(matches!(result, Err(WriterUnlockError::NotFound(_, _))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot release writer lock: not found"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));
    }

    #[test]
    fn test_unlock_purge_on_non_existent_wal_fails() {
        let lock_manager = WalLock::new();
        // Try to unlock purge on a non-existent entry - should fail
        let result = lock_manager.unlock_purge("1", wal_id(1));
        assert!(matches!(result, Err(PurgeUnlockError::NotFound(_, _))));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot release purge lock: not found"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));
    }

    #[test]
    fn test_update_writer_min_segment_id_decrease_fails() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(10))
            .unwrap();

        let result = lock_manager.update_writer_min_segment_id("1", wal_id(1), seg_id(5));
        assert!(matches!(
            result,
            Err(UpdateSegmentError::SegmentIdDecreased { .. })
        ));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("new minSegmentId must be >= current"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));
        assert!(err_msg.contains("current=10"));
        assert!(err_msg.contains("new=5"));

        // Original value should be preserved
        assert!(lock_manager.is_segment_locked("1", wal_id(1), seg_id(10)));
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(9)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_update_writer_min_segment_id_on_purge_exclusive_is_noop() {
        let lock_manager = WalLock::new();
        // Only purge holds the lock (no writer)
        lock_manager.lock_for_purge("1", wal_id(1)).unwrap();

        // Updating segment ID should be a no-op (not error, not change anything)
        lock_manager
            .update_writer_min_segment_id("1", wal_id(1), seg_id(10))
            .unwrap();

        // No segments should be locked by writer since there's no writer
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(10)));

        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_clear() {
        let lock_manager = WalLock::new();
        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();
        lock_manager
            .lock_for_write("2", wal_id(1), seg_id(0))
            .unwrap();

        assert!(lock_manager.is_locked("1", wal_id(1)));
        assert!(lock_manager.is_locked("2", wal_id(1)));

        lock_manager.clear();

        assert!(!lock_manager.is_locked("1", wal_id(1)));
        assert!(!lock_manager.is_locked("2", wal_id(1)));
    }

    #[test]
    fn test_concurrent_writers_on_different_wals() {
        let lock_manager = Arc::new(WalLock::new());
        let num_threads = 10;
        let barrier = Arc::new(std::sync::Barrier::new(num_threads));
        let error = Arc::new(AtomicBool::new(false));

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let lock_manager = Arc::clone(&lock_manager);
                let barrier = Arc::clone(&barrier);
                let error = Arc::clone(&error);
                thread::spawn(move || {
                    barrier.wait();
                    if lock_manager
                        .lock_for_write("1", wal_id(i as u32), seg_id(0))
                        .is_err()
                    {
                        error.store(true, Ordering::SeqCst);
                        return;
                    }
                    thread::sleep(Duration::from_millis(10));
                    if lock_manager.unlock_write("1", wal_id(i as u32)).is_err() {
                        error.store(true, Ordering::SeqCst);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
        assert!(!error.load(Ordering::SeqCst));
    }

    #[test]
    fn test_concurrent_writer_and_purge_same_wal() {
        let iterations = 100;

        for i in 0..iterations {
            let lock_manager = Arc::new(WalLock::new());
            let error = Arc::new(AtomicBool::new(false));

            let min_segment_id = (i % 100) as i32;

            let lock_manager_writer = Arc::clone(&lock_manager);
            let error_writer = Arc::clone(&error);
            let writer = thread::spawn(move || {
                if lock_manager_writer
                    .lock_for_write("1", wal_id(1), seg_id(min_segment_id))
                    .is_err()
                {
                    error_writer.store(true, Ordering::SeqCst);
                    return;
                }
                thread::sleep(Duration::from_millis(1));
                if lock_manager_writer.unlock_write("1", wal_id(1)).is_err() {
                    error_writer.store(true, Ordering::SeqCst);
                }
            });

            let lock_manager_purge = Arc::clone(&lock_manager);
            let error_purge = Arc::clone(&error);
            let purge = thread::spawn(move || {
                thread::sleep(Duration::from_millis(1)); // Small delay to increase chance of interleaving
                let max_purgeable = match lock_manager_purge.lock_for_purge("1", wal_id(1)) {
                    Ok(seg) => seg,
                    Err(_) => {
                        error_purge.store(true, Ordering::SeqCst);
                        return;
                    }
                };
                // max_purgeable should be None (no writer) or Some(min_segment_id)
                if let Some(seg) = max_purgeable {
                    if seg != seg_id(min_segment_id) {
                        error_purge.store(true, Ordering::SeqCst);
                    }
                }
                thread::sleep(Duration::from_millis(1));
                if lock_manager_purge.unlock_purge("1", wal_id(1)).is_err() {
                    error_purge.store(true, Ordering::SeqCst);
                }
            });

            writer.join().unwrap();
            purge.join().unwrap();

            assert!(!error.load(Ordering::SeqCst), "Iteration {} failed", i);

            // Ensure clean state for next iteration
            assert!(!lock_manager.is_locked("1", wal_id(1)));
        }
    }

    #[test]
    fn test_writer_waits_for_purge_then_proceeds_with_correct_segment() {
        let lock_manager = Arc::new(WalLock::new());

        // Purge locks first
        lock_manager.lock_for_purge("1", wal_id(1)).unwrap();

        let segment_correct = Arc::new(AtomicBool::new(false));

        let lock_manager_clone = Arc::clone(&lock_manager);
        let segment_correct_clone = Arc::clone(&segment_correct);

        let writer_thread = thread::spawn(move || {
            lock_manager_clone
                .lock_for_write("1", wal_id(1), seg_id(42))
                .unwrap();
            // After acquiring, verify segment is correctly set
            segment_correct_clone.store(
                lock_manager_clone.is_segment_locked("1", wal_id(1), seg_id(42)),
                Ordering::SeqCst,
            );
        });

        thread::sleep(Duration::from_millis(50));
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();

        writer_thread.join().unwrap();
        assert!(segment_correct.load(Ordering::SeqCst));
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(41)));

        lock_manager.unlock_write("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_has_segment_never_locked() {
        let lock_manager = WalLock::new();

        // No locks exist
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(0)));

        // Purge lock exists
        lock_manager.lock_for_purge("1", wal_id(1)).unwrap();
        assert!(!lock_manager.is_segment_locked("1", wal_id(1), seg_id(0)));
        lock_manager.unlock_purge("1", wal_id(1)).unwrap();
    }

    #[test]
    fn test_writer_already_exists() {
        let lock_manager = WalLock::new();

        lock_manager
            .lock_for_write("1", wal_id(1), seg_id(0))
            .unwrap();

        let result = lock_manager.lock_for_write("1", wal_id(1), seg_id(1));
        assert!(matches!(
            result,
            Err(WriterLockError::WriterAlreadyExists(_, _))
        ));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot acquire writer lock: already exists"));
        assert!(err_msg.contains("dir_name=1"));
        assert!(err_msg.contains("wal_id=1"));
    }

    #[test]
    fn test_next_writer_already_exists() {
        let lock_manager = Arc::new(WalLock::new());

        lock_manager.lock_for_purge("0", wal_id(0)).unwrap();
        let lm = Arc::clone(&lock_manager);
        let _first_writer = thread::spawn(move || {
            lm.lock_for_write("0", wal_id(0), seg_id(0)).unwrap();
        });

        thread::sleep(Duration::from_millis(5)); // Ensure first writer is waiting
        let result = lock_manager.lock_for_write("0", wal_id(0), seg_id(1));
        assert!(matches!(
            result,
            Err(WriterLockError::WriterAlreadyExists(_, _))
        ));
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("cannot acquire writer lock: already exists"));
        assert!(err_msg.contains("dir_name=0"));
        assert!(err_msg.contains("wal_id=0"));
    }
}
