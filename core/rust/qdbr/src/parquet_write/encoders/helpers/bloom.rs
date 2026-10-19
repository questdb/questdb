use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::parquet::error::{fmt_err, ParquetResult};

/// Lock the bloom set mutex if present, returning a guard that lives until the
/// caller drops it. Returns `None` if no bloom set was provided.
pub fn lock_bloom_set(
    bloom_set: Option<&Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Option<std::sync::MutexGuard<'_, HashSet<u64>>>> {
    bloom_set
        .map(|arc| {
            arc.lock()
                .map_err(|_| fmt_err!(Layout, "bloom filter mutex poisoned"))
        })
        .transpose()
}
