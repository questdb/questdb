# Row expiry (`EXPIRE ROWS`)

`EXPIRE ROWS WHEN <predicate> [CLEANUP EVERY <duration>]` on a table or materialized view marks rows as
expired once `<predicate>` is `TRUE` for them. The clause does two independent jobs:

1. **Read-time filter (authoritative).** Every reference to a policied table in a query is rewritten so only
   rows that have **not** expired are visible. A row expires only when the predicate is `TRUE`, so the
   keep-filter keeps rows for which the predicate is `FALSE` *or* `NULL` (QuestDB filtering is three-valued):
   `CASE WHEN (<predicate>) THEN false ELSE true END`, except a designated-timestamp ordering comparison is
   flipped to a bare `ts >= T` so the optimiser can prune partitions. This is the correctness guarantee:
   expired rows are never returned, immediately, regardless of physical cleanup.
2. **Physical cleanup (best-effort, primary-only).** A background job (`RowExpiryCleanupJob`) reclaims disk
   space by dropping fully-expired partitions and compacting partially-expired ones. It is best-effort: the
   read filter is authoritative, so deferred or skipped reclamation only affects disk usage, never query
   results.

The predicate is stored as raw SQL text in `_meta` and re-parsed on every read. `ALTER TABLE|MATERIALIZED
VIEW ... SET EXPIRE ROWS WHEN ...` / `... DROP EXPIRE` change or remove the policy. `tables()` and
`materialized_views()` expose the policy (`expire_predicate`, `expire_cleanup_every`), and `SHOW CREATE`
renders the clause.

## Physical cleanup behaviour

The cleanup job classifies each non-active logical partition (the active/newest partition is never touched):

- **Bounds-DROP** — for a designated-timestamp predicate (`ts < T`), a partition lying wholly below `T` is
  dropped with no scan. This is **unconditionally safe** (every row there, including any concurrent
  back-fill, satisfies `ts < T` and so is expired) and is the primary reclamation for time-based expiry,
  including under heavy ingestion.
- **REPLACE / count-DROP** — for the straddling partition of a time predicate, or for custom (non-time)
  predicates, the partition is scanned and either compacted (`REPLACE_RANGE` keeping the survivors) or
  dropped (when no survivors remain).

REPLACE / count-DROP are **gated for concurrency safety**. Because they compute survivors from a separate
handle, a non-expired row concurrently back-filled into the partition must not be physically deleted. On a
WAL table the survivor scan sees only *applied* state, so the job instead gates on the **sequencer
transaction**: it proceeds only when the table is fully applied at the start of the sweep and no external
transaction has been sequenced (beyond the cleanup's own commits) by the moment of each destructive commit;
otherwise it **defers** to the next sweep. The trade-off is that on a concurrently-written table REPLACE /
count-DROP defer until a quiescent sweep — for time-based expiry the bounds-DROP still reclaims old
partitions, and the straddling partition is reclaimed once it ages fully below `T`. For custom predicates,
physical reclamation of partial partitions may lag while the table is under continuous write load. In all
cases the read filter keeps results correct.

## Known limitations & operational notes

- **Concurrent conflicting DDL on a referenced column (recoverable).** `ALTER ... SET EXPIRE ROWS WHEN <p>`
  rejects a predicate that does not bind to the table's columns, and `ALTER ... DROP / RENAME / ALTER COLUMN
  TYPE` rejects a column that the current predicate references — so sequentially-issued DDL can never leave
  the predicate referencing a missing column. The one residual is a WAL apply-lag race: two **conflicting**
  ALTERs on the same table — `SET EXPIRE` referencing column `X` and a DROP/RENAME/retype of `X` — issued so
  close together that the second compiles while the first is sequenced-but-not-yet-applied. Both checks then
  miss the other, both apply, and the stored predicate references a missing column, which makes every read of
  the table fail (`Invalid column: X`). This requires concurrent conflicting DDL (an anti-pattern) within the
  apply-lag window. **No data is lost** and it is fully recoverable:

  ```sql
  ALTER TABLE my_table DROP EXPIRE;   -- clears the orphaned policy without reading table data; reads work again
  ```

  Avoid changing the schema of a column at the same time as setting an expiry policy that references it.
  (A complete close would require apply-time conflict detection on the WAL apply path; the existing
  optimistic-concurrency machinery is column-structure-specific and does not cover the non-structural
  `SET EXPIRE`.)
- **Cleanup runs on the primary only — but the reclamation still replicates.** The job's `REPLACE_RANGE`
  and `DROP PARTITION` are ordinary WAL transactions, so they are sequenced on the primary and shipped down
  the normal WAL stream; a replica reclaims the identical rows simply by applying them (deterministic apply —
  same survivors, same range, same dropped partitions). A read-only replica cannot originate WAL writes, so
  it neither runs the job (the gate is `!isReadOnlyReplica()`) nor needs to. On promotion the new primary
  starts running it. Reclamation is best-effort, and deferred reclamation never affects correctness — the
  read filter is authoritative on both primary and replica.
- **A mat view ignores its base table's policy during refresh.** A refresh reads the *raw* base table, not
  the base's expired-row-filtered view; the view's own `EXPIRE ROWS` policy (if any) applies when the view is
  queried. This keeps refresh deterministic (a `now()`-based base policy would otherwise be rejected as
  non-deterministic) and avoids folding the base's expiry into the aggregation.
