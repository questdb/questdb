# Row expiry (`EXPIRE ROWS`)

`EXPIRE ROWS` is a retention policy on a **materialized view** that hides — and eventually reclaims — rows
that are no longer wanted, computed continuously as the view refreshes. It is **materialized-view-only**:
`CREATE TABLE … EXPIRE ROWS` / `ALTER TABLE … SET EXPIRE` are rejected (base tables use TTL and storage
policies for retention). It additionally requires a **passthrough** (non-aggregating) view for *every* mode
(scalar `WHEN` included), i.e. `CREATE MATERIALIZED VIEW v AS (SELECT * FROM base)`: the cleanup job
physically reclaims rows, which is only coherent when the view mirrors base rows 1:1, so an aggregating
(e.g. `SAMPLE BY`) view is rejected.

```sql
-- per-row predicate: a row expires when <predicate> is TRUE for it
… EXPIRE ROWS WHEN <predicate> [CLEANUP EVERY <duration>]

-- keep only the latest row per key (dedup to current state)
… EXPIRE ROWS KEEP LATEST [ON <ts>] PARTITION BY <cols> [CLEANUP EVERY <duration>]

-- keep the rows tied at the group max/min of a column (all ties)
… EXPIRE ROWS KEEP HIGHEST|LOWEST <col> [PARTITION BY <cols>] [CLEANUP EVERY <duration>]

-- keep the top-N per group by a column
… EXPIRE ROWS KEEP <N> HIGHEST|LOWEST <col> [PARTITION BY <cols>] [CLEANUP EVERY <duration>]

-- arbitrary window predicate (the escape hatch)
… EXPIRE ROWS WHEN <predicate referencing window functions> [CLEANUP EVERY <duration>]
```

A passthrough view + `KEEP LATEST` is an incrementally-maintained **current-state-per-key** table; `KEEP
HIGHEST/LOWEST` keeps the extreme rows per group; `KEEP <N> …` keeps a leaderboard.

## How it works

1. **Read-time filter (authoritative).** Every reference to a policied view is rewritten so only the kept
   rows are visible — immediately, regardless of physical cleanup. The rewrite depends on the mode:
   - **per-row `WHEN`** → `SELECT * FROM "v" WHERE CASE WHEN (<pred>) THEN false ELSE true END`. A row expires
     only when the predicate is `TRUE`, so `FALSE` *and* `NULL` are kept (QuestDB filtering is three-valued).
     A designated-timestamp comparison is flipped to a bare `ts >= T` so partitions can be pruned.
   - **`KEEP LATEST`** → `SELECT * FROM "v" LATEST ON <ts> PARTITION BY <cols>` (the designated timestamp is
     always used). `LATEST ON` cannot share a level with `WHERE`, so isolating it in this sub-query is exactly
     right: an outer predicate filters the already-latest rows.
   - **`KEEP HIGHEST/LOWEST/<N>` and window `WHEN`** → the keep-filter references a window function, illegal in
     a plain `WHERE`, so it is computed as a boolean column in an inner projection over the whole view and
     filtered in the outer query: `SELECT <cols> FROM (SELECT *, CASE WHEN (<window pred>) THEN false ELSE
     true END __keep FROM "v") WHERE __keep`. `KEEP HIGHEST c` desugars to `c < max(c) OVER (PARTITION BY …)`,
     `KEEP <N> HIGHEST c` to `row_number() OVER (PARTITION BY … ORDER BY c DESC, <ts> DESC) > N`.
2. **Physical cleanup (best-effort, primary-only).** A background job (`RowExpiryCleanupJob`) reclaims disk
   for non-active partitions via `REPLACE_RANGE` on the view's WAL writer: a fully-expired partition is wiped
   (an empty `REPLACE_RANGE` is a pure delete that removes the partition) and a partially-expired one is
   compacted to its survivors. (`DROP PARTITION` via SQL is *not* used — it is rejected for materialized views
   and is replicated as re-compiled SQL.) It is best-effort: the read filter is authoritative, so deferred or
   skipped reclamation only affects disk usage, never query results.

The policy is stored as a single encoded string in `_meta` (so storage/replication/backup are unchanged).
`ALTER MATERIALIZED VIEW … SET EXPIRE ROWS …` / `… DROP EXPIRE` change or remove it, `SHOW CREATE` renders
the clause, and `tables()` / `materialized_views()` expose it (`expire_predicate`, `expire_cleanup_every`).

## Semantics notes

- **NULLs.** The three-valued CASE keep-filter means a `NULL` value is never *less than* the group max (the
  comparison is `UNKNOWN`), so `KEEP HIGHEST/LOWEST` and value-based `WHEN` predicates **keep** rows whose
  value is `NULL`. `KEEP LATEST` uses the designated timestamp, which is never `NULL`. **`KEEP <N>` is the
  exception:** it ranks with `row_number()` and QuestDB has no `NULLS LAST` to force a uniform position, so
  where a `NULL` lands in the ranking is **type-dependent**. Under `DESC`, a floating-point `NULL` (NaN) sorts
  FIRST so it is kept while there is room within `N`, but an integer/timestamp `NULL` (a MIN sentinel) sorts
  LAST so it is expired first; `ASC` mirrors this. A `NULL`-valued row may therefore be kept or expired under
  `KEEP <N>` depending on the column type and direction — use `KEEP HIGHEST/LOWEST` (no `N`) when every `NULL`
  must be kept regardless of type.
- **Ties / determinism.** `KEEP HIGHEST/LOWEST` keeps *all* rows tied at the max/min — monotonic and
  deterministic. `KEEP <N> …` makes the order total by appending the designated timestamp as a tiebreak, so
  the N-th boundary is deterministic (assuming `(col, ts)` is effectively unique; pair with `DEDUP UPSERT
  KEYS` if needed).
- **Monotonicity = cleanup safety.** Physical deletion is only safe when expiry is **monotonic**: a row that
  is expired now must stay expired forever. The relative/window modes are monotonic by construction (the
  "best" row per group is kept, so removing the others cannot change it), as is a designated-timestamp
  predicate like `ts < now()` (a row only gets older). A scalar `WHEN <predicate>`, however, is arbitrary
  SQL and **monotonicity is the author's responsibility**. A non-monotonic predicate such as `WHEN ts >
  now()` expires *future* rows that **un-expire** as `now()` advances past them: the read filter recomputes
  `now()` on every read and is always correct, but the cleanup job could physically delete a row that a later
  read would show. Use only monotonic `WHEN` predicates (time-in-the-past or fixed value thresholds); see the
  limitation below.

## Known limitations & operational notes

- **Reads recompute the keep-set.** A relative/window policy computes its keep-set over the whole physical
  view on every read. `KEEP LATEST` on an indexed symbol key is cheap; the window modes (and non-indexed
  keep-latest) scan the whole view. Aggressive `CLEANUP EVERY` keeps the physical residue — and thus the read
  cost — small. (Performance work is tracked separately.)
- **Cleanup defers under continuous refresh.** Reclamation gates on the sequencer transaction (it proceeds
  only when the view is fully applied and unchanged by others through each commit), so a view being refreshed
  continuously defers reclamation to a quiescent sweep. The read filter stays authoritative meanwhile.
- **`KEEP LATEST [ON <ts>]`.** The optional `ON <ts>` is accepted for familiarity but the view's designated
  timestamp is always used (a table-input `LATEST ON` requires it).
- **Non-monotonic `WHEN` predicates are unsupported for cleanup.** EXPIRE does not reject a non-monotonic
  scalar predicate (detecting it in general would mean statically reasoning about arbitrary SQL, including
  `now()` nested anywhere in the expression), but the cleanup job assumes monotonicity. With a predicate like
  `WHEN ts > now()` the read filter stays correct — a future row reappears once `now()` passes its timestamp —
  while physical cleanup may have already removed it (recoverable only by a full refresh). Treat `now()` as
  "expire things in the past" (`ts < now()`); do not write predicates that expire rows the passage of time
  will later keep.
- **Cleanup runs on the primary only — but reclamation still replicates.** The `REPLACE_RANGE` commits are
  ordinary WAL transactions shipped down the normal stream; a replica reclaims the identical rows by applying
  them. A read-only replica neither runs the job nor needs to.
