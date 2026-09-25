# Handoff: carry live-view checkpoint payloads as native memory

- **Context:** PR #6939 (branch `puzpuzpuz_live_view`), review item 4, "repair routes allocate heap
  objects per key".
- **Status:** not started. This is a follow-up to the native partition-key conversion, which is
  implemented on the same branch in three phases: the partition-map layer, the repair output key
  domain Q, and the seal / publish / transplant path. Start this work only after the key conversion
  has landed.
- **Line numbers:** in the payload sections, every `file:line` refers to the tree before the key
  conversion (HEAD `4064db0cbd` plus the uncommitted review fixes). The key conversion moves most of
  them, so locate code by symbol name. All files are in `core/src/main/java/io/questdb/cairo/lv/`
  unless noted.
- **Other open work:** the last section, "To do: open follow-ups from the review-fix run", lists
  every item the PR #6939 review-fix run found or deferred, payloads aside. Its line numbers refer
  to the tree as committed with this document.

## Why this is a separate step

The checkpoint seal, publish, restore and repair paths carry two kinds of per-key byte data as
`byte[]`:

- **Partition keys:** the encoded `PARTITION BY` value, from `LiveViewSnapshotKeyCodec`.
- **Payloads:** the per-key state images: inline scalar state, fused window-state payloads, and
  member state images.

The user decided to move keys off-heap first, and payloads later.

Keys move first because they carry the correctness hazard that started this work:

- A repair capture (`LiveViewCheckpointTimelineStoreWriter.RepairCapture`) shares its output key
  domain Q by reference with the worker's `KeyedReplay` and repair plan.
- A parked capture outlives the turn that built it.
- Pooling those key arrays would recycle memory still named by a parked capture.

Payloads have no such sharing. Each payload is read by its own publication, inside the lease of
the scratch that owns it, and no payload crosses owners. Keys and payloads are paired only by list
index or by argument position, never through an object that must hold both. Converting keys alone
therefore leaves a coherent design, and payloads can follow as a self-contained change.

## What the key conversion leaves behind (the starting point)

After key phases 1-3 land:

- Keys pass as `(long address, int length)`, valid only for the duration of the call. Holders store
  handles, which are offsets into an arena, never addresses. Handles don't always point into an
  arena the holder owns:
  - frozen holders store handles into their scratch's arena: `FrozenFunction` reaches it through
    `scratch`, and `FrozenWindowState` through `keyArena`;
  - a chained capture's two `LiveViewCheckpointKeyIndex` instances hold handles into the capture
    scratch's arena.

  The new primitives are:
  - `LiveViewCheckpointKeyArena`: an append-only `[int length][bytes]` record arena on
    `MemoryCARWImpl`, tagged `NATIVE_LIVE_VIEW_IN_MEM`;
  - `LiveViewCheckpointKeyIndex`: the native per-operation partition index, with 24-byte slots at a
    load factor of 0.4;
  - `LiveViewCheckpointKeys`: static `hash`, `equals` and `compare` over native memory.

  Reuse them for payloads where they fit. Tests stage and probe keys through the test-side helper
  `LiveViewCheckpointTestKeys`.
- `PartitionMapNode` has no heap representation any more. It decodes key, scalar and refs into a
  per-node native `MutationArena` that the reader owns, tagged `NATIVE_LIVE_VIEW_IN_MEM`. The reader
  clears that arena per decode and releases it at detach once it has grown past 16 MiB.
- `LiveViewCheckpointByteArrayPool` survives, for payloads only. Its users are:
  - `FreezeScratch.frozenByteArrays`: inline state, fused payloads, member images and the ring
    scalar copy;
  - `LiveViewRefreshJob.transplantByteArrays`: fused payloads on the keyed-repair transplant.
    Review item 4 added this pool in its first round, replacing a per-key `new byte[]`.

- **Retention bounds** (all in `LiveViewCheckpointTimelineStoreWriter`):
  - `releaseFrozenGraph` drops a writer's frozen graph when `(long) arrays + frozenKeyCount >
    MAX_RETAINED_FROZEN_ARRAYS`, or when the pooled bytes pass `MAX_RETAINED_FROZEN_ARRAY_BYTES`.
    The `arrays` term counts pooled payload arrays. Adding the frozen key count keeps each seal
    shape's warm limit (0 B per seal) at or above what it was before the key conversion. Moving
    payloads off the pool removes the `arrays` term, so the payload work must re-derive this
    predicate and re-check every shape's warm limit: page-backed, inline, fused, fused with grouped
    members, decimal and ring. `LiveViewCheckpointPublicationAllocationTest` pins those limits.
  - `MAX_RETAINED_FROZEN_KEYS = 32_768` and `MAX_RETAINED_FROZEN_KEY_BYTES = 4_194_304` bound only
    the transplant's key arena. The transplant trims that arena by count and by bytes, and restores
    its handle lists. A payload arena on the transplant would sit beside `transplantKeyArena`.
- Of the `PartitionMapEntry` state, only the scalar stays on the heap. The entry's key `WidthCache`
  is gone. Two WidthCaches remain: the scalar cache, which holds payloads, and the state-page-reference
  cache, which holds no payloads.
- **Tracker charges: none for native key memory.** The frozen key arena, the scratch partition index,
  a chained capture's two key indexes and a capture's copy of Q are not charged to the view's refresh
  tracker. `LiveViewCheckpointKeyArena` and `LiveViewCheckpointKeyIndex` have no tracker binding at
  all. `cairo.live.view.refresh.memory.limit.bytes` therefore counts exactly what it counted before the
  key conversion, when that key memory lived on the untracked heap. The FreezeScratch
  `keyBuffer` / `stateBuffer` and the root-builder staging are still charged, as before.

  An earlier iteration did charge key memory to the view tracker. The exit review showed the cost:
  the index takes about 96 to 192 B per indexed key, and a deep 16-root splice needed 2.55x the
  limit. Within a band of limits that the pre-conversion code tolerated, a repair capture breached
  mid-replay. The mid-drain recovery (`handleRefreshFailure`) then swallowed the breach, and the view
  livelocked, replaying the same O3 correction every turn without applying it. The charge was removed
  for that reason. The livelock handling itself is unchanged and still applies to any other breach.
- `LiveViewWindow.snapshot` (the fallback overlay, used when
  `cairo.live.view.checkpoint.repair.isolated.runtime.enabled=false`) reuses one image array per
  call, also added by item 4's first round. It no longer allocates per key, but it still stages
  the payload on the heap.

## Inventory: payload holders and producers

Paired payload structures:

| Holder | Where (pre-conversion) | Paired with | Notes |
|---|---|---|---|
| `FrozenPartition.scalarState` | TimelineStoreWriter :3650 | the partition's key | Inline image (P1 path :2794) or `NO_BYTES` |
| `FrozenWindowState.payloads` | TimelineStoreWriter :3469 | `keys`, by index | A null payload means "outside Q, keep the predecessor" (:3025, :6375-6377, :6480-6487) |
| `memberImages` (`ObjList<ObjList<byte[]>>`) | TimelineStoreWriter :3263, :3271 | `groupedFreezeKeys`, by index (:2917-2927) | |
| `ChainedPreviousBoundary.windowPayloads` | TimelineStoreWriter :5066 | the chained boundary's window keys | Lives for one repair capture |
| `PartitionMapEntry.scalarState`, `scalarBuffers` | PartitionMapEntry :64, :66 | the entry key (now native) | Entry flyweight |
| `transplantPayloads` | LiveViewRefreshJob :439 | the transplant key handles | One transplant call |

`PartitionMapNode.scalarStates` is not on this list: key phase 1 already moved node scalars into the
reader's native node arena.

Producers:

- `LiveViewWindow.encodeWindowStatePayload*` and `encodeMemberStateImage` (LiveViewWindow
  :3003-3170).
- `LiveViewAccumulatorDescriptor.freezeStateInto`, `resetStateInto` and `restoreStateFrom`.
- `LiveViewCheckpointWindowRoot.encodeAnchorValue` and `readWindowState`.
- `LiveViewCheckpointRangeRingStateReader.encodeScalar` (:463-476) allocates a `new byte[]` per
  ring key, which is a pre-existing per-key allocation.
- `LiveViewCheckpointRangeRingStateBuilder.freeze` (:254) allocates a `new StatePageRef[refCount]`
  per key, also pre-existing.

Heap-side consumers that must follow:

- **Elision compares:** `Arrays.equals(previous.getScalarState(), image)` at TimelineStoreWriter
  :2805, :2935 and :3047. These compare payload against payload and would become
  `LiveViewCheckpointKeys.equals` or `Vect.memeq`, over 128 B, on native payloads.
- **`MutationArena.put(keyAddr, keyLen, byte[] scalar, refs)`:** after the key conversion it
  takes a native key next to a heap payload, and it would take both natively.

## Lifetimes

Payloads follow the lifetime of the scratch or reader that holds them. None of them crosses owners.

| Class | Holders | Must live | Freed today |
|---|---|---|---|
| One cadence seal | FreezeScratch payload lists (`publicationScratch`) | freeze, then buildRoot, then commit; nothing reads them after `releaseScratchBuffers` (:3134) | pool epoch at the next `bind` (:3325) |
| One repair capture, possibly parked | a leased repair FreezeScratch (`acquireRepairScratch` :3143) and `ChainedPreviousBoundary.windowPayloads` | `beginRepair` (:485) to `publishRepair` or discard, then `RepairCapture.close()` (:4719-4741). `close()` can run on the DROP thread under the refresh latch, at registry shutdown, or at worker close | the scratch is released in `close()`'s final `finally` (:4738) |
| Entry flyweight | `PartitionMapEntry.scalarState` / `scalarBuffers` | until the next `of` or lookup into that entry | WidthCaches |
| Transplant | `transplantPayloads` | one transplant call | pool reset per call |

## Proposed design

Scope, as sized by the design pass: about 10 live-view files and about 800 lines, all inside
`io.questdb.cairo.lv`. The `WindowFunction` restore API already takes a native
`LiveViewStatePageReader`, so no change outside the package is expected. Confirm this before
starting.

1. **Payload arena per owning scratch.** `FrozenPartition.scalarState`,
   `FrozenWindowState.payloads` and `memberImages` become handles into a payload arena owned by
   the FreezeScratch (cadence or repair lease). The key conversion's rule applies unchanged:
   - pass `(address, length)` for the duration of a call only;
   - never hold an address across an append to the same arena, because `MemoryCARWImpl` is
     contiguous and moves when it grows;
   - store offsets, not addresses.

   A null payload ("outside Q, keep the predecessor") needs a sentinel handle, for example `-1`.
2. **Encoders write into the arena.** `encodeWindowStatePayload*`, `encodeMemberStateImage`,
   `LiveViewAccumulatorDescriptor.freezeStateInto` / `resetStateInto` / `restoreStateFrom`,
   `LiveViewCheckpointWindowRoot.encodeAnchorValue` / `readWindowState` and
   `RangeRingStateReader.encodeScalar` gain `(addr)` / arena forms.
3. **Map entries go native.** `PartitionMapEntry.scalarState` and `scalarBuffers` move to a native
   buffer, beside the entry's native key buffer. Node scalars are already native in the reader's node
   arena.
4. **The ring's per-key `StatePageRef[]`** in `RangeRingStateBuilder.freeze` is pooled.
5. **Delete `LiveViewCheckpointByteArrayPool`**, the entry's scalar `WidthCache` and the pool
   retention constants. Keep the state-page-reference cache, which holds no payloads. Re-derive the
   `releaseFrozenGraph` predicate without its `arrays` term (see the starting point above), and
   re-derive the entry retention limits once they count native bytes.
6. **Transplant:** `transplantPayloads` becomes a handle list over a transplant payload arena, reset
   per call, with the same seal-derived retention bound item 4 applied to `transplantByteArrays`.

## Decisions to take before starting

- **Memory accounting.** Heap payload arrays are not charged to the view's refresh tracker today. The
  key conversion kept native key memory off that tracker for the same reason (see the starting
  point above). A payload arena charged to the tracker would change what
  `cairo.live.view.refresh.memory.limit.bytes` counts. That limit defaults to 0, meaning no limit.
  Views running near a limit sized for the old accounting would then fail seals and splices sooner,
  and a capture breach mid-replay livelocks through `handleRefreshFailure`. Either keep payload
  arenas untracked, or first make a capture breach fail cleanly, and then document the new
  accounting as a compatibility change in the PR body.
- **Memory tag.** The key conversion uses `NATIVE_LIVE_VIEW_IN_MEM` for new regions, including the
  reader's per-node `MutationArena`s, through a package-private tag-taking constructor. Build-staging
  `MutationArena`s and the FreezeScratch `keyBuffer` / `stateBuffer` stay on `NATIVE_DEFAULT`. Pick one tag for payload arenas,
  and state whether the existing buffers get retagged.

## Risks

- **Stale handles.** With heap arrays, a stale reference reads an old but valid payload. With native
  handles, it reads the wrong bytes or crashes. The elision path compares a predecessor payload with
  a new image, so a wrong-bytes read there would silently skip or force a write. Use handle-bounds
  assertions, as the key conversion does.
- **Close obligations.** Every new arena needs a free on every exit path. That covers constructor
  failure, an exception mid-seal, a parked capture discarded at shutdown or closed on the DROP
  thread, and a repair abandoned by `close()`. The key design already flags a double-release trap in
  `beginRepair`: freeing the capture and also releasing its scratch. Payload arenas that live in the
  same scratch inherit that trap.
- **Byte-identical output.** Payload bytes are persisted into checkpoint roots. The native encoders
  must produce exactly what the heap encoders produce, so there must be no on-disk format change.
  Prove it with golden page-bytes tests and randomized equivalence tests against the heap encoders,
  as key phase 1 does for the compare order.
- **Per-operation malloc.** The FreezeScratch key arena and partition index are released when each
  operation ends. A payload arena released the same way makes each seal pay a malloc and O(log n)
  reallocs. Presize from the operation's own size, never from an
  earlier peak. The key conversion presizes the partition index from each function's own key count,
  capped by Q, because a peak hint would charge one view for another view's width. It does not
  presize the key arena.

## Tests to write

- **Allocation bounds.** Use `ThreadMXBean.getThreadAllocatedBytes`, as
  `LiveViewCheckpointPublicationAllocationTest` and `LiveViewCheckpointOutputKeyDomainTest` do.
  After the conversion, the seal, transplant and fallback-snapshot paths allocate O(1) heap bytes
  regardless of key count. Never assert on wall-clock time.
- **Leak tests.** `assertMemoryLeak` coverage for each new arena owner on success, on exception
  mid-seal, and on parked-capture discard.
- **Golden bytes.** Page bytes for checkpoint roots written before and after, for inline, fused and
  ring state.
- **Elision.** Elision decisions are unchanged: the same roots are written and skipped as before.
- **Suites.** The whole `io.questdb.test.cairo.lv` package passes.

## To do: open follow-ups from the review-fix run

Line numbers refer to the tree as committed with this document. Production code is correct today for
every "must do" item; they are missing tests on correctness paths, and a regression there would go
unnoticed.

### Must do before merge

1. **Pin the resumed no-capture head-miss truncate.**
   - **What it guards:** in `LiveViewRefreshJob.o3HeadMissReplay`, the replacement-commit arm (around
     :9997) runs `truncateOrRetireTimelineOnO3` and writes the `_repairing` marker. It does this for a
     localized head miss that holds no capture, including one that parked and resumed in-process.
   - **The gap:** no lv test fails if the resumed branch skips that call.
   - **What a regression would do:** it leaves permanently wrong cumulative values in the view. For
     example, 150.0 instead of 199.0 at d3 09:05, after a DEDUP UPSERT correction followed by a second
     correction just above a stale root.
   - **Why it matters:** with default settings, no-capture repairs are deep and usually park, so this
     path is common.
   - **Test:** add it to `LiveViewRebuildRestatementGuardTest`, using its existing helpers.
     1. Set `MAX_CHAINED_BOUNDARIES=0` and `REPLAY_MAX_ROWS=1`, then call `seedSixRows("")`.
     2. Commit the day-2 correction, call `driveUntilParked`, then call `driveRefreshToQuiescence` on
        the same job.
     3. Assert `CORRECTED_ROWS`, `countSealedBoundaries("lv") == 4`, superblock
        `normalizedBaseSeqTxn == getLastProcessedSeqTxn()`, and that no repair marker exists.
     4. Restart, then assert `assertRestoredFromTimeline` and
        `getO3BoundaryReplayRows() + getO3ResumeReplayRows() == 0`.

     The mutant gives 6 boundaries, 6 vs 7 seqTxn, and 4 replay rows.
2. **Pin namespace separation in the shared frozen partition index.**
   - **What it guards:** FreezeScratch's scratch-wide `LiveViewCheckpointKeyIndex` separates frozen
     functions by a per-function namespace (`LiveViewCheckpointTimelineStoreWriter` around :3729 and
     :3784).
   - **The gap:** with every function forced to namespace 0, all lv tests pass. For functions that
     share one partition key the mutation is equivalent. For different keys it is not.
   - **What a regression would do:** take `first_value(x)` over a RANGE frame by `sym`, plus `sum(x)`
     over a ROWS frame by `sym2`. A non-chained repair splice across two or more roots, with at least
     128 rows per key per frame, freezes one key's ring rows into another key's partition. A later
     resume then shows key a's `first_value` as 5520.5, which is key d's data, instead of 2520.5.
   - **Test:** after one non-chained splice over that view shape, with the key encoded in `x`
     (`second + 0.5 + 1000 * k`), read every re-versioned function root's ring partitions and assert
     that each decodes to its own key. The end-to-end alternative is two corrections over
     RANGE/ROWS with different keys. It must assert the dispositions (1, then 2) and `rootsVersioned`,
     so that a planner-pricing change fails loudly instead of making the test vacuous.

### Should do

3. **Drop the unread session copy of Q.**
   - **Where:** `LiveViewCheckpointRepairSession.of()` calls `LiveViewCheckpointRepairPlan.copyFrom`
     (around :474), which calls `LiveViewCheckpointOutputKeyDomain.copyFrom`. This gives every
     localized repair session its own native copy of Q.
   - **Nothing reads it:** production code only null-checks that copy (`LiveViewRefreshJob` :9017).
   - **The cost:** the worker plan and the capture hold copies too, so three native copies coexist
     during a repair. At the default 100,000-key cap (`cairo.live.view.checkpoint.repair.scan.max.keys`)
     that is 35.8-48 MB, versus 11-15 MB of heap at base, plus 14-18 MB per extra parked view.
   - **When it bites:** under a process memory limit that leaves 32-48 MB of headroom, head's repair
     fails, and the view is invalidated after 5 retries. Base repairs the view in the same headroom.
     At 32 MB of headroom, the allocation that failed was the session copy.
   - **Fix:** make `RepairPlan.copyFrom` copy only `hasOutputKeyDomain`, plus anything else the
     session actually reads, and replace the null check with that flag. Keep the capture's owned copy,
     which closes the parked-capture aliasing hazard.
4. **Pin the unlocalized no-row retire.**
   - **What it guards:** when an unlocalized O3 rebuild's replay probe finds no row, it retires the
     timeline (`LiveViewRefreshJob` around :9461).
   - **The gap:** no test pins this statement. At base it was one shared statement that two tests
     caught.
   - **What a regression would do:** removing it writes silently wrong window output. For example
     b@25 becomes 600/400 instead of 200/null. That output survives a restart once a seal has run. The
     view stays active; the only signals are a CRITICAL log line and a
     `checkpoint_row_count_mismatches` tick.
   - **Trigger (rare):** an O3 correction that empties the whole unlocalized view, then forward rows,
     then a later O3 row.
   - **Test:** place it next to `LiveViewSmokeTest#testO3HeadMissConvergentEmptyingClearsGhostRows`,
     or in `LiveViewRebuildRestatementGuardTest` using `createUnlocalizedView` with a WHERE filter over
     a DEDUP UPSERT base.
     1. Drive C1, then C2 (the emptying upsert), then C3 (rows a@30 and b@31), then C4 (O3 row b@25).
     2. Assert with `assertQuery(...).returns(...)`: b@25 200/null, a@30 500/null, b@31 900/200.
     3. Optionally add a seal, a restart and `assertRestoredFromTimeline`.
5. **Fix stale comments and log text.**
   - `WalPurgeJob.java:641-643` says a view whose head an O3 repair cleared recovers by rebuilding from
     the applied base and needs no raw WAL. Between a no-capture repair's first turn and its commit,
     the timeline is still intact, and the timeline floor arm (:653-654) holds the WAL.
   - The `truncateOrRetireTimelineOnO3` javadoc (`LiveViewRefreshJob` :4644-4645) says the marker
     forces a mid-repair crash restart to rebuild. The marker is now written at the commit, so it
     covers only a crash after the commit.
   - The localized-no-capture bullet around `LiveViewRefreshJob` :9350-9353 lists the reasons a repair
     holds no capture, but omits "ROWS repair with no key domain".
   - The log line "live view checkpoint timeline repair capture unavailable, retiring instead"
     (`LiveViewRefreshJob` :1833) is wrong: that route truncates rather than retires.
6. **Replace reflection in the transplant retention tests.** Three tests call the private
   `LiveViewRefreshJob.transplantKeyedRepairState` through reflection, passing the primary window as
   its own source. Their verdicts match a real keyed resume, but a non-reflective route exists:
   `setForceOpenSegmentKeyedReplayForTest` plus one wide correction. It costs about 0.7 to 2.4 s more
   per test.
7. **Pin that DROP frees a parked repair's native memory promptly.** This gap already existed at base.
   Right after `DROP LIVE VIEW`, assert `assertNull(getSuspendedRepair())`, and assert that the plan's
   and the capture's key domains report `getSlotCount() == 0`. Today the drop path does free them. A
   regression would hold about 208 B of native memory per key until the worker closes.
8. **Update the PR #6939 description.**
   - **Rollback to 10.0.x (review item 2).** Replace the "this build (version 2) | 10.0.x" row of the
     compatibility table and add a matching upgrade note. Suggested text:

     > **Rolling back to 10.0.x.** A live view created on this build with a single SYMBOL
     > `PARTITION BY` key carries dedup keys on (designated timestamp, key) in its table metadata while
     > `cairo.live.view.checkpoint.repair.sparse.publication.enabled` is on, which is the default.
     > 10.0.x rebuilds such a view from its base on the first start, then honors those keys on every
     > forward commit. Two output rows that share a timestamp and key collapse into one, and queries
     > over the view can also omit other rows until the next 10.0.x restart rebuilds it. Views carried
     > over from 10.0.x carry no such keys and are unaffected. Before rolling back, drop views created
     > on this build and re-create them on 10.0.x.
   - **Window pass-1 skip (review item 5).** Move the sentence about skipping pass 1, and the "about
     two percentage points" tradeoff, under a separate window-functions heading. That heading should
     name the affected query shapes: two-pass `sum` / `avg` / `count` `OVER (PARTITION BY ...)` over a
     DOUBLE-widened argument, with `cairo.sql.window.map.fusion.enabled=true` (the default). Drop the
     claim that the live-view refresh path skips map probes; `LiveViewWindow.updateWindowState` has no
     such skip.
   - **New sections for this run's work:**
     - the O3 repair timing change (truncate, retire and marker now happen at the replacement commit);
     - the native partition-key conversion, and that the per-view memory limit still counts what it
       counted before;
     - the retention bounds;
     - the per-seal native malloc;
     - the log-level change: after an interrupted repair on a view with no generation, recovery now
       logs at ERROR instead of INFO, with the same outcome.

### Could do (improvements, not defects)

9. **Shrink `LiveViewCheckpointKeyIndex`.**
   - **Today:** 24-byte slots at a load factor of 0.4, with power-of-two capacity, give 4.4 to 8 slots
     per entry, or 105-192 B. That is about 5 to 9 times what a table of key references and values
     needs at a normal load factor.
   - **Room to trim:** `version` is always 0 in the frozen partition index, and the stored hash is only
     a probe and rehash optimisation. Growth also allocates the new table before freeing the old one,
     a 1.5x transient.
10. **Make a memory-limit breach during a repair capture fail cleanly.** The mid-drain recovery in
    `handleRefreshFailure` swallows the breach, and the view livelocks: it replays the O3 on every turn
    and logs only at INFO. This predates the change, and any capture breach still reaches it.
11. **Add a byte cap to the retained output-key arenas.** `KeyedReplay.clear()` and `RepairPlan.of()`
    free the arena only past 1,024 keys. Up to 1,000 keys of about 16,000 characters each can pin
    about 32 MiB per domain on a worker. The transplant arena already has a 4 MiB byte cap.
12. **Remove the vestigial `LiveViewCheckpointRepairSession.isRepairMarkerLive`.** It is now true only
    inside the committing turn.
13. **Record the live view's seqTxn in the repair marker.** A restart that finds a committed but
    unapplied replacement could then treat the marker as stale. This predates the change.
14. **Add hysteresis to the keyed-repair scratch.** A steady stream of wide repairs regrows it on
    every arm.
15. **Stop `KeyedReplay.clear()` from allocating on cleanup paths.** It allocates past 1,024 keys, so
    a heap `OutOfMemoryError` there could skip `Misc.free(timelineCapture)` (around
    `LiveViewRefreshJob` :10613-10615). Allocate in `arm()` instead, or reorder the free first.
16. **Widen coverage of the timing change** to cover:
    - the EOF arm;
    - the copy-aside arm (isolated runtime off);
    - the direct `o3Replay` entry;
    - a runtime discard or unwind followed by an in-place restore;
    - an in-place non-drift fault on the unlocalized route;
    - the seal's key-imaging count after a truncating resume.
17. **Pin arena reuse with malloc and realloc counters.** The arena-reuse assertion in
    `LiveViewOpenSegmentKeyedReplayTest` (`testASecondKeyedResume...`) compares addresses. The allocator
    can return the same block, so the test catches a malloc-per-transplant regression only when it
    runs after other tests. Counters would pin reuse deterministically.
18. **Strengthen `LiveViewNoGcSourceHygieneTest`.** Its rule matches by name, so it works as a
    tripwire rather than a proof. It passes `ObjList<byte[]>` whose name contains "payload" or
    "image", passes `byte[] keyBytes` and `byte[] k`, and does not scan other containers.
