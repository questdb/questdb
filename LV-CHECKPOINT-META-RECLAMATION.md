# Live-view `_checkpoints/meta/` growth - design and implementation plan

- **Context:** PR #6939 `feat(sql): add live views`, branch `puzpuzpuz_live_view`
- **Verified against:** `5d1dd718b6`
- **Date:** 2026-07-31
- **Supersedes:** the "Critical 8" section of the review handoff, whose fix options
  (per-segment refcounting *or* metadata compaction) were framed against an
  incomplete model of what grows
- **Status:** **All four phases landed, plus the catalogue-entry retirement that
  decision 7 named, the purge cadence its leftover named, the
  uncatalogued-file collection the cadence's own leftover named, and the
  rule unification that collection left over in turn.** Phase 1
  (`a2712f7217`) - section 5.1. Phase 2a (`3175302fdc`) - section 5.2, where
  implementing it replaced the closure-summary mechanism this document originally
  specified with per-segment page counts. Phase 2b (`ab0bd57871`) - section 5.3,
  which took the closure-summary mechanism after all, for a reason the plan had
  not stated. Phase 3 (`d7bf14f612`) - section 5.4. Phase 4 (`a8bc16da33`) -
  section 5.5, the catalogue's own entry retirement, which was decision 7 rather
  than a planned phase. Phase 5 (`ec6493a268`) - section 5.6, the purge cadence,
  which was the "when the sweep runs" leftover decision 7 recorded. Phase 6
  (`afcf762b2a`) - section 5.7, the files a failed publication leaves that no
  count describes, which was the final-orphan leftover section 5.6 recorded and
  turned out to be a leak rather than a lag. Phase 7 (`5d1dd718b6`) - section 5.8,
  which gave reconciliation the catalogue rule too, closing the half of that leak
  a process with no cadence sweep still carried. Class A and Class B are both
  closed as mechanisms, so is the residual each of them left, so is the
  restart-scoped lag in collecting it, and so is the one disposition that was
  neither, under both collectors rather than one.
  **Scope decision, 2026-07-31: the retention horizon is not shipping.** The brief
  was metadata GC, section 1 widened it to bound retained state too, and that
  widening is withdrawn - so Class A is the deliverable and Phase 3 comes back out.
  What remains is the four follow-up tasks in section 12, of which task 1 is that
  removal and task 2 is the regression guard it must not take with it. Class A
  itself needs no further code. The consequence, stated because it is the point:
  **nothing bounds a default install's retained checkpoint state**, and task 3
  requires the PR body to say so; what the cadence and the reconciler bound is the
  garbage beside it.
  The original three semantic questions in section 6 were traced on 2026-07-30 and
  came back clear.

---

## 1. Scope, and one correction to it - the correction withdrawn

The brief was `_checkpoints/meta/`. That is the right primary target - it is the
only surface with **no** reclamation mechanism whatsoever - but the plan below
also covers `_checkpoints/data/`, because the two share a root cause and the data
side needs no new machinery once that cause is addressed.

`data/` is not fine today. Its garbage collection works (reference counts plus
`PurgeSweep`), but nothing ever releases a reference in the steady state, so its
retained-state growth is unbounded in exactly the same way `meta/`'s is. Phase 1
addresses both at once; Phase 2 is metadata-only.

**The second correction this section originally made is withdrawn.** It read that
the plan "also bounds" the two directories, and Phase 3 - the retention horizon -
was how it proposed to do that. Bounding retained state means deciding to stop
keeping live boundaries, which is a policy feature rather than a collector, and
the scope decision of 2026-07-31 (section 11) takes it back out. So this plan
reclaims garbage in both directories and bounds neither: what a generation cannot
reach is collected, and what it can reach stays for as long as the boundary
naming it does. Task 1 in section 12 removes Phase 3; sections 2 through 10 are
left as written, because the growth model and the trade-offs they record are what
a future retention proposal would start from.

The live view's own table (partitions) is explicitly out of scope - TTL support
lands separately.

---

## 2. The growth model

Reachability from the superblock decides whether a byte is garbage or retained
state. The chain is:

```
superblock slot (A/B)
  -> timeline root ref            -> timeline B+ tree
                                       -> entry (one per sealed boundary)
                                            -> checkpoint root
                                                 -> function roots
                                                      -> partition-map pages
                                                           -> state pages   (data/)
                                                 -> anchor root
                                                      -> partition-map pages
  -> row-position-delta root ref  -> row-position-delta B+ tree
  -> segment-directory root ref   -> segment-directory B+ tree
```

The three direct metadata roots are versioned per generation. The newest two
root versions are reachable from the two valid superblock slots, but each
copy-on-write tree can reuse subtrees written by many earlier generations.
Everything hanging off a timeline **entry** stays reachable for as long as that
entry exists, and no code path removes an entry from below.

### Class A - garbage (a collector can reclaim it)

| Item | Per-seal volume | Reclaimable today |
|---|---|---|
| superseded timeline spine pages | `O(log N)` | **yes, since Phase 2a** |
| superseded segment-directory spine pages | `O(log N)` | **yes, since Phase 2a** |
| metadata closure orphaned by a repair splice (`LiveViewCheckpointTimelineStoreWriter.java:574`) | `O(live_keys)` per repair | **yes, since Phase 2b** |
| metadata closure orphaned by `publishTruncate` (`:776`) | `O(dropped entries)` | **yes, since Phase 2b** |
| row-position delta index pages superseded by a repair (section 6.5) | `O(log R)` per repair, where `R` is the number of delta breakpoints | **yes, since Phase 2a** |

Metadata only. `data/`'s equivalent was already handled: both of those sites
release their data-segment references through `applyRootReferenceChanges`, and
neither had a metadata counterpart. That asymmetry was the whole of Class A, and
Phase 2b closed it by giving the metadata half the same reference transaction -
the same call, on the same list, with metadata ids beside the data ones.

Order of magnitude for the steady-state part: single-digit MB/day per view.

Class A is closed. Everything `meta/` and `data/` still hold belongs to a
boundary the timeline names, so what remains is a retention question rather than
a garbage-collection one - which is what Phase 3's horizon moves. The catalogue's
own entries were the one exception - reachable from the newest generation but
naming nothing, once the sweep had unlinked their files - and Phase 4 retires
them. Closing a row of this table means a collector *may* reclaim it, which is
not the same as its doing so: until Phase 5 the collector ran once per process,
so every row above was closed in accounting and open on disk.

One kind of file this table cannot hold at all: the segments a publication renamed
into place and then failed to commit. They are not reachable from the superblock,
so no row of the model above prices them and no reference count decides them -
their whole description is that the catalogue never held them. That put them
outside both classes rather than inside Class A, which is why they survived every
phase up to Phase 6, and outside the id-ceiling rule too once a later publication
allocated past them. Volume is one publication's worth of segments per failure -
two files in the measured case - and the population is every failed retention,
compaction or repair, since a failed seal is the one shape the old rule did catch.
Phase 6 collected them on the sweep cadence and Phase 7 at reconciliation, so
both collectors now decide them; before Phase 7 a process that never swept - the
cadence disabled, or a view that stopped sealing after the failure - still held
them for the life of the directory.

### Class B - retained state (no collector may reclaim it)

Every retained boundary's closure, in both `meta/` and `data/`. From
`freezeFunction` (`LiveViewCheckpointTimelineStoreWriter.java:1217`):

| Function shape | Written per seal | Growth variable |
|---|---|---|
| non-ring (`sum`, `avg`, `row_number`, `rank`, `first_value`, ...) | fresh whole-image state page **per live key**, no diff against the previous boundary (`:1270-1274`) | seals x live_keys |
| ring, under `MAX_LIVE_CHUNKS = 256` | new chunks only; chunk sharing works | cumulative rows |
| ring, above that wall (>~1M live rows/key) | full ring re-image | seals x live_rows |

Plus the matching partition-map rewrite in `meta/`: `putPartition` runs for
**every** live key (`:2313`), so every leaf and therefore every internal node is
rewritten each seal.

Seal cadence is `cairo.live.view.checkpoint.rows = 1_000_000` or
`cairo.live.view.checkpoint.max.duration.micros = 5m`, whichever comes first
(`PropServerConfiguration.java:1526,1530`), so >=288 seals/day for anything
continuously ingesting.

Order of magnitude at 10K live keys, non-ring: ~1 MB/seal, ~275 MB/day, ~100
GB/year, for one view. At 1M keys, ~27 GB/day. Class B is roughly two orders of
magnitude larger than Class A.

**The sharp edge:** there is no dirty-key check. One row into one key satisfies
the cadence, and the seal then re-images every live key. A view with a static key
set and a one-row-per-five-minutes trickle rewrites its complete state 288x/day
forever.

*Phase 1 closed the sharp edge for non-ring functions:* an untouched key's page
and map entry are both reused, so that trickle now costs one key per seal rather
than the whole key set. The rest of the table stands - the retained closure of
every surviving boundary is still Class B.

*Phase 3 bounds the table as a whole,* but only where an operator sets a horizon.
Every row above prices one boundary; the horizon fixes how many boundaries a
generation holds, so the store's footprint becomes `retained_boundaries x
per-boundary cost` instead of `seals x per-boundary cost`. At the default horizon
of zero it fixes nothing, which is the state of open decisions 1 and 2.

---

## 3. Findings that shape the fix

### 3.1 The elision mechanism already exists and is defeated by write ordering

`LiveViewCheckpointAnchorRootBuilder`'s javadoc (`:42-47`) records that
`LiveViewCheckpointPartitionMapWriter` **already drops a put whose key and value
already match**, and that this is what keeps an adjacent seal proportional to the
partitions whose anchor value actually moved rather than to the map's size.

Anchors benefit because an anchor value is a small immutable long: an unchanged
key produces a byte-identical payload, so the writer drops the put.

Function state does not benefit, and the reason is ordering, not a missing
mechanism. `freezeStatePage` (`:1279`) writes a fresh page at a fresh offset
*before* the put, so the entry payload carries a new `(segmentId, offset)` even
when the encoded state bytes are byte-identical. The existing elision can never
fire.

So Phase 1 is not "add dirty tracking to 150 window classes". It is: compare
against the previous boundary's page before writing, and reuse its ref when the
bytes match. The plumbing is already threaded - `freezeFunction` takes
`previousBoundary` and the ring branch already calls
`previousBoundary.find(identity, stateFormatVersion, key)` at `:1258`. Only the
non-ring branch ignores it.

### 3.2 Segment ids are one namespace across `meta/` and `data/`

`skipPublishedSegmentIds` (`:1293`) and `nextFreeSegmentId`
(`LiveViewCheckpointCompaction.java:237`) both treat a candidate id as taken if
*either* `meta/<id>` or `data/<id>` exists. So an id names at most one file, in
exactly one of the two directories.

That means the existing catalogue can hold metadata entries with no change to
`addSegment` / `applyRootReferenceChanges` semantics and no new id space. It also
means `PurgeSweep` can be taught one extra path probe rather than a second sweep.

### 3.3 Cross-boundary metadata sharing already exists

Function partition maps rewrite every leaf today because their state-page refs
always change. Anchor maps do not: their equal-put elision already leaves later
anchor roots pointing into partition-map subtrees written for earlier
boundaries. The timeline, row-position delta index and segment directory also
reuse old subtrees across generations.

Phase 1 extends this existing property to function partition maps; it does not
introduce it. A naive "drop every segment older than the oldest retained
boundary" rule is therefore unsafe even before Phase 1. **Any metadata
reclamation must account for the complete transitive closure of every surviving
root.** Phase 2a does that for the three direct trees by counting pages rather
than closures; Phase 2b does it for the function and anchor maps - which is
exactly where the cross-boundary sharing described here lives - by persisting the
closure in each root and counting roots at the catalogue.

---

## 4. Machinery already in place

Worth inventorying because substantial pieces are reusable, even though the
closure accounting itself is new structure.

| Existing | Location | Reused for |
|---|---|---|
| per-segment `referenceCount` + `retireGeneration` catalogue | `LiveViewCheckpointSegmentDirectoryEntry.java:47,53` | metadata entries - **done, Phase 2a/2b** |
| `addSegment(segmentId, fileLength, referenceCount)` | `LiveViewCheckpointSegmentDirectoryWriter.java:111` | registering metadata segments - **done, Phase 2a/2b** |
| `applyRootReferenceChanges(removed, added, generation)` | `:142` | boundary-metadata reference deltas - **done, Phase 2b**; META (tree) entries move through `releaseMetadataPages` instead |
| purge rule (`oldestValidSlotGeneration >= retireGeneration && minPinnedGeneration > retireGeneration`) | `LiveViewCheckpointDataStore.java:595-626` | unchanged, gained a metadata path probe - **done, Phase 2a**; Phase 2b needed no change to it at all; Phase 4 made it also the proof that an *entry* is dead; Phase 5 runs it on a cadence rather than once per process, and the rule is indifferent to how often it runs |
| persisted data-segment use-count tally inside a function root | `LiveViewCheckpointFunctionRootBuilder.java:156-158`, `adjustSegment:193` | carries the metadata closure beside the data one - **done, Phase 2b** |
| copy-on-write put elision | `LiveViewCheckpointPartitionMapWriter` (documented at `LiveViewCheckpointAnchorRootBuilder.java:42-47`) | Phase 1 |
| high-side timeline truncate | `LiveViewCheckpointTimelineWriter.truncateAbove:205`, `publishTruncate:711` | template for the low-side mirror - **done, Phase 3** |
| generation pins, try-with-resources at all seven sites | `LiveViewCheckpointGenerationPin` | unchanged |
| foreign-layout-version retire path | `LiveViewCheckpointLifecycle.java:54,84,403` | format migration |
| final-orphan id-ceiling scan | `LiveViewCheckpointLifecycle.cleanupOrphans`, `purgeFinalOrphans:204` | the shape Phase 6's catalogue scan copied, and the rule it had to replace - the ceiling it compares against stops naming a file once a later publication has stepped over it. Phase 7 left it only the case the catalogue cannot answer: a directory with no valid generation, where the ceiling is zero |
| deterministic publish-stage crash injection | `setTestFailureStage`, `TEST_FAIL_AFTER_{DATA,METADATA,SUPERBLOCK}_PUBLISH` | crash tests |

Phase 2a supplied the directory's deferred self-registration, per-segment page
accounting for the three direct trees, and the metadata purge path; Phase 2b
supplied the metadata closure accounting for function and anchor maps, and found
that the release sites it needed - the repair splice and `publishTruncate` -
already carried it once the roots stated the closure. Phase 3 added the low-side
timeline truncate and the row-position-delta prune this table listed as absent,
and needed no new reclamation machinery at all: retiring a boundary is the same
reference transaction the truncate already ran, against the closure Phase 2b
taught the roots to state. Phase 4 added one tree operation - an entry removal
that prunes through the existing emit path - and one field on the writer to
carry the sweep's proposal to the seal that applies it. Phase 5 added no
machinery whatsoever: it calls the sweep more than once. Phase 6 added one
directory scan, held against a catalogue read the purge rule was already entitled
to make, and no state at all - it decides and acts in the same pass, which is
what keeps it correct where the deferral it replaced was not. Phase 7 added none
either: it calls Phase 6's scan from the reconciler as well, over a superblock
that method already had open.

---

## 5. Design

Three mechanisms, landed in this order and in four commits, plus a fifth commit
for the residual they left, a sixth for the collection lag that residual's
own fix inherited, a seventh for the one disposition none of the six
accounts for, and an eighth for the collector that still could not take it.
Phase 1 is independent. Phase 2 lands without a
retention horizon and closes existing metadata garbage; it split into 2a and 2b
during implementation, for the reason section 5's Phase 2 preamble gives. Phase 3
depended on Phase 2b for metadata reclamation, and in the event needed no
reclamation code of its own: retiring a boundary is the reference transaction
Phase 2b had already made complete. Phase 4 was not in this plan at all - it is
open decision 7, taken as code once it was the only term left growing with a
view's age. Phase 5 was not either - it is the "when the sweep runs" leftover
Phase 4 recorded, which had become the only thing left between the mechanisms
above and the disk they were supposed to give back. Phase 6 is the final-orphan
leftover Phase 5 recorded, and it is the one whose scope the plan had wrong:
those files were not waiting for a restart, they were lost, because the rule
naming them stops holding as soon as another publication allocates past them.
Phase 7 is the leftover Phase 6 recorded, and it finished that correction: the
cadence sweep collected them, the reconciler still could not, so a process that
never swept lost them exactly as before.

### Phase 1 - state-page elision in the non-ring freeze - LANDED

Closes the seal-rate multiplier on cold keys, for both `data/` and `meta/`.
No on-disk format change. It does require reader/cache plumbing in addition to
the byte scratch buffer.

For a partitioned non-ring function (`freezeFunction:1270`):

1. Look up `previousBoundary.find(frozen.identity, frozen.stateFormatVersion, key)`,
   as the ring branch already does.
2. Encode the state into a reusable scratch buffer rather than straight into
   `dataWriter.beginPage()`.
3. If the previous entry holds exactly one state page of the same stored length
   and the bytes compare equal, reuse its `LiveViewCheckpointStatePageRef`
   verbatim and skip the page write.
4. Otherwise commit the scratch buffer as a new page, as today.

`putPartition` then receives a byte-identical payload for an unchanged key, the
partition-map writer drops the put, and neither the leaf nor its ancestors are
rewritten.

There are two previous-boundary implementations and they need different byte
access:

- `RootPreviousBoundary` resolves metadata only today. Comparing a published
  state page requires a bounded data-segment reader opened with the catalogue's
  checksummed file length. Cache readers by segment id for the duration of the
  seal; do not assume the data segment is already mmap'd.
- `CapturedPreviousBoundary` points into the repair capture's still-unpublished
  temporary data segment. Add a comparison method on the open data writer, or
  retain the encoded bytes with the captured partition. A published-segment
  reader cannot open this case. A first implementation may conservatively skip
  elision for an in-flight previous boundary, but the tests and sizing must say
  so.

The `map == null` scalar branch (`:1225-1232`) also ignores the previous root.
Extend `PreviousBoundary` with scalar lookup and apply the same comparison there.
It is not the wide-key growth term, but leaving it out makes the claimed
non-ring elision incomplete.

Cost: one encode and one page comparison per cold key per seal in place of one
page write. It may map several old data segments because earlier elision spreads
live refs across generations. A cheaper variant - a per-partition dirty or
version stamp on `WindowFunction` so the encode and read are skipped - touches
many window implementations and remains a later optimisation.

Does not apply to ring functions: their entry payload carries an advancing row
count, so no payload is ever byte-identical. Chunk sharing is their analogue and
already exists.

One detail that must not be missed: a reused ref keeps a reference on an older
*data* segment. It has to be reported to `applyRootReferenceChanges` in the
`added` list, or Phase 3 will purge a segment a live root still names. Harmless
until Phase 2b/3 exist, fatal after.

### 5.1 What Phase 1 actually shipped (`a2712f7217`)

Landed on `puzpuzpuz_live_view` as designed above, in
`LiveViewCheckpointTimelineStoreWriter` plus two small additions to
`LiveViewCheckpointDataSegmentWriter` (`addressOfPage`, `getSegmentId`). No
on-disk format change; ~300 lines of production code.

What differs from the plan, and what the implementation turned up:

- **Both previous-boundary implementations compare bytes; neither was skipped.**
  `RootPreviousBoundary` gained a `LiveViewCheckpointSegmentDirectoryReader`
  bound to the old directory root, so a comparison read is bounded by the
  catalogue's checksummed file length exactly as a restore's is, plus an
  eight-slot clock cache of `LiveViewCheckpointDataSegmentReader`.
  `CapturedPreviousBoundary` reads its own unpublished segment through the open
  data writer, and refuses any reference that does not name it - which cannot
  happen, because the first boundary of a repair shares against nothing.
- **A previous page that cannot be read is not a seal failure.** The comparison
  answers false and the freeze writes its own image, which is what it did before
  elision existed; the first such failure per boundary logs at error level. The
  root that still names an unreadable page is a restore's problem, and restore
  already reports it.
- **The scalar (`map == null`) arm is implemented but effectively unobservable.**
  A non-partitioned function's single global state moves whenever any row
  arrives, and a seal requires rows, so the comparison practically never
  succeeds. It is completeness, not a saving, and the tests assert only that the
  path still seals and restores correctly.
- **The "must not be missed" reference-accounting detail was already handled.**
  `LiveViewCheckpointFunctionRootBuilder.build` adjusts the candidate segment
  use counts from the old and new entry of every mutation, whether or not the
  partition-map writer then elides the put, and `getReferencedSegmentIds` unions
  those into the root's own catalogue. A reused reference on an older data
  segment therefore reaches `applyRootReferenceChanges` in the `added` list with
  no new code. Phase 3 inherits that unchanged.
- **A repair capture holds only the keys its replay carried.** Writing the
  capture test surfaced this: a localized repair replays `[L, H)` over runtime
  state the scratch overlay has taken out of the way, so a boundary it
  re-versions images the keys that appear in that range rather than the view's
  whole key set. It is pre-existing behaviour, unrelated to elision, and
  orthogonal to this plan - but it is worth a separate look, because a restore
  from such a boundary starts from a key set narrower than the one the boundary
  originally described.

Costs the change adds, stated because they are real:

- A key that *did* change now pays a partition-map lookup and a page comparison
  before its page is written anyway. A workload that touches every key on every
  seal pays that with nothing to show for it.
- A cold key's live reference stays in the segment it was first written into, so
  one boundary spreads its references across more segments than before and a
  restore maps more of them. The per-segment reference counts also grow by one
  per seal per referenced segment - exactly as ring chunk sharing already made
  them, and equally unreclaimed until Phase 3.

Tests: `LiveViewCheckpointStatePageElisionTest` - a trickle into one key of 24
(exactly one map entry may change per seal; `data/` grows by the touched key's
pages and nothing else), a restart whose head boundary names the first seal's
segment for every cold key, a repair whose capture shares one page across the
boundaries it re-versions, and a RANGE control. The first three fail before the
change and pass after; the control passes either way. The whole
`io.questdb.test.cairo.lv` package (1401 tests) is green.

### Phase 2 - metadata segment reclamation (closes Class A)

Phase 2 split in two while it was being implemented, along the line the
accounting itself draws. **Phase 2a** covers everything reachable from the three
roots the superblock names directly - the timeline, the row-position delta index
and the segment catalogue itself. **Phase 2b** covers everything reachable from a
timeline *entry*: the checkpoint root and the anchor root, function root and
partition-map pages below it. The split is not cosmetic: the two halves want
different mechanisms, because the first is one live version at a time and the
second is one live version per surviving boundary.

The split also reorders the value. 2a closes both steady-state Class A rows and
the delta-index one - the recurring per-seal leak - while 2b closes the two
repair-driven rows and is the prerequisite Phase 3 actually needs. 2a is a strict
prefix of 2b: the kind byte, the purge path and the deferred registration are the
same machinery.

### Phase 2a - direct-root metadata reclamation - LANDED

1. **Catalogue metadata segments.** Add an explicit `kind` byte (DATA / META) to
   the segment-directory leaf entry. The shared id namespace (3.2) makes a kind
   field strictly unnecessary, but `LiveViewCheckpointCompaction` iterates the
   catalogue expecting data segments only, and inferring kind from an `exists()`
   probe makes a missing file indistinguishable from a wrong-kind entry. Pay the
   byte.
2. **Count pages, not closures, for a metadata segment.** This is where the
   implementation departed from what this document originally specified, and
   section 5.2 records why.
   A metadata segment's `referenceCount` is the number of its pages the selected
   generation's trees still reach. A publication adds the pages each tree writer
   wrote and releases the ones its path copy replaced. Zero still means "the
   selected generation names nothing in this file", which is all the purge rule
   reads, and the existing slot-generation and reader-pin gates keep an older
   slot or pinned reader safe without adding to the count.
3. **Report released pages from each tree writer.** The timeline, delta and
   directory writers already read every page they supersede - a path copy emits a
   replacement for every node it decodes and reuses every subtree it does not
   descend into, so "decoded" and "superseded" are the same set. The one
   exception is `truncateAbove`, which drops subtrees without reading them; those
   are walked explicitly, at a cost proportional to what is dropped.
4. **Register each tree's own new segment** with `addSegment(id, bytes,
   pageCount, META)` before publishing the new directory root.
5. **Defer registration of the directory's own segment by one publication.** A
   directory segment cannot record its own exact file length in the immutable
   tree it is still constructing. The superblock carries
   `(pendingDirectorySegmentId, bytes, pages)` and the next publication registers
   it. This is safe because:
   - while it is unregistered it is the selected directory root and cannot be a
     purge candidate;
   - after a crash, an unpublished next segment is removed by the existing final
     orphan rule;
   - once superseded, its entry is present in the new selected directory and the
     normal slot/pin gates apply.
6. **Resolve the directory's own release set to a fixed point.** Which pages a
   publication supersedes depends on which keys are staged, and staging a release
   adds the released segment's own id as a key, so the touched-key set is a
   closure rather than a snapshot. A read-only descent repeats until the staged
   set stops growing.
7. **Extend `PurgeSweep.onEntry`** to build `metaSegmentPath` for META entries
   under the identical slot-floor plus reader-pin rule. The rule needs no change.
8. **Keep byte-counter semantics cumulative.** `metadataBytes`, `dataBytes` and
   `rowPositionDeltaBytes` currently mean bytes written in the history epoch;
   `LiveViewCheckpointTimelineStats.getPhysicalBytes` explicitly says purged
   segments are not subtracted. Do not decrement one of them. Report current
   live and obsolete bytes from the catalogue/sweep instead, and extend the
   existing obsolete-segment metric to include META entries.
9. **Bump `SLOT_FORMAT_VERSION` 2 -> 3.** An older `_timeline` then retires
   through the existing foreign-layout path
   (`LiveViewCheckpointLifecycle.java:403`), which is free: the timeline is
   derived state, so discarding it costs fast restart recovery, not correctness.
   Live views are unreleased, so no real migration exists.

### 5.2 What Phase 2a actually shipped (`3175302fdc`)

Landed on `puzpuzpuz_live_view` as the nine steps above, across eleven production
classes; ~700 lines of production code plus a 400-line test.

**The mechanism changed, and this is the substantive deviation from the plan as
written.** Steps 2-4 of the original design called for persisted transitive
closure summaries in every root and node. Prototyping showed that shape is wrong
for the direct trees on its own terms: a timeline root's closure holds one entry
per distinct segment its live pages sit in, which is `O(N / leafCapacity)` and
grows with the timeline, so writing the summary every seal is quadratic in the
seal count. Per-segment *page* counts are exact, incremental, `O(path length)`
per publication, and need no format change to any root or node at all - the whole
format cost of Phase 2a is the kind field and three superblock longs.

The reason the plan reached for closures is still valid, and it is what makes
Phase 2b a different problem rather than more of the same. Page counts are cheap
to maintain and expensive to *release in bulk*: retiring a boundary means
decrementing the pages that die with it, and which of its pages die depends on
what the neighbouring boundaries still share. For the direct trees that question
never arises - one live version at a time, and the publication knows exactly
which pages it replaced. For boundary metadata it is the whole problem. See
Phase 2b.

What else the implementation turned up:

- **A release against an uncatalogued segment is skipped, not refused.** The
  catalogue is what the purge sweep walks, so an uncatalogued file is one it can
  never unlink and one no count decides the fate of. This is what lets the
  catalogue's own unit tests keep publishing directory trees without registering
  the segments carrying them, and it keeps those tests measuring tree mechanics
  rather than the seal protocol.
- **The two descents are held against each other by an assertion.** `publish`
  resolves its release set with a read-only twin of `applyRec`; an `assert`
  compares what the twin predicted against what the path copy actually visited,
  so the duplicated descent logic cannot drift apart silently.
- **`checkpoint_data_segment_count` stays data-only.** It is a published
  catalogue column documented as data segments, so cataloguing metadata beside
  them must not redefine it. `checkpoint_obsolete_segment_bytes` does span both,
  because both kinds wait on the same fallback slot, so a view that has sealed
  twice now reports a non-zero collection lag where it reported nothing.
- **The catalogue tree itself is still unbounded, and Phase 2a makes it grow
  about three times faster.** A purge unlinks the file and leaves the entry - no
  code path removes a directory entry - so the catalogue holds one entry per
  segment ever written, and its own tree gains a leaf every 64 entries. Those
  leaves are live metadata pages, so the *live* metadata a generation reaches
  grows slowly rather than not at all: measured at 1 segment after 8 seals and 2
  after 32. This is a pre-existing property of the data-side catalogue, but it is
  now the residual growth term in `meta/` and it wants an entry-retirement path
  of its own. Not scoped here. *Phase 4 (`a8bc16da33`, section 5.5) added that
  path.*

Measured on the test workload (one boundary per commit, 24 further seals): the
sweep reclaimed 47 segments, `meta/` grew by 49 files rather than the ~96 it
would have, and the remainder is the two boundary-metadata files per seal that
Phase 2b and Phase 3 own.

### Phase 2b - boundary metadata reclamation - LANDED

Closes the two repair-driven Class A rows, and is the prerequisite Phase 3
depends on. The unit of accounting is the checkpoint root's closure: its anchor
root, function directory, function roots and every partition-map page below them.

The design question was which of the two mechanisms to use, and unlike in
Phase 2a the answer was not obvious from the write cost alone:

- **Closure summaries** (the original design). Extend
  `LiveViewCheckpointFunctionRoot`'s existing `(segmentId, useCount)` list and
  `LiveViewCheckpointRoot`'s segment list to carry META ids beside the DATA ones
  they already hold - the shared id namespace (3.2) means no new id space and,
  for the checkpoint root, no format change at all. Releasing a boundary is then
  what `publishTruncate` already does: decrement every id the root names. The
  cost is at write time and it is real: a function root's summary holds one entry
  per distinct segment its partition-map pages sit in, which after Phase 1's
  elision is roughly one per cold leaf, so ~160 entries at 10K keys - about 2.5
  KB per function per seal.
- **Page counts plus an adjacent-boundary diff.** Keeps Phase 2a's shape and
  writes nothing extra per seal, but retiring boundary `K` requires knowing which
  of its pages `K+1` does not share. A parallel descent of the two partition maps
  that prunes wherever a child ref is identical costs what the two boundaries
  differ by, which is what seal `K+1` wrote - so the cost lands at retirement
  rather than at seal, and only for boundaries that actually retire.

**The first won, and not on the cost trade this document framed it on.** The
page-count variant is unsound at the site Phase 2b exists for. An
adjacent-boundary diff rests on reachability of a page being a contiguous
interval in boundary order - true while boundaries are only ever appended by a
cadence seal, because each seal's map is a path copy of the one below it. A
repair breaks it: `publishRepair` builds each re-versioned boundary from *its
own* old root rather than from the boundary before it, so boundary `K+1` can
supersede a page that boundary `K` and boundary `K+2` both still name. Diffing a
retired range against its two surviving neighbours would then release a page a
live root reaches. Reference counting is order-independent and has no such
precondition.

The write cost that argued against summaries turns out to be moot as well. A
checkpoint root already lists one entry per data segment its state pages sit in,
and Phase 1's elision already spread that over roughly one segment per seal; the
seal already applies that whole list to the catalogue on every publication.
Adding the metadata ids to the same list keeps the same shape and the same
`applyRootReferenceChanges` call - it does not introduce an asymptotic the seal
did not already pay.

Phase 2b also needed:

- registration of the anchor/function/checkpoint-root segments each seal writes,
  which turned out **not** to be the same `addSegment(..., META)` call Phase 2a
  makes - see section 5.3 on why the kind field grew a third value instead;
- release of the closure at `publishTruncate` (`:776`) and at the repair splice
  (`:574`), which is where the Class A volume actually is - and which needed no
  new code at all once the roots stated the closure, because both sites already
  hand the old root's segment list to `applyRootReferenceChanges`;
- the reconciliation rule the crash-safety section states: exactly the currently
  selected directory-root segment may be absent from its own catalogue, and any
  other reachable unregistered metadata segment is corruption. Phase 2a names
  that segment explicitly in the superblock, so the rule is checkable rather than
  inferred; Phase 2b checks it for the three superblock-rooted trees.

### 5.3 What Phase 2b actually shipped (`ab0bd57871`)

Landed on `puzpuzpuz_live_view` across ten production classes; ~250 lines of
production code plus ~200 lines of test. One on-disk format change to a metadata
page, and `SLOT_FORMAT_VERSION` 3 -> 4 so an older `_timeline` retires through
the existing foreign-layout path rather than meeting it.

**The catalogue now keeps two kinds of metadata segment, not one.** The plan said
registration would be "the same `addSegment(..., META)` call Phase 2a already
makes". It cannot be: a META entry's `referenceCount` counts pages and a boundary
entry's counts roots, and one field cannot carry both meanings unchecked. A third
kind - `SEGMENT_KIND_BOUNDARY` - makes the unit explicit, so
`applyRootReferenceChanges` refuses a page-counted segment and
`releaseMetadataPages` refuses a root-counted one. `isMetadata()` becomes "not
DATA" and decides only which directory the file lives in, which is all the purge
sweep ever asked it.

Where the closure lives:

- A **function root**'s existing `(segmentId, useCount)` list carries metadata
  entries beside its data ones - the number of pages of that segment the root
  reaches, its own page plus the partition-map pages below it. No format change:
  the two id spaces are disjoint, so one ordered list serves both.
- An **anchor root** gained the same list, which is the one format change
  (`FORMAT_VERSION` 1 -> 2). It needed it for the same reason a function root
  does: the equal-put elision documented at `LiveViewCheckpointAnchorRootBuilder`
  leaves later anchor roots pointing into map pages older seals wrote.
- A **checkpoint root**'s sorted segment list unions both halves and adds the
  segment carrying its own page and its function directory. No format change at
  all - it was already a list of ids.

Counts are maintained from the delta of one build, never from a walk:
`released + written`, where `written` is the pages the build put in its fresh
segment and `released` is what the path copy took away.

**The subtle half is what "released" excludes.**
`LiveViewCheckpointPartitionMapWriter` now records the segment of every decoded
page it rewrites or drops. A page it decoded and left alone is *not* released,
and after Phase 1 that is the common case rather than a corner: a put whose key
and value already match makes `mutate` answer false, the parent keeps its
existing child reference, and the new map still names that page. Counting it
would release a page a live root reaches. The three drop sites that are not a
rewrite - an emptied child removed from its parent, a collapsed root, a map that
went empty entirely - are released explicitly, and `releaseSource` clears the
node's source so a second visit cannot double-count it.

What else the implementation turned up:

- **The release sites needed no code.** `publishTruncate` and the repair splice
  already build `removedSegmentIds` from the old root's segment list and hand it
  to `applyRootReferenceChanges`. Once that list carries metadata ids, both
  reclaim the boundary metadata by construction. The whole of the "release the
  closure" bullet is one widened list.
- **Registration reuses the data segment's pattern exactly.** Each publication
  registers the segments its boundary build wrote with a count of one and drops
  each from the root's own added set, the way `addSegment(dataSegmentId, ..., 1)`
  plus `dropSegmentId` already worked. A written segment the root does not name
  is refused rather than silently registered, because it would mean the closure
  the root publishes and the files the build wrote have diverged.
- **The reconciliation rule is checked for the three superblock roots.** A
  generation naming a metadata segment its own catalogue does not hold - the
  pending directory segment excepted, which the superblock names - resets the
  directory and rebuilds from the base table, the same disposition a foreign
  layout takes. The boundary half of the rule is still unchecked: proving it
  needs one partition-map walk per surviving boundary, which is the sweep the
  accounting exists to avoid.
- **A synthetic fixture had to learn the protocol.** `LiveViewCheckpointLifecycleTest`
  published superblocks naming a directory root it never registered or declared
  pending. That is exactly what the new rule calls corruption, so the fixture now
  sets the pending triple as a real publication does.

Costs the change adds, stated because they are real:

- A seal stages one catalogue mutation per metadata segment its boundary closure
  names, on top of the data ones it already staged. That is the same shape Phase 1
  gave the data half and it is applied by the same call, but it is more of it.
- The catalogue takes about five entries per seal where Phase 2a took two, so its
  own tree - which never retires an entry - gains leaves about 2.5x faster. The
  residual it leaves behind is measurable: over 24 further seals the live
  tree-metadata segment count moved by 3 rather than by 1. Open decision 7 owns
  that. *Phase 4 closed it: an entry does retire now, once its file is unlinked.*
- One extra metadata page read per seal, for the anchor root the checkpoint root
  builder now opens to union its segment list.

Measured on a 12-seal history with 24 boundary-metadata segments live before the
event: a truncate deep in the history reclaimed 14 of them, and a splice just
below the head reclaimed 2. Both reclaimed nothing at all before the change.

What Phase 2b does **not** change: a cadence seal still leaves its boundary
metadata behind, because the boundary is live. At the test workload's shape that
is about three `meta/` files per seal; Phase 3's horizon retires them, and
without one set they stay.

One thing worth recording for Phase 3, because it inverted an assumption while
the tests were being written: **which disposition a correction takes is not
"deeper means more drastic".** In the `ROWS 3 PRECEDING` view the cases use, a
correction just below the head classified a converged suffix and spliced, while
one seven boundaries down could not and truncated. Section 6.4 already says the
lower bound comes from the function's dependency rather than from checkpoint
availability; this is the same point from the other end, and it means a Phase 3
horizon test must assert which publication ran rather than infer it from how deep
the correction was.

### Phase 3 - retention horizon (closes Class B) - LANDED

The retention semantics are clear - see section 6 - but implementation depends on
Phase 2b for metadata reclamation. `publishTruncate:711` is a close template for
the publication, with the delta-index exception below. The timeline pages a
low-side truncate drops are already released by Phase 2a's dropped-subtree walk,
which `truncateBelow` inherits by mirroring `truncateAbove`.

1. **`LiveViewCheckpointTimelineWriter.truncateBelow`** - the mirror of
   `truncateAbove:205`. Keep the high suffix by page reference, path-copy the
   boundary spine, promote a subtree that collapses to a single child.
2. **`LiveViewCheckpointTimelineStoreWriter.publishTruncateBelow`** - the mirror
   of `publishTruncate:711`. Walk the dropped range, release each entry's data
   *and* metadata references, and decrement `logicalStateBytes`. The physical
   byte counters remain cumulative per Phase 2a step 8.
   Carry `normalizedBaseSeqTxn` and `coveredLvSeqTxn` forward untouched exactly as
   `publishTruncate` does (`:795-800`) - that is what keeps the WAL purge floor
   still (section 6.1).
3. **Diverge from the template on two fields.** Carry `seedCursorOffset` forward
   rather than clearing it (a low-side truncate does not invalidate a mid-sweep
   resume point). Prune the row-position delta index, but preserve its prefix
   contribution: let `K` be the first surviving timeline key and `P` the sum of
   every delta entry below `K`; drop those entries and add `P` to the delta at
   `K` (inserting one when absent). Every surviving lookup then sees the same
   prefix sum as before. Simply deleting breakpoints keyed to dropped boundaries
   is incorrect because each difference applies to the entire later suffix.
   Implement this as a low-side prune operation on
   `LiveViewCheckpointRowPositionDeltaWriter`, path-copying the boundary spine and
   folding the discarded subtree sums into `K`.
4. **Policy.** Retain boundaries covering the last D of event time, or the newest
   K. Once view TTL lands, derive D from it so the checkpoint horizon never sits
   below data the view still retains.
5. **Semantics of the horizon.** The oldest retained boundary bounds how far back
   an out-of-order correction can *resume* from a sealed anchor. It does not bound
   how far back a correction can be *localized* - that comes from the function's
   own dependency (section 6.4). Below the horizon the plan takes
   `DISPOSITION_BOUNDARY_REBUILD`, an already-named and already-priced
   disposition. A cost dial, not a correctness change.

### 5.4 What Phase 3 actually shipped (`d7bf14f612`)

Landed on `puzpuzpuz_live_view` as the five steps above, across nine production
classes; ~450 lines of production code plus ~900 lines of test. One superblock
format change and `SLOT_FORMAT_VERSION` 4 -> 5, for a reason no step anticipated
(the entry count, below).

**Policy: an event-time window, `cairo.live.view.checkpoint.retention.micros`,
default zero (disabled).** Step 4 offered "the last D of event time, or the newest
K" and left the choice to a human. Event time won on cost rather than on
semantics: the floor is `head.maxTimestamp - D`, so deciding whether anything
retires is one `O(log N)` predecessor probe, while "newest K" would need the
timeline's k-th-from-the-end key and the reader exposes no such navigation. It
also matches where decision 1 says this ends up - derived from view TTL, which is
an event-time window.

The pass runs after every seal rather than on a cadence of its own. When the
horizon has nothing to retire it costs that one probe; once saturated it retires
one boundary per seal, which keeps the footprint flat instead of sawtoothing, at
the price of one extra publication per seal. That price is real and it is the main
cost this phase adds: a seal that retires something now writes a second timeline
segment and a second catalogue segment.

What differs from the plan, and what the implementation turned up:

- **A surviving child's stored minimum key has to be raised.** The plan described
  `truncateBelow` as the mirror of `truncateAbove`, and structurally it is - same
  spine copy, same single-child collapse. It is not symmetric in one respect the
  plan did not name: a prefix truncation changes the *minimum* of the straddling
  child, and navigation reads the minimum a parent stores rather than the subtree
  under it. A stale-low minimum is not immediately wrong - a descent into it finds
  nothing and reports absence, which is the right answer - but it misclassifies
  the straddle on the *next* truncation, so the recursion returns the new minimum
  and the parent stores it. `truncateAbove` never had to: dropping a suffix leaves
  every surviving child's minimum where it was.
- **The delta prune needed a read-only probe, for the empty-tree case rather than
  the no-op one.** Refusing a prune that discards nothing is obvious. The case
  that forced a full probe is subtler: the leaf a prune descends into can be
  emptied while leaves to its right survive, so "this leaf went empty" cannot
  decide whether to insert the folded breakpoint, and "the tree went empty" is not
  knowable at the leaf. One root-to-leaf descent answers all three questions
  (anything below, anything at or above, and the sum below), which lets the prune
  take the empty-tree disposition - a null delta root - without opening a segment
  for a tree with nothing in it.
- **The superblock had to learn how many boundaries the epoch retired.**
  `checkpoint_timeline_entries` was `nextCheckpointId`, on the stated grounds that
  ids are allocated from zero and monotonically. Retention breaks that: the id
  counter keeps climbing while the live set stays flat, so the column would grow
  without bound while reporting a number that is meant to be bounded. A
  `retiredCheckpointCount` field makes the count exact. Note this was **already**
  wrong before Phase 3 - `publishTruncate` has always dropped boundaries without
  adjusting the counter - so the fix is a correction as much as an addition, and
  the high-side truncate maintains the field too.
- **The horizon never retires the head.** A floor above every boundary is refused
  outright rather than published: a timeline with no boundary restores by
  rebuilding from `START FROM`, which is not a retention outcome. Both the tree
  operation and the publication check it, so a mis-set horizon costs nothing.
- **`seedCursorOffset` carries forward, and that is the one field where the
  template is wrong.** `publishTruncate` clears it because it discards the head
  the sweep was resuming into. A retention pass drops boundaries the sweep is long
  past, so clearing it would lose a resume point that is still correct. Step 3
  predicted this; it is recorded here because it is the only line of
  `publishTruncate` that could not be copied.
- **The release sites needed no code, again.** Retiring a boundary is
  `applyRootReferenceChanges` over the closure the root already states, which is
  exactly what `publishTruncate` does to the other end of the timeline. The whole
  of "release the data *and* metadata references" is one loop that already existed.

Costs the change adds, stated because they are real:

- One extra publication per seal once the horizon is saturated, as above.
- A correction below the horizon has no sealed anchor to resume from. For a view
  whose every function localizes this costs nothing - the tests confirm a bounded
  `RANGE` view still plans a dependency-localized repair with nothing sealed under
  the change - but a view carrying a function with no finite dependency reads its
  whole history instead. Section 6.4 prices that population; the shape census that
  would say how large it is has still not been done.
- The `SLOT_FORMAT_VERSION` bump retires an older `_timeline` through the
  foreign-layout path, which costs a rebuild of derived state. Free in practice,
  since live views are unreleased.

Measured on the test workload (one boundary per commit, a 60-second event-time
horizon at a 10-second commit spacing) between 10 seals and 30: the boundary count
held at 7 and 7, `meta/` went 20 -> 24 files and `data/` 10 -> 11 over the 20
further seals. Without a horizon those 20 seals add 20 boundaries and about four
files each. The residual that does move is the segment catalogue's own tree
(decision 7, closed by Phase 4) plus the one data segment a cold key keeps its
elided reference in.

**What Phase 3 does not do: change the default.** The horizon ships at zero, so a
default install still grows exactly as it did before this phase. Open decisions 1
and 2 own that, and they are now the only thing between the mechanism and a
bounded default.

### Phase 4 - catalogue entry retirement - LANDED

Not a planned phase: this is open decision 7, taken as code once the three
mechanisms above had left it as the only term in `_checkpoints/` that still grew
with a view's age. The catalogue held one entry per segment ever written, because
a purge unlinks the file and nothing removed the entry, and its own B+ tree
gained a leaf every 64 of them - the residual section 5.2 named and Phase 2b made
about 2.5x faster by cataloguing boundary segments beside the rest.

Decision 7 already stated both halves of the answer: retiring an entry is safe
exactly when its file has been unlinked, which the sweep proves, and the sweep
publishes no generation, so the removal has to be staged into a publication.

### 5.5 What Phase 4 actually shipped (`a8bc16da33`)

Landed on `puzpuzpuz_live_view` across six production classes; ~215 lines of
production code, over half of it comment, plus ~370 lines of test. No on-disk format change and no
`SLOT_FORMAT_VERSION` bump: an entry leaving a leaf is an ordinary copy-on-write
mutation of a tree whose layout is unchanged.

**The hand-off is a proposal, not a transfer.** `PurgeSweep` collects the id of
every entry it leaves with no file - the ones it unlinked in this pass *and* the
ones an earlier pass unlinked that no publication has carried away yet - and
`PurgeResult` / `ReconcileResult` carry the list out.
`LiveViewCheckpointTimelineStoreWriter` holds it per checkpoint directory between
the reconciliation that produced it and the seal that applies it. Re-proposing
the already-gone ones is what makes the whole thing crash-proof without a durable
queue: a failed publication, a `BoundaryNotAboveHeadException`, or a process that
dies between the two loses nothing, because the next sweep says it again.

That is not a corner case, it is the ordinary production path. `CairoEngine`
reconciles every checkpoint directory at boot and drops the list, because it
publishes nothing; the writer then reconciles the same directory again at its
first seal of it, and the second sweep re-proposes what the boot sweep unlinked.
Without the re-proposal, the boot sweep's work would never reach the tree.

Where the work lands:

- **`removeSegment` stages a removal like any other mutation**, so the removals
  path-copy once alongside the publication's registrations and releases rather
  than in a pass of their own. The seal's `releaseOwnPages` pre-pass and the path
  copy descend the same paths for a removal key as for any other, so the
  assertion that holds those two descents against each other needed no change.
- **A node that empties writes no page.** `emitNodes` returns without writing at
  a count of zero, so the parent keeps no child reference to it, and a parent
  that loses every child empties in turn. That is the whole of the pruning: no
  rebalancing pass, no merge rule, no minimum-occupancy invariant. A B+ tree
  whose only deletion pattern is "the low ids die first" gets a correct shape out
  of the emit path alone.
- **A surviving child's stale-low minimum is left alone,** for the reason
  section 5.4 gives for `truncateBelow`: navigation reads the minimum a parent
  stores, and a descent into a subtree whose real minimum has risen finds nothing
  and reports absence, which is the right answer. Unlike the timeline's prefix
  truncate, the catalogue never has to raise it, because it takes no second
  operation whose straddle classification would depend on it.
- **The tree may empty entirely,** and `LiveViewCheckpointMetaSegmentWriter`
  gained a `discard()` so that publication leaves no page-less segment behind.
  This is unreachable from a real publication - every one of them registers at
  least the timeline segment it just wrote, so `staged` always holds an insert -
  but it is reachable from the catalogue's own unit tests, and a null root is the
  empty shape `begin()` already accepts, so supporting it costs less than
  refusing it.

**Two guards keep the two units from meeting.** `removeSegment` refuses a
still-referenced entry, whose file cannot have been unlinked, and one this
publication registers; `applyRootReferenceChanges` and `releaseMetadataPages`
refuse an entry already staged for retirement. Neither can fire from a correct
sweep - the file is gone, so nothing can reach it - which is exactly why they are
worth having: firing means the count the sweep acted on and the closure a root
publishes have diverged.

Crash safety adds no case. The removal commits with the generation carrying it,
and the fallback slot keeps its own copy of the entry at a zero count, so nothing
under that generation reads the missing file and a sweep over it re-proposes the
retirement rather than faulting.

**What Phase 4 does not change: when the sweep runs.** `purge()` is called from
`LiveViewCheckpointLifecycle.reconcile` and nowhere else, and a writer reconciles
a directory once - at its first seal of it. So entry retirement follows the
sweep: one seal after a restart's reconciliation, not on a cadence of its own.
That is the same bound the segment *files* already have - nothing unlinks a
superseded segment mid-process either - so the catalogue is now exactly as
bounded as the directory it catalogues, and making the sweep periodic would move
both at once. Worth doing, and out of scope here. *Phase 5 (`ec6493a268`,
section 5.6) did it, and moved both.*

Measured on the test workload (one boundary per commit, 32 seals, then one
reconciliation): the sweep left 57 of the catalogue's 159 entries naming unlinked
files, and the seal that followed removed exactly those 57 and no others.

### Phase 5 - purge cadence - LANDED

Not a planned phase either: this is the leftover section 5.5 names. Every
mechanism above decides *what* may be collected; none of them decides *when*
anything is. `purge()` had exactly one caller, and a worker reaches it once per
checkpoint directory - at its first seal of it - so a process that runs for a
week collects what its first seal could see and nothing after. The accounting was
complete and the disk did not come back.

### 5.6 What Phase 5 actually shipped (`ec6493a268`)

Landed on `puzpuzpuz_live_view` across eight production classes; ~290 lines of
production code, of which the reclamation logic is about forty - the rest is
javadoc, a result class and the five files a new config key has to touch - plus
~190 lines of test. No on-disk format change and no `SLOT_FORMAT_VERSION` bump:
running an existing pass more often changes nothing a file records.

**`cairo.live.view.checkpoint.purge.interval`, counted in seals, default one.**
It matches the compaction interval beside it - the same counter shape, and for
the same reason that one counts seals rather than testing a base seqTxn modulo -
and zero disables it, which restores the reconcile-only behaviour exactly.
Default-on is the deliberate part: unlike the retention horizon, whose default
decision 2 leaves to a human because a horizon costs repair reach, a sweep costs
nothing semantically. It unlinks files that nothing can reach, under a rule the
reconciler already ran on every restart.

`LiveViewCheckpointTimelineStoreWriter.sweep()` is the reclamation half of
`reconcile` on its own, without the epoch, repair-descriptor and orphan rules
that only a directory nobody has published under yet needs. It opens the
generation, refuses one some other history epoch owns, runs
`LiveViewCheckpointDataStore.purge()`, and stores the retirement proposal in the
per-directory map Phase 4 already gave the writer. `LiveViewRefreshJob` runs it
after retention and compaction, so it walks a catalogue both of them have
finished writing, and gates it on an actual seal exactly as those two are.

What the implementation turned up:

- **The proposal supersedes rather than accumulates.** A sweep re-derives every
  entry whose file is already gone, so the list it produces is a superset of
  whatever an earlier sweep left pending, minus what a publication has since
  carried away. Replacing the map entry is therefore correct, and an empty result
  means the pending list is genuinely empty rather than unknown.
- **Nothing about the purge rule needed to change, and that is the whole
  argument.** `oldestValidSlotGeneration >= retireGeneration` already means the
  fallback slot has caught up with the retirement, so a segment retired at
  generation `G` becomes collectable once `G+1` commits and not before - whether
  the sweep that notices runs then or a week later. Frequency is not a term in
  the rule, which is why a cadence needs no argument about safety, only about
  cost.
- **The shared id namespace is not weakened by unlinking sooner.**
  `skipPublishedSegmentIds` and `nextFreeSegmentId` allocate from the monotonic
  `nextSegmentId` and only skip *forward* over files that exist, so unlinking a
  file below that ceiling never makes its id reusable. Without that property a
  cadence sweep would hand out ids the catalogue still holds entries for.
- **Two `live_views()` columns change meaning slightly.**
  `checkpoint_data_segment_count` and `checkpoint_obsolete_segment_bytes` are
  recorded by whatever sweep last ran, so they used to read NULL until a restart
  had reconciled the view. They now fill in from the first seal. That is better
  observability and a visible change, and `LiveViewSmokeTest` states it.
- **Two existing cases had to be pinned to interval zero.** They measure the
  reconciliation sweep - one counts what it reclaims, the other needs a run of
  seals to accumulate dead catalogue entries for one publication to retire - and
  with the cadence on there is nothing left for either to find. Pinning them
  keeps the reconcile-only configuration covered, which is still a supported
  setting.

Costs the change adds, stated because they are real:

- One metadata-store open and one catalogue walk per seal, against the several
  segments that seal already writes. The walk is proportional to the catalogue,
  which Phase 4 made proportional to what the view holds rather than to its age.
- More `unlink` syscalls spread across the process rather than batched at
  restart. Same total, different distribution.

Measured on the test workload (one boundary per commit, 32 seals, no restart and
no explicit reconciliation): the catalogue ended holding **1** zero-reference
segment whose file was still on disk, against **59** for the same run with the
cadence disabled - the one is what the fallback slot still protects, and the 59
is about two per seal that nothing was going to collect until the process ended.
`meta/` ended at 69 files rather than 128, and held 20 at seal 8 rather than 32.
The 69 that remain are retained state, not garbage: with no retention horizon set
every boundary is live, and its metadata with it.

**What Phase 5 does not change: the final-orphan pass.**
`cleanupOrphans` / `purgeFinalOrphans` still run only at reconciliation. They
collect the final-name files a crashed or failed publication leaves above the
valid slots' id ceilings, which is a crash-recovery rule rather than a
steady-state one - but a publication that fails inside a running process leaves
the same shape, and those files still wait for a restart. Smaller than what the
cadence now collects, and out of scope here. *Phase 6 (`afcf762b2a`, section
5.7) collected them, and found the wait was not the whole of it: the rule those
files waited for stops being able to name them once any later publication has
stepped its allocation over their ids, so they were not late but lost.*

### Phase 6 - uncatalogued-segment collection - LANDED

Not a planned phase either: this is the leftover section 5.6 names, and reading
it against the code turned it from "collected late" into "not collected at all".

Three publications can leave a final-name file behind - retention, compaction and
repair - and none of them re-arms a reconciliation the way a failed `append`
does. So nothing read the id ceiling that named their files, and the next seal's
`skipPublishedSegmentIds` stepped over them and raised the ceiling past them. The
id-ceiling rule then has no way to tell those files from live segments, at that
seal or at any restart after it. `LiveViewCheckpointDataStore` already recorded
the consequence in a javadoc - a compaction target abandoned under a fail-closed
catalogue read "leaks it once a publication has stepped over that id" - without
naming it as reachable from the other two.

### 5.7 What Phase 6 actually shipped (`afcf762b2a`)

Landed on `puzpuzpuz_live_view` across four production classes; ~145 lines of
production code, of which the pass itself is about eighty and the rest is a
result field and a test-only failure stage, plus ~165 lines of test. No on-disk
format change and no `SLOT_FORMAT_VERSION` bump: the pass reads a catalogue the
publication protocol already maintains.

**The rule is the catalogue's silence, not an id comparison.**
`LiveViewCheckpointLifecycle.purgeUncataloguedSegments` removes every final-name
file in `meta/` or `data/` that the newest durable generation neither catalogues
nor names as its pending directory segment. Since Phase 2a/2b the catalogue holds
an entry for every segment a published root can reach - data, tree metadata and
boundary metadata alike - and the pending directory segment is the one documented
exception, because a tree cannot list the file it is being written into. A file
outside both sets is reachable from nothing, whatever its id, and stays so
however far the ceiling travels. `LiveViewCheckpointTimelineStoreWriter.sweep`
runs it beside `purge()`, so the two halves of one sweep decide the fate of the
segments a generation named and of the files it never named.

What the implementation turned up:

- **Nothing has to advance past these ids first.** The id-ceiling rule defers
  because it removes a file only after a new slot commits above it; this pass
  removes at once, because the monotonic `nextSegmentId` is what preserves
  allocation order on its own. That is Phase 5's third bullet read the other way
  round: unlinking never lowers `nextSegmentId`, and the id skip only ever moves
  forward, so an id can never come back into circulation carrying a meaning some
  root remembers. Removing the deferral is also what removes the leak - the
  proposal-and-apply shape Phases 4 and 5 use would have re-introduced it,
  because the ceiling can move between the two.
- **Fail-closed at every step, and the asymmetry is why.** A catalogue this build
  cannot read, a superblock naming no directory root, or a slot that lost the
  newest-generation race all leave every file where it is. Keeping a dead file
  costs disk; unlinking a live one costs the timeline. It is the same disposition
  and the same test `isSegmentDurablyCatalogued` already applied to an abandoned
  compaction target, which is now the pass that finishes that job rather than the
  one whose javadoc apologised for not finishing it.
- **Temporary files stay out of it.** A `.tmp` belongs to whichever writer holds
  it open - a repair capture spans several `capture` calls before it commits - so
  ownership rather than reachability decides its fate. Reconciliation runs where
  no writer can own one, which is where that decision belongs, and it keeps it.
- **The failure shape needed a test hook of its own.** Every existing crash
  injection fires in a seal, and a failed seal is exactly the case that *does*
  re-arm the reconciliation, so it could not produce the shape this phase exists
  for. `TEST_FAIL_AFTER_RETENTION_METADATA_PUBLISH` fires only in
  `publishTruncateBelow`, so the seal gets through and the retention pass behind
  it does not.

Costs the change adds, stated because they are real:

- One directory listing of `meta/` and `data/` per sweep, plus one `O(log N)`
  catalogue probe per file found. Against the several segments a seal already
  writes and the catalogue walk `purge()` already makes, but it is a second scan
  of the same two directories.
- The pass reads the catalogue through its own reader rather than the one
  `purge()` just used, so a sweep opens the directory root twice.

Measured on the test workload (one boundary per commit, a 60-second horizon, one
retention publication failed after its metadata publish): that publication left
exactly two files behind - its timeline path copy and its catalogue path copy -
and the next cadence sweep removed both. With the cadence
disabled the same two survived five further seals, by which point the durable
`nextSegmentId` ceiling had reached 84 against their ids of 47 and 48; a full
reconciliation then took `meta/` from 70 files to 23 and left both of them
exactly where they were.

**What Phase 6 does not change: reconciliation's own orphan rule.**
`cleanupOrphans` still records the id ceiling and `append0` still applies it,
which is what a directory whose first seal in this process follows a restart
needs, alongside the `.tmp` and crashed-repair rules only a reconciliation runs.
The two now overlap, with the catalogue rule strictly stronger for final names,
and unifying them would mean giving reconciliation a catalogue read it currently
does without. Worth doing, and out of scope here. *Phase 7 (`5d1dd718b6`,
section 5.8) did it, and the read turned out to be one the reconciler already
had open.*

### Phase 7 - the catalogue rule at reconciliation - LANDED

Not a planned phase either: this is the leftover section 5.7 names, and reading
it against the code turned "the two overlap" into "one of the two collectors is
still running the rule that decays".

Phase 6 gave the cadence sweep a rule that holds however far the id ceiling has
travelled, and left the reconciler the one that does not. That is only a
duplication where a sweep runs. Where none does - `purge.interval` at zero, or a
view that stops sealing after the failed publication - reconciliation was the
only collector left, and it still compared ids against a ceiling later seals had
moved past. So the leak Phase 6 closed for a running process stayed open for
those two configurations, across every restart, for the life of the directory.

### 5.8 What Phase 7 actually shipped (`5d1dd718b6`)

Landed on `puzpuzpuz_live_view` across four production classes; ~15 lines of
production code and about 50 of javadoc, plus ~150 lines of test. No on-disk
format change and no `SLOT_FORMAT_VERSION` bump: it calls an existing pass from a
second place.

**`reconcile` runs `purgeUncataloguedSegments` over the generation it adopts,**
beside the `purge()` it already ran, so both halves of one sweep decide the fate
of the segments a generation named and of the files it never named - the same
pairing `LiveViewCheckpointTimelineStoreWriter.sweep` makes. The superblock it
needs is the one the reconciler already has open for the purge, and the catalogue
read is one `hasUnregisteredRootSegment` already makes a few lines above.

What the implementation turned up:

- **The two rules separate themselves by ordering, with no flag to carry.** The
  catalogue pass runs before `cleanupOrphans`, and every catalogued id sits below
  the ceiling by construction - a generation's `nextSegmentId` bounds every id it
  ever allocated. So when the pass has run, `cleanupOrphans` finds nothing above
  the ceiling and records a bound equal to it, which makes `purgeFinalOrphans` a
  no-op. No signal has to say whether the catalogue answered: the directory it
  left behind says it.
- **The deferred rule keeps exactly the case the catalogue cannot speak for.** A
  directory with no valid generation has no catalogue to ask, and there the
  ceiling is zero, so every final name is an orphan by definition and the ceiling
  can never decay off one. The deferral there costs one publication rather than a
  restart, and it is what keeps allocation monotonic across the crash that shape
  comes from.
- **The fail-closed arms are narrower than they look.** A catalogue this build
  cannot read never reaches the pass at all: `hasUnregisteredRootSegment` already
  treats an unreadable catalogue as a mismatch and resets the directory. What is
  left is a slot that lost the newest-generation race, where the pass refuses and
  the ceiling rule still records - which is the one case the two genuinely still
  overlap, and the weaker rule is the right one to keep there.
- **The invariant it rests on is the one Phase 6 established, re-checked from the
  reconciler's side.** The newest generation's catalogue holds an entry for every
  segment any valid slot can reach: an entry is retired only after the sweep
  unlinked its file, and the sweep unlinks only once the fallback slot has passed
  the retirement generation. So a file that exists and is reachable is a file the
  catalogue holds - the pending directory segment excepted, which the superblock
  names.

Costs the change adds, stated because they are real:

- One directory listing of `meta/` and `data/` per reconciliation, plus one
  `O(log N)` catalogue probe per file found, and a second open of the directory
  root beside the one `purge()` just used. Both are the costs section 5.7 already
  priced for the cadence sweep, now also paid once per directory at boot.
- A reconciliation removes more files than it used to, so a boot that meets a
  directory full of failed-publication orphans does more `unlink` work before the
  view is available.

Measured on the test workload (one boundary per commit, a 60-second horizon, one
retention publication failed after its metadata publish, the purge cadence
disabled): the two files that publication left survived five further seals - by
which point the durable ceiling had stepped over both - and the reconciliation
that followed removed them, where before the change it left them exactly where
they were.

**What Phase 7 does not change: the `.tmp` and crashed-repair rules.** Both are
still reconciliation's alone, and both are ownership questions rather than
reachability ones - a `.tmp` belongs to whichever writer holds it open, and a
repair descriptor to a repair that may still be running. Reconciliation runs
where no writer can own either, which is where those decisions belong. The
catalogue rule has nothing to say about them.

---

## 6. Verification results (traced 2026-07-30)

The original three semantic questions came back clear: the WAL purge floor is
independent of the oldest boundary, no boundary has distinguished anchor status,
and the below-horizon repair fallback already exists. The later implementation
review found a separate row-position-delta preservation requirement, resolved in
section 6.5 and Phase 3 step 3.

### 6.1 The WAL purge floor is generation-scoped, not boundary-scoped - CLEAR

`LiveViewCheckpointSuperblock.select():527-542` computes
`walPurgeFloor = min(normalizedBaseSeqTxn)` across the **two valid A/B slots**.
`normalizedBaseSeqTxn` is a plain scalar field in the 176-byte slot
(`SLOT_NORMALIZED_BASE_SEQTXN_OFFSET`), supplied by the caller as an `append`
parameter (`LiveViewCheckpointTimelineStoreWriter.java:134`). Nothing derives it
from the timeline tree, so no boundary - oldest or otherwise - feeds it.

`publishTruncate` already demonstrates the safe pattern: it assigns
`generation`, `nextSegmentId`, the byte counters and the two root refs, and
deliberately leaves the watermarks alone, with the comment "The base and
live-view watermarks carry forward unchanged: this publication moves no
coordinate" (`:795-800`). A `publishTruncateBelow` that follows the template
inherits the property. **No new hazard, and no verification burden beyond
mirroring the existing code.**

### 6.2 There is no distinguished anchor boundary - CLEAR

Anchors are per-boundary, not global: each checkpoint root carries its own
`anchorRootRef` (`LiveViewCheckpointTimelineStoreReader.java:515-517`, `:599-607`),
holding the anchor map as of that boundary's timestamp. No entry has special
status.

`truncateAbove`'s javadoc line "the tail roots go, the long-term anchors stay"
(`LiveViewCheckpointTimelineWriter.java:51-57`) is descriptive of *which end that
operation preserves*, not a structural dependency on the oldest entry. Dropping
the low end costs resume reach, not correctness.

### 6.3 The below-the-horizon fallback already exists and is already priced - CLEAR

It is a named disposition, not an error path: `DISPOSITION_BOUNDARY_REBUILD`
(`LiveViewCheckpointRepairPlan.java:288`), documented as "The residual O(view age)
fallback: no sealed anchor sits below the change, the trigger carries no timestamp
to search with, or the apply-ahead range cannot be classified."

The lookup already reports absence rather than raising, on three separate routes
(`LiveViewRefreshJob.TimelineAnchorSource.findAnchorBelow:9128-9166`):

- `predecessorLvRowPosition` returns `LONG_NULL` when the generation holds no
  boundary below the correction (documented at
  `LiveViewCheckpointTimelineStoreReader.java:368-371`), which maps to `false`
- the re-anchoring loop walks strictly below a boundary that no longer covers its
  own timestamp group, and terminates on the same sentinel
- a `catch (Throwable)` logs "live view checkpoint timeline holds no resume
  anchor" and returns `false`

The class javadoc states the intent outright: "A view with no readable timeline -
never sealed, retired by an earlier repair, or corrupt - reports no anchor rather
than raising, so the plan takes the rebuild it would take for a change below every
boundary." A retention horizon produces exactly that condition, which is already
exercised.

### 6.4 The finding that matters: localization is dependency-driven, not checkpoint-driven

This came out of tracing 6.3 and it materially lowers the cost of a horizon.

`LiveViewCheckpointRepairPlan`'s javadoc (`:88-155`) documents three localization
shapes, none of which consults the checkpoint store for its lower bound:

| Shape | Lower bound `L` | Needs a sealed anchor? |
|---|---|---|
| RANGE `W PRECEDING` | `R - W`, key-independent arithmetic | no |
| ROWS `N PRECEDING` | discovered by `RowsBoundSource` over the pinned snapshot | no, but needs a provably insert-only change set and a `FINITE` high bound |
| anchored | the anchor segment walls holding `R` and `changeMaxTs` | no |

Quoting `:89-96`: "The `L`/`R` split is what makes a correction older than every
sealed anchor local. A boundary rebuild has no anchor to restore from, but a
bounded `RANGE W PRECEDING ... CURRENT ROW` view needs none ... **The finite
dependency, rather than checkpoint availability, provides the lower bound.**"

The plan then prices the two dispositions rather than preferring availability
(`:156-168`): it derives the rebuild bounds even when an anchor is available,
prices both intervals through `ScanCostSource` against the same pinned snapshot,
and takes the cheaper. And it names the case where keeping old anchors actively
loses: "the anchor a cadence leaves just below an old correction is exactly the
anchor whose resume replays every row above it."

So the horizon's cost is not "deep corrections get expensive". It is narrower:

- **Views whose every function localizes** - bounded RANGE, anchored with a full
  reset, or ROWS with insert-only plus a finite `H`: a correction below the
  horizon costs **nothing extra**. The dependency supplies the floor.
- **Views carrying one non-localizable function** - a ROWS arm whose change set
  cannot be proven insert-only or whose high bound comes back `EOF`: the union
  sinks to `EOF`, the floors collapse to the `START FROM` boundary `S`, and the
  repair reads the whole view history. This is the only population that pays, and
  it pays only on corrections below the horizon.

That is the input open decision 2 was missing.

*One correction to this table, from writing the Phase 3 tests (section 5.4).* The
population named above was originally "an unbounded-preceding aggregate, or a ROWS
arm whose high bound comes back `EOF`". The first half does not exist: a bare
unbounded window is rejected at `CREATE LIVE VIEW`, which requires an ANCHOR, and
an anchored window localizes off the anchor wall. What does not localize is a ROWS
dependency over a **DEDUP** base - the change set cannot be proven insert-only, so
the discovered bound is unavailable. That is the shape the census in decision 2
should be looking for.

### 6.5 A fifth structure the trace turned up: the row-position delta index

`LiveViewCheckpointRowPositionDeltaWriter`'s only mutation is `suffixAdd`
(`:123`) - no truncate, no prune. It is written on the repair path only
(`LiveViewCheckpointTimelineStoreWriter.java:628-641`) and accounted as
`rowPositionDeltaBytes`, documented as a share of `metadataBytes`
(`LiveViewCheckpointSuperblock.java:154-159`).

It is another append-only metadata B+ tree with no reclamation, repair-driven
rather than seal-driven. Each `suffixAdd` creates one difference breakpoint and
path-copies `O(log R)` pages, rather than rewriting the logical suffix. It is
smaller than the per-seal surfaces but still unbounded on an O3-heavy workload.
Phase 2a counts its pages like the other two direct trees, so a repair's
superseded delta pages are now reclaimed, and Phase 3's `pruneBelow` prunes the
index itself when low boundaries retire.

Note the asymmetry this creates against the `publishTruncate` template: that
method carries `rowPositionDeltaRootRef` forward unchanged because "dropping the
suffix moves no surviving prefix key's cumulative recovery position", and clears
`seedCursorOffset` because "a truncate leaves no mid-sweep resume point behind".
A low-side truncate carries the seed offset forward, but it cannot merely delete
delta entries keyed to dropped boundaries. `effectivePosition` adds the prefix
sum of all earlier differences, so an old breakpoint still contributes to every
surviving suffix key. The prune must fold the discarded prefix sum into the first
surviving key as Phase 3 step 3 specifies.

---

## 7. Crash safety

Metadata reference counts live in the segment-directory spine, which is
published copy-on-write and named by the superblock slot, so ordinary reference
deltas commit atomically with the generation exactly as the data counts already
do. The existing fsync order - data pages, then metadata segments, then the
superblock that names them - remains unchanged.

Phase 2a nevertheless adds one crash-safety case: deferred self-registration of
the selected directory segment. Reconciliation must accept that exactly the
currently selected directory-root segment is absent from its own catalogue; any
other reachable unregistered metadata segment is corruption. The next
publication registers that segment before publishing its successor, and a crash
before the successor superblock commits leaves only final-name orphans above the
valid slots' id ceilings, which the existing orphan sweep removes.

As shipped, the exception is named rather than inferred: the slot carries
`pendingDirectorySegmentId` and bounded slot validation rejects a half-set
triple, so a reconciler can check the rule instead of deducing it from what the
catalogue lacks. Because the field is part of the atomically published slot, a
crash cannot leave the pending registration and the root it describes
disagreeing: either both generations' worth of state commits or neither does.

Phase 2b checks that rule, for the three roots the superblock names directly. A
generation naming a metadata segment its own catalogue does not hold - the
pending directory segment excepted - resets the checkpoint directory and rebuilds
from the base table, which is the disposition a foreign layout already takes and
costs a rebuild rather than correctness. The boundary half of the rule holds by
construction now that a boundary's segments are registered by the publication
that writes them, but it is not checked: proving it needs one partition-map walk
per surviving boundary, which is the sweep the whole accounting exists to avoid.

A segment may be unlinked only after the superblock generation that stopped
referencing it is durable. `PurgeSweep`'s existing
`oldestValidSlotGeneration` gate already expresses this; metadata entries inherit
it unchanged.

Phase 1 is crash-neutral - it only decides whether to write a page or reuse a
ref, both of which are captured in the same atomic publish.

Phase 4 adds no case either, and the reason is worth stating because the removal
looks like it should. An entry retirement commits with the generation carrying
it, and the fallback slot keeps its own copy of the entry - at a zero count, so
no root of that generation reads the missing file, and a sweep over it re-proposes
the retirement rather than faulting. The proposal itself is deliberately not
durable: the sweep re-derives it from what the catalogue and the directory
disagree about, so a crash between the sweep and the publication that would have
applied it costs one more sweep and nothing else.

Phase 5 adds none either, and for the reason that makes it a small change: a
sweep commits nothing. It unlinks files the purge rule proves unreachable and
leaves a proposal in memory, so a crash mid-sweep leaves a directory the next
sweep re-derives the same answer over, and a crash between the sweep and the seal
that would have applied its proposal costs one more sweep. The rule the sweep
applies is generation-scoped, not sweep-scoped, so running it more often cannot
make it collect anything it would not have collected once.

Phase 6 adds no case, and the reason is worth stating because it removes one
rather than adding it. The id-ceiling rule needs the deferral - a final-name file
above the ceiling goes only after a new slot commits above it, so a crash between
the decision and the removal leaves the file for the next reconciliation to
decide on again. The catalogue rule needs no deferral, because a file the newest
durable generation does not catalogue is unreachable from every root of that
generation whatever happens next, and the monotonic `nextSegmentId` preserves
allocation order without help: unlinking never lowers it, and the id skip only
ever moves forward, so no id comes back into circulation carrying a meaning some
root remembers. A crash mid-pass therefore leaves a directory the next sweep
re-derives the same answer over. What the pass will not do is act on a partial
answer: a catalogue it cannot read, a superblock naming no directory root, or a
slot that lost the newest-generation race all leave every file where it is.

Phase 7 adds no case for the same reason, from a caller that already had it. A
reconciliation publishes no generation either, so the pass commits nothing there
that it does not commit on a sweep, and a crash mid-reconciliation leaves a
directory the next reconciliation re-derives the same answer over. The one
question it raises that a sweep does not - whether an adopted generation is
enough evidence to unlink under - is answered by the invariant the catalogue
already maintains: the newest generation's catalogue holds an entry for every
segment any valid slot can reach, because an entry retires only after the purge
rule proved the fallback slot had passed the retirement generation.

Phase 3 adds no case of its own. A retention pass is one A/B swap over an
unchanged fsync order, so a crash before it commits leaves the pre-retention
generation whole and the segments it wrote as final-name orphans above the valid
slots' id ceilings, which the existing orphan sweep removes. It differs from the
high-side truncate in needing no repair marker beside it: that publication
deliberately leaves the superblock naming a head it discarded, while a retention
pass moves no coordinate at all - the head, both watermarks, the checkpoint id
counter and the seed resume point all carry forward - so there is no window in
which the committed generation describes something the runtime cannot restore.

---

## 8. Test plan

The review recorded that no test asserts any bound on `meta/` file count. That is
the headline gap, and the correction it also recorded matters:
`LiveViewCheckpointCompactionTest.purgeCycle` calls `reconcile` directly four
times, but that is not why nothing catches this - `purge()` is a data-segment
sweep by construction and would never delete metadata however often it ran.

New tests, per phase:

**Phase 1** - all landed in `LiveViewCheckpointStatePageElisionTest`

- Seal N times over a static key set with a trickle of ingest into one key;
  assert `data/` and `meta/` byte growth is proportional to touched keys, not to
  `N x live_keys`. Red before the change. **Done**, stated structurally rather
  than as a byte threshold: exactly one map entry may change per seal, the run
  writes `live_keys + seals` distinct pages, and `data/` grows by exactly the
  touched key's pages. The metadata half follows from the entry being
  byte-identical, which is what the partition-map writer's own tested elision
  keys on.
- Byte-identical state must still round-trip: freeze, elide, restore, and compare
  against a from-scratch recompute. **Done** - every case asserts the from-base
  recompute at a zero refresh-fault count, and the restart case asserts the
  restore actually came off the timeline.
- Cover a published previous page in several old data segments, exercising the
  bounded reader cache and its catalogue length checks. **Done** - the restart
  case asserts the head boundary names two distinct segments, the cold keys'
  original and the hot key's newest.
- Cover the map-null scalar path and two boundaries captured into the same
  unpublished repair segment (or assert the documented conservative fallback).
  **Partly done** - the repair capture is covered directly (one page shared
  across the boundaries one capture re-versions). The scalar path is not covered
  positively and cannot be through SQL: see section 5.1.
- A ring function must be unaffected (control). **Done** - a RANGE view over the
  same workload seals, restarts and matches its recompute, and passes with and
  without the change.

**Phase 2a** - all landed in `LiveViewCheckpointMetadataReclamationTest` except
the format case, which went to `LiveViewCheckpointLifecycleTest`

- Seal N times; assert the `meta/` file count stays bounded. This is the test the
  review asked for. **Done**, stated as two measurement points rather than an
  absolute: 32 seals against 8, with the live metadata segment count allowed to
  move by at most 2 (see section 5.2 on why it is not flat) and `meta/` growth
  required to be under four files per seal. Red before the change: the sweep
  reclaimed nothing.
- Reference-count invariant. **Partly done, and this is the gap.** What the test
  asserts is the consequence rather than the invariant: every catalogued entry
  the selected generation references has its file, the unregistered directory
  segment survives, and a restart recomputes the view from base at a zero fault
  count - so a count that retired a segment one page early fails as a bad restore.
  What it does not do is compute the expected count independently by walking the
  three trees' pages, which would need a page-level walker no reader exposes.
  There is a cheaper substitute in place: an assertion inside the directory
  writer holds the release pre-pass against what the path copy visited.
- Directory self-registration executes one publication late. **Done** - the
  pending segment is absent from the catalogue it carries, present in the next
  one, and retired at that generation because the same publication replaced its
  root. The zero-count variant the original plan asked for is unreachable: a
  publication that stages the pending registration always writes a segment.
- Crash injection, then reconcile, then assert no live segment was unlinked.
  **Done** for `TEST_FAIL_AFTER_METADATA_PUBLISH`, which is the stage that leaves
  orphan metadata segments; the other two stages are covered by
  `LiveViewCheckpointTimelineSealTest`, which now checks the catalogue by kind.
- An older-format `_timeline` retires and rebuilds rather than erroring. **Done**
  in `LiveViewCheckpointLifecycleTest.testSupersededTimelineVersionResets...`,
  which stamps the *previous* magic and version rather than a later one, so it
  covers the migration direction this bump actually creates.
- A repair reclaims the timeline and delta pages it supersedes. **Done** - an O3
  correction over a 12-seal history, then a purge, then a restart against the
  recompute.

**Phase 2b** - all landed in `LiveViewCheckpointMetadataReclamationTest`

- An unchanged anchor map must reuse old metadata pages across boundaries without
  either leaking them forever or allowing purge to unlink them early. **Done** -
  an anchored view over eight seals leaves boundary segments several roots name,
  and every one of them survives the sweep. The "not forever" half is now
  assertable through Phase 3's horizon, and
  `LiveViewCheckpointRetentionHorizonTest` states it as the boundary count and the
  file counts both staying flat while the view keeps sealing.
- Repair-splice and `publishTruncate` orphans get reclaimed (the two Class A
  entries with `O(live_keys)` volume). **Done**, one case each, told apart by the
  property that distinguishes the two publications from the outside: a splice
  preserves every logical key and a truncate drops a suffix of them. Both go red
  when the sweep is made to skip boundary segments.
- Reference-count invariant computed independently, which is worth building the
  page-level walker for once boundary closures are counted too. **Not done, and
  closed as an accepted gap on 2026-07-31.** What the cases assert is the two
  consequences - some superseded segment went, and no segment a surviving root
  names went - plus the restart and the from-base recompute. Computing the
  expected count instead needs
  a page-level walk of every boundary's partition maps, which no reader exposes;
  the top-level roots are reachable through public APIs but the map pages below
  them are not, and those are exactly where the cross-boundary sharing lives.
  The judgement is that the in-writer assertion holding the release pre-pass
  against the path copy, plus a restore that recomputes at a zero fault count,
  covers enough of it to not be worth a test-only walker. Reopen it if a
  reference-count bug ever reaches a branch, since this is the assertion that
  would have named it directly.

**Phase 3** - landed in `LiveViewCheckpointRetentionHorizonTest`, plus tree-level
cases in `LiveViewCheckpointTimelineTest` / `LiveViewCheckpointRowPositionDeltaTest`
and one arm in `LiveViewFuzzTest`

- Boundaries below the horizon retire; `data/` and `meta/` both shrink; the view
  still restores from the newest boundary after restart. **Done**, stated as
  flatness rather than as a shrink: the boundary count is measured at 10 seals and
  again at 30 and must be *identical*, and `meta/` and `data/` may each grow by at
  most a quarter of a file per further seal against the four a seal writes. The
  residual is the catalogue's own tree, which retired no entry when this case was
  written; Phase 4 makes it retire them, and the bound the case states is loose
  enough to hold either way.
- An O3 correction below the horizon takes `DISPOSITION_BOUNDARY_REBUILD` and
  lands on the same answer as a from-scratch recompute. **Done, and the case had
  to change shape to be honest.** A bare unbounded window is rejected at CREATE
  (live views require an ANCHOR), and a `ROWS N PRECEDING` view over an ordinary
  base localizes below the horizon - it reports `BOUNDARY_REBUILD` with
  `DENIAL_NONE`, which is a *localized* rebuild and exactly section 6.4's point.
  The genuinely denied case needed a DEDUP base, which is what makes a change set
  unprovable as insert-only and leaves the ROWS dependency with no floor. That is
  the population that pays, and the case asserts a non-`NONE` denial rather than
  the disposition alone.
- The paired control that makes the above meaningful: a **localizable** view
  (bounded RANGE) takes a dependency-localized repair below the horizon and does
  *not* read the whole history, per section 6.4. **Done** - same correction depth,
  `DENIAL_NONE`, against a view with nothing sealed under the change.
- The WAL purge floor does not move when the horizon does - a regression guard on
  the carry-forward in step 2, since section 6.1 established the floor is
  generation-scoped. **Done**, against a retention pass driven directly so no seal
  moves a watermark beside it.
- The row-position delta index shrinks when boundaries retire, and an
  `effectivePosition` lookup on every surviving boundary returns the same value
  it returned before the prune. Include multiple discarded breakpoints, an
  existing breakpoint at the first survivor, and a first survivor with no prior
  breakpoint, so prefix folding is non-vacuous. **Done** at the tree level, where
  the invariant can be stated directly: each case snapshots `prefixSum` at every
  key at or above the floor, prunes, and requires each to be unchanged. All three
  shapes are covered, plus the tree-goes-empty case and randomized rounds.
- `metadataBytes`, `dataBytes` and `rowPositionDeltaBytes` remain cumulative
  across reclamation; live/obsolete catalogue metrics and actual directory bytes
  reflect the shrink instead. **Done** for the cumulative half and for
  `logicalStateBytes`, which is the counter that *does* shed retired boundaries.
- `seedCursorOffset` survives a low-side truncate taken mid-sweep, and the sweep
  resumes from it. **Partly done.** The carry-forward is asserted directly, by
  publishing a generation carrying a resume point and reading it back after the
  retention pass. That a *sweep* then resumes from it is not covered: driving a
  seed sweep to a mid-sweep seal and a retention pass in the same case is a
  fixture this suite does not have.
- A fuzz arm: randomized ingest plus O3 plus restart with a short horizon,
  cross-checked against the SQL recompute oracle, following the existing
  `LiveViewFuzzTest` shape. **Done** -
  `LiveViewFuzzTest.testFuzzCheckpointRetentionHorizon`, one boundary per row
  against a horizon of at most five seconds of event time, so most boundaries
  retire while the fuzz is still ingesting.

**Phase 4** - four cases in `LiveViewCheckpointSegmentDirectoryTest` and one in
`LiveViewCheckpointMetadataReclamationTest`

- The catalogue stops growing with the view's age: a sweep leaves entries naming
  unlinked files and the seal that follows a reconciliation removes them.
  **Done** -
  `LiveViewCheckpointMetadataReclamationTest.testTheCatalogueRetiresTheEntriesOfSweptSegments`,
  stated as an identity rather than as a bound: it names the exact set the sweep
  left dead, requires every one of them to be gone afterwards, requires the
  catalogue to have shrunk, and requires every survivor to name a file. Red
  before the change, on the first of those.
- Retiring an entry prunes the node it emptied rather than blanking it. **Done**
  at the tree level, measured through the only thing that can tell the two apart
  from outside: the pages a later append path-copies. Retiring 27 of 31 entries
  at node capacity three makes the next append cost strictly less than it did
  against the full tree, which a name-only removal could not.
- The two refusals, and the no-op. **Done** - a still-referenced entry, an entry
  the same publication registers, an entry something then tries to reference, and
  an id the catalogue no longer holds, which must be silently accepted because
  the sweep re-proposes it.
- Randomized inserts, re-references, releases and retirements in one pass against
  a `TreeMap` oracle, at node capacity three so a retirement empties leaves a
  neighbouring insert just split into. **Done**, 120 rounds, checked through a
  freshly bound reader per round so a corrupted reused subtree cannot survive to
  the end of the walk.
- The tree goes empty. **Done** - the publication writes no page, leaves no
  segment file behind, publishes a null root, and the next publication builds a
  fresh catalogue over it. Unreachable from a real publication (section 5.5), so
  the case drives the writer directly.

**Phase 5** - two cases in `LiveViewCheckpointMetadataReclamationTest`, plus one
column-value update in `LiveViewSmokeTest`

- Segments are unlinked and their entries retired within one process, with no
  restart and no explicit reconciliation. **Done** -
  `testTheCadenceSweepCollectsWithoutARestart`, stated as three things the
  reconcile-only build cannot produce: at least one file present at seal 8 is
  gone by seal 32, the catalogue ends with at most a quarter of a seal's worth of
  zero-reference segments whose files are still there (measured: 1, against 59),
  and the entries the last sweep proposed are all gone after one further seal.
  Red before the change on the first of those.
- The paired control that gives it meaning: at interval zero nothing is unlinked
  and one reconciliation then finds the whole queue. **Done** -
  `testTheCadenceSweepStaysOffAtIntervalZero`, which also fixes the reconcile-only
  numbers the case above is measured against.
- The two cases that measure the reconciliation sweep itself pin the interval to
  zero. **Done**, and it is the honest disposition rather than a workaround: they
  exist to cover a configuration that is still supported, and with both sweeps
  running there is nothing left for either to find.
- `live_views()` reports the collection columns from the first seal rather than
  from the first restart. **Done** in
  `LiveViewSmokeTest.testLiveViewsCatalogueExposesCheckpointTimelineColumns`,
  which asserted the pre-change NULLs directly.

**Phase 6** - two cases in `LiveViewCheckpointMetadataReclamationTest`

- The files a failed publication renamed into place are unlinked within the
  process, with no restart and no reconciliation. **Done** -
  `testTheCadenceSweepCollectsTheOrphansOfAFailedPublication`, which fails a
  retention publication after its metadata publish, names the exact set it left
  uncatalogued, seals past it, and requires every one of them to be gone and
  nothing uncatalogued to remain. Red before the change, on that set surviving.
- The paired control, and the one that states what was broken rather than merely
  late: at interval zero those same files survive the seals that follow, because
  the durable `nextSegmentId` ceiling has moved past every one of them. **Done** -
  `testTheIdCeilingRuleCannotReachTheOrphansOfAFailedPublication`, which asserts
  the ceiling has stepped over each id before asserting the file is still there,
  so a run where the seals happened not to step over them fails as
  inconclusive rather than passing. *Phase 7 changed its tail and its name: the
  full reconciliation it ends with now collects them, so the case is
  `testAReconciliationCollectsTheOrphansTheIdCeilingRuleCannotReach` and states
  both halves - which rule cannot name them, and which one does.*
- A failure shape only a non-seal publication produces. **Done** through a new
  `TEST_FAIL_AFTER_RETENTION_METADATA_PUBLISH` stage: every existing injection
  fires in a seal, and a failed seal re-arms the reconciliation, which is exactly
  the case that was never broken.
- Not covered: the same shape from a failed compaction or a failed repair. Both
  reach the pass through the identical rule - the catalogue does not hold the id -
  and neither has an injection point that leaves the seal intact, so the retention
  case stands for all three. Nor does anything assert the fail-closed arms
  directly; they are stated in code and exercised only where a catalogue read
  happens to succeed.

**Phase 7** - one reshaped case in `LiveViewCheckpointMetadataReclamationTest`
and two in `LiveViewCheckpointLifecycleTest`

- A reconciliation collects what the id ceiling can no longer name. **Done** as
  the tail of `testAReconciliationCollectsTheOrphansTheIdCeilingRuleCannotReach`
  above, which is red before the change on exactly that step: it still proves the
  seals stepped the ceiling over every orphan first, so the collection cannot be
  the old rule firing.
- Which files a reconciliation keeps, and why each one. **Done** -
  `testOrphanCleanupCollectsWhatNoGenerationCatalogues`, which needed a fixture
  that publishes a *real* catalogue over two generations rather than the empty
  leaf the suite used: a referenced data entry survives, so does the previous
  generation's directory segment at a zero count against a retirement the
  fallback slot has not reached, and so does the pending directory segment the
  superblock names. Four uncatalogued final names and two temporaries go. Red
  before the change, on the removal count.
- The deferral, in the one place it still lives. **Done** -
  `testOrphanCleanupDefersWhereNoGenerationCanAnswer`, a directory with no
  `_timeline` at all: the ceiling is zero, the bound is recorded, nothing final is
  removed until a publication allocates above it, and the publication's own
  segment - which sits above the bound, as a seal's does - survives the sweep that
  follows. Passes before the change too, which is the point: that path is
  untouched.
- Not covered: the one arm where both rules still apply, a slot that lost the
  newest-generation race. The catalogue pass refuses there and the ceiling rule
  records as before; the refusal is stated in code and reached only through a
  bounded-validation failure this suite has no fixture for.

Not covered, and worth naming: nothing asserts the *purge* half end to end for a
retention pass under a concurrent reader pin - the generation gates are shared
with the other publications and are covered there, but a case that pins a
generation across a retention pass and proves nothing it reaches was unlinked
would be the direct statement. Nor does anything drive a sweep concurrently with
the seal that consumes its proposal, because nothing can: both run on the refresh
worker, under the same serialization the reconciliation sweep always had.

---

## 9. Rough sizing

| Phase | Scope | Notes |
|---|---|---|
| 1 | **landed**: 2 production classes, ~300 lines, plus a 560-line test | estimate held; no format change; both the published and the in-flight previous page are covered |
| 2a | **landed**: 11 production classes, ~700 lines, plus a 400-line test | came in well under the "larger than 600 lines" estimate, because per-segment page counts removed the closure summaries that were supposed to dominate it; format cost is one leaf field and three superblock longs |
| 2b | **landed**: 10 production classes, ~250 lines, plus ~200 lines of test | came in under the estimate because the release sites needed no code: once a root states its closure, `publishTruncate` and the repair splice reclaim it through the reference transaction they already ran. Format cost is one field on the anchor root and a third value for the catalogue's kind |
| 3 | **landed**: 9 production classes, ~450 lines, plus ~900 lines of test | came in under the estimate for the same reason 2b did: retiring a boundary is the reference transaction `publishTruncate` already ran. Format cost is one superblock field, needed to keep the published entry count honest rather than by the plan's design |
| 4 | **landed**: 6 production classes, ~215 lines, plus ~370 lines of test | never estimated - it was decision 7 rather than a phase. Small because a B+ tree whose only deletion pattern is "the low ids die first" gets a correct shape out of the existing emit path, so there is no rebalancing rule and no format cost at all |
| 5 | **landed**: 8 production classes, ~290 lines, plus ~190 lines of test | never estimated either - it was the leftover Phase 4 recorded. The reclamation logic is about forty lines; the rest is javadoc, a result class and the five files a new config key touches. No format cost, and no new rule: the purge rule is indifferent to how often it runs |
| 6 | **landed**: 4 production classes, ~145 lines, plus ~165 lines of test | never estimated either - it was the leftover Phase 5 recorded. The pass is about eighty lines, and it is small because the catalogue already states what it needs: the reachability rule was written by Phase 2a/2b, and this only reads it from the other side. No format cost, and no new state - it decides and acts in one pass |
| 7 | **landed**: 4 production classes, ~15 lines of code and ~50 of javadoc, plus ~150 lines of test | never estimated either - it was the leftover Phase 6 recorded. The smallest phase by a wide margin: the pass, the superblock it needs and the catalogue read all existed, and the two rules separate themselves by ordering rather than by a flag. Most of the work is in the test fixture, which had to learn to publish a real catalogue |

---

## 10. Rejected alternatives

- **Metadata compaction as the primary fix** (one of the review's two options).
  It could walk and repack the complete live metadata graph, then drop unreachable
  old segments; it does not technically require a retention floor. Rejected as
  the primary mechanism because without Phase 3 every boundary is live, so each
  pass is proportional to the ever-growing retained graph and closes only Class
  A, not Class B. It remains a reasonable *addition* after Phase 3, to defragment
  a bounded horizon window that has gone sparse.
- **Enabling the existing compaction by default.** Does not help:
  `LiveViewCheckpointCompaction` repacks live state pages into a fresh **data**
  segment, touches metadata only via an `exists()` probe at `:240`, and
  `publishCompaction` then *writes* new root, timeline and directory segments and
  adds to `metadataBytes`. Setting the interval non-zero does not bound `meta/`
  at all.
- **Self-contained metadata segments** (no cross-segment node sharing, so
  reclamation is a pure generation comparison). Rejected: it turns every seal's
  `O(log N)` path copy into a full spine rewrite, which is the write amplification
  Phase 1 exists to remove.
- **Copy-forward of nodes older than N segments at seal time.** Attractive
  because it needs no format change, but it does not close: a cold partition key
  is never touched, so its leaf stays in an old segment forever and a full sweep
  is still required.
- **Dirty tracking on the `WindowFunction` interface** as the way to get Phase 1.
  Rejected for now in favour of the byte comparison, which stays inside the
  checkpoint storage layer rather than changing every window implementation.
  Worth revisiting as an optimisation once Phase 1 proves the shape.
- **Persisted transitive closure summaries for the three direct trees** (steps
  2-4 of the original Phase 2, superseded by what 2a shipped). A timeline root's
  closure holds one entry per distinct segment its live pages sit in, which is
  `O(N / leafCapacity)` and grows with the timeline, so writing the summary every
  seal makes the metadata cost quadratic in the seal count - reintroducing, in a
  smaller constant, the growth Phase 1 exists to remove. Per-segment page counts
  give the same answer incrementally and need no format change. The summary shape
  is *not* rejected for boundary metadata, where the closure is bounded by the key
  set and bulk release is the operation that matters - and that is what Phase 2b
  took, for the soundness reason section 5's Phase 2b preamble states rather than
  for the cost.
- **Mark-and-sweep over the reachable metadata graph** instead of counting.
  Correct and needs no accounting at all, but the live graph is every retained
  boundary's partition maps - `O(seals x live_keys)` pages - so a sweep is
  proportional to the whole history. It is the same objection as metadata
  compaction, one level up.

---

## 11. Open decisions for a human

**Settled on 2026-07-31, and it changes what this plan delivers.** The brief was
metadata GC - reclaiming the garbage in `_checkpoints/meta/` - and the plan
widened it in section 1 to bound retained state as well, because Class A alone
leaves the store growing. That widening is withdrawn: **the retention horizon is
not shipping.** The deliverable is Class A, which Phases 2a, 2b, 4, 5, 6 and 7
close and which is on by default. Task 1 in section 12 removes Phase 3, and
decisions 1 and 2 below go with it. Section 1's correction to the scope stands as
a record of why the horizon was proposed, not as something this plan does.

1. ~~**Horizon policy.**~~ **Withdrawn** with Phase 3. Phase 3 took an
   event-time window, `cairo.live.view.checkpoint.retention.micros`, because the
   floor it implies is one `O(log N)` probe while a boundary count would need
   navigation the timeline reader does not expose (section 5.4). The end state
   this decision named - deriving that window from the view's TTL once TTL lands,
   so the checkpoint horizon never sits below data the view still retains - has no
   horizon to apply to once task 1 lands. It comes back only if checkpoint
   retention is proposed again as its own change.
2. ~~**Default horizon.**~~ **Withdrawn** with Phase 3, and it was the only
   thing between the mechanism and a bounded default - so with the mechanism gone,
   a default install's *retained* checkpoint state is unbounded and nothing in
   this plan bounds it. That is the accepted outcome of the scope decision above,
   and task 3 requires the PR body to say it plainly rather than leave it implied.
   The analysis below is kept because it is what any future retention proposal
   starts from. It shipped at zero, which disables retention, so a default install
   grew exactly as it did before Phase 3. Section 6.4 narrows the question: a
   view whose every window function localizes pays nothing for a short horizon,
   because the dependency rather than the checkpoint supplies the repair floor.
   Only a view carrying a non-localizable function pays, and only on corrections
   below the horizon, where it reads the whole view history. Phase 3's tests
   sharpened what that population is: a bare unbounded window cannot be created at
   all (live views require an ANCHOR), and a `ROWS N PRECEDING` view over an
   ordinary base still localizes below the horizon. What did not localize was the
   same view over a **DEDUP** base, where the change set cannot be proven
   insert-only. So the census is narrower than "unbounded-preceding aggregates":
   it is closer to "views over a DEDUP base, or with a ROWS arm whose high bound
   comes back `EOF`". It has still not been done.
3. ~~**Whether Phase 1 ships inside #6939.**~~ **Settled:** it did, as
   `a2712f7217` on `puzpuzpuz_live_view`, and Phases 2a, 2b and 3 followed it on
   the same branch. None of them needed data-page access outside the checkpoint
   storage layer.
4. **What the PR body says.** *Now tracked as task 3, whose two clauses are the
   conclusion; what follows is how the wording got there.* The old line -
   "Live-view disk growth is unbounded
   in V1; operators size retention upstream" - was wrong on the second clause: no
   upstream retention setting bounds `_checkpoints/`. Phase 3 made the first
   clause conditional rather than false, and removing Phase 3 makes it flatly true
   again for retained state: with no horizon there is no live-view config key that
   bounds the checkpoint store. What did *not* revert with it is everything below,
   which is why the honest line is two clauses rather than one - the garbage half
   is bounded and on by default, and the retained half is not bounded at all.
   Phase 4 closed the residual that used to
   qualify even that - the segment catalogue's own tree - so nothing is left that
   grows with the view's *age* as opposed to with what it holds.
   Phase 5 closed the last qualifier after that: unlinking used to wait for a
   restart, so a long-running process held every superseded segment whether a
   horizon was set or not, and now it collects on a seal cadence
   (`cairo.live.view.checkpoint.purge.interval`, default one, zero to disable).
   Phase 6 closed the term that was not a qualifier at all: the files a failed
   retention, compaction or repair publication renamed into place were never
   collected, in this process or any later one, because the rule naming them
   compares against a ceiling the next seal moves past. The same cadence sweep
   now removes them under the catalogue's own reachability rule, and Phase 7 gave
   reconciliation the same rule, so the sentence no longer needs the qualifier
   "with the cadence on": a process that never sweeps collects them at its next
   restart. Nothing in `_checkpoints/` is left that a running process cannot
   reclaim, and nothing that a restart cannot either; what a restart still owns
   alone is the `.tmp` and crashed-repair rules, which are ownership questions
   rather than reachability ones.
6. ~~**Which mechanism Phase 2b uses** for boundary closures.~~ **Settled:**
   persisted summaries, and not on the cost trade this document framed it on. The
   page-count-plus-diff variant needs page reachability to be a contiguous
   interval in boundary order, and a repair breaks that - it builds each
   re-versioned boundary from its own old root rather than from the one below it,
   so a diff against the two surviving neighbours can release a page a live root
   still names. The Phase 2b preamble in section 5 carries the argument.
7. ~~**Whether the catalogue itself should retire entries.**~~ **Settled: it
   should, and it does** - Phase 4, `a8bc16da33`, section 5.5. The answer was the
   one the decision stated: the sweep proves an entry dead by unlinking its file,
   and since it publishes no generation of its own, it hands the ids to the next
   cadence seal, which removes them in the same path copy it was already making.
   What the implementation added to that was the failure discipline - the sweep
   re-proposes every entry whose file is already gone, so the hand-off needs no
   durability - and the observation that no rebalancing rule is needed, because a
   node that empties simply writes no page and its parent keeps no reference to
   it. ~~What is left over is *when* the sweep runs: only at reconciliation, so
   entry retirement is bounded the same way segment unlinking already is. Making
   the sweep periodic would move both, and is the natural follow-up.~~ **Also
   settled:** Phase 5, `ec6493a268`, section 5.6. It did move both at once, and
   needed no new machinery to - the hand-off Phase 4 built for reconciliation is
   the one a cadence sweep uses, and the purge rule that decides what may go is
   generation-scoped rather than sweep-scoped, so calling it more often collects
   sooner and nothing else.
5. **The narrowed key set a repair leaves behind** (turned up while testing Phase
   1, section 5.1). A localized repair re-versions a boundary from a replay of
   `[L, H)` only, so the boundary it publishes images the keys that range
   carried rather than the key set the boundary originally described. Whether a
   restore from such a boundary is meant to come back with fewer keys is a
   question for the repair design, not for this plan. **Tracked as task 4**, which
   is where the concrete steps are; it stays listed here because the disposition -
   bug, or intended semantics that want documenting - is still a human's call and
   the task's first step is to establish which.

---

## 12. Follow-up tasks

Open work, in the order it should be taken. Tasks 1 and 2 come from the scope
decision at the head of section 11 and land together; tasks 3 and 4 are
independent of each other.

### Task 1 - remove the retention horizon (Phase 3)

**Why:** the deliverable is metadata GC. Retention is a policy mechanism that
decides to stop keeping live boundaries, which is a different feature, and it
ships disabled - so removing it costs a default install nothing and takes ~450
lines of production code, a config key and a public surface out of #6939.

`d7bf14f612` is the commit, but do not expect `git revert` to apply: Phases 4, 5,
6 and 7 all landed on top of it and two of them reference it.

**Remove:**

- `LiveViewCheckpointTimelineWriter.truncateBelow` and its recursion, result pool
  and javadoc (~161 lines; the low-side mirror of `truncateAbove`, which stays).
- `LiveViewCheckpointRowPositionDeltaWriter`'s low-side prune (~221 lines) and the
  `LiveViewCheckpointRowPositionDeltaNode` / `LiveViewCheckpointTimelineNode`
  helpers Phase 3 added for it (~73 lines across the two).
- `LiveViewCheckpointTimelineStoreWriter.publishTruncateBelow` and its
  `RetentionResult`, less the `retiredCheckpointCount` maintenance - see below.
- `LiveViewRefreshJob.maybeTrimCheckpointTimeline` and its call in the
  `if (sealed)` maintenance block, where retention currently runs ahead of
  compaction and the sweep. The comment there explaining that ordering needs
  rewriting for the two passes that remain.
- The config key end to end: `PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_RETENTION_MICROS`,
  the `PropServerConfiguration` read, `CairoConfiguration.getLiveViewCheckpointRetentionMicros`,
  the wrapper and default implementations, the `server.conf` template entry and
  the `ServerMainTest` line that counts it.
- `LiveViewCheckpointRetentionHorizonTest` (627 lines), the retention cases in
  `LiveViewCheckpointTimelineTest` (268) and `LiveViewCheckpointRowPositionDeltaTest`
  (174), and `LiveViewFuzzTest.testFuzzCheckpointRetentionHorizon` (18).

**Keep, and this is the one piece that looks removable and is not:**
`LiveViewCheckpointSuperblock.retiredCheckpointCount` and the
`SLOT_FORMAT_VERSION` bump that carries it. Section 5.4 records that the counter
was **already wrong before Phase 3** - `publishTruncate` has always dropped
boundaries without adjusting `nextCheckpointId`, so `checkpoint_timeline_entries`
over-reported after a high-side truncate - and the high-side truncate maintains
the field too (`LiveViewCheckpointTimelineStoreWriter.java:893`, against the
retention site at `:1081`). Removing the field would reintroduce that bug. Live
views are unreleased, so leaving the format version where it is costs nothing and
avoids a second bump.

**Acceptance:** no `retention` symbol left in `core/src/main` outside the WAL and
base-table senses of the word; `checkpoint_timeline_entries` still correct across
a high-side truncate; the `io.questdb.test.cairo.lv` package green.

### Task 2 - re-point Phase 6 and 7's non-seal failure injection

**Blocks task 1 from being complete.** Do not let it be dropped silently, because
it is a regression guard on the GC work that is shipping.

Phase 6 exists for the files a publication renames into place and then fails to
commit. Proving that needs a failure in a publication that is **not** a seal: a
failed seal re-arms the reconciliation that reads the id ceiling, which is
exactly the case that was never broken. The only such injection point today is
`TEST_FAIL_AFTER_RETENTION_METADATA_PUBLISH`
(`LiveViewCheckpointTimelineStoreWriter.java:94`, fired at `:1068`), and it fires
inside the retention publication task 1 removes.

Two cases depend on it -
`LiveViewCheckpointMetadataReclamationTest.testTheCadenceSweepCollectsTheOrphansOfAFailedPublication`
and `testAReconciliationCollectsTheOrphansTheIdCeilingRuleCannotReach` - and both
are red-before/green-after guards, one for Phase 6 and one for Phase 7.

**Do:** add the equivalent stage to `publishCompaction` and point both cases at
it. Compaction is the natural replacement - it is one of the three publications
section 5.7 names as producing this shape, it re-arms no reconciliation either,
and `cairo.live.view.checkpoint.compaction.interval` already drives it from a
test. Section 8's Phase 6 entry currently records "the same shape from a failed
compaction" as *not covered*; this converts that line rather than adding to it.

**Acceptance:** both cases still fail against a build with Phase 6's pass disabled
and pass with it, and section 8's Phase 6 and 7 entries are updated to name the
new stage.

### Task 3 - rewrite the #6939 PR body

Decision 4 below carries the wording history. Task 1 makes the current draft wrong
again: with retention removed there is no live-view config key that bounds the
checkpoint store, so the accurate statement is two clauses rather than one.

- **Garbage is collected, by default.** Superseded metadata and data segments, the
  catalogue entries naming them, and the files a failed publication left are
  reclaimed on a seal cadence (`cairo.live.view.checkpoint.purge.interval`,
  default one, zero to disable) and again at every reconciliation. Nothing in
  `_checkpoints/` that a generation cannot reach survives a running process or a
  restart.
- **Retained state is not bounded.** Every sealed boundary stays reachable, so the
  store grows with the view's history. Phase 1's elision makes that proportional
  to the keys that actually change per seal rather than to the whole key set, which
  is a large constant but not a bound. State it plainly; do not imply an upstream
  retention setting sizes it, which is the error the original line made.

### Task 4 - the narrowed key set a localized repair leaves behind

This is decision 5, tracked as work rather than as a question, per the scope
discussion on 2026-07-31. It is **pre-existing repair behaviour, unrelated to
reclamation** - Phase 1 only made it visible - so it neither blocks nor is blocked
by tasks 1 to 3, and it may well belong outside #6939 entirely. Establishing that
is step 3.

**The observation** (section 5.1, found while writing the Phase 1 capture test):
`RepairCapture.capture` (`LiveViewCheckpointTimelineStoreWriter.java:2678`)
freezes the runtime's window state as the new root version of each boundary the
replay crosses. A localized repair replays `[L, H)` over runtime state the scratch
overlay has taken out of the way, so the state it freezes covers the keys that
range carried, not the key set the boundary originally described. A restore from
such a re-versioned boundary therefore comes back with fewer keys than the
boundary it replaced.

**Steps:**

1. Reproduce it directly, at the level the claim is made: seal a boundary over a
   wide key set, drive a localized repair whose `[L, H)` touches one key, and
   compare the restored key set against the one the original boundary described.
   The Phase 1 capture case in `LiveViewCheckpointStatePageElisionTest` is the
   nearest existing fixture, but it asserts page sharing rather than key coverage.
2. Decide which it is. Either the replay is meant to carry the untouched keys
   forward - in which case a re-versioned boundary is losing state and this is a
   correctness bug - or a repair is meant to re-version only what it replayed, in
   which case the restore contract needs to say so and the callers that fall back
   to a re-versioned boundary need to tolerate a narrower key set.
3. Route it. If it is a bug, size the fix against `capture` and the scratch
   overlay and decide whether it lands in #6939 or its own PR. If it is intended,
   the deliverable is javadoc on `RepairCapture.capture` and on the restore path,
   plus a test that pins the narrowing so it cannot change silently.

**What is already known, and bounds the urgency:** nothing in this plan reshapes a
boundary. Phase 3 would have shrunk the set a restore could fall back to - fewer
intact neighbours under a narrowed boundary - but task 1 removes it, so the
population that meets a narrowed boundary is whatever it was before this work
started.
