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

package io.questdb.cairo.lv;

import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.DirectIntIntHashMap;
import io.questdb.std.DirectIntList;
import io.questdb.std.DirectLongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

/**
 * One out-of-order change set decomposed into the anchor segments it actually touches.
 * <p>
 * A repair's cost is paid per <b>replacement range</b>, not per correction: the apply
 * merges rather than appends every live-view partition the range covers and writes each
 * of those partitions whole. Today's repair takes one union range running from the anchor
 * below the lowest correction to the frontier, so a commit carrying rows at the head and
 * rows a month back rewrites a month of output for the sake of a few thousand rows. The
 * union is an artefact of how the change set is measured - a scalar minimum and a scalar
 * maximum - rather than of what it holds: a deep commit reaches the head and one old
 * segment, and nothing in between.
 * <p>
 * Under a pure fixed-anchor plan the anchor resets every stateful function at the segment
 * boundary, so a row in one segment cannot influence the output of any other. That is what
 * makes the decomposition usable: each closed segment below the runtime's own segment can
 * be repaired and published on its own, over its own range, and the segments between them
 * are left untouched. {@link LiveViewSegmentRepairEnvelope#segmentScopeGate} is the
 * predicate that proves it; a view outside that gate keeps the union range.
 * <p>
 * The residual - everything at or above the active segment's start - is not decomposed.
 * It is the correction the runtime is still standing in, and it takes the ordinary resume
 * from the anchor below it, which Fix 2 already bounds to one checkpoint cadence.
 * <p>
 * Rows below the view's {@code START FROM} boundary belong to no segment here. They
 * produce no output at all, so the caller drops them before they reach {@link #addRow}
 * rather than letting them drag the change floor down to a boundary that denies the
 * repair.
 * <h2>The affected keys</h2>
 * A segment also collects the partition keys its corrections carried, when the caller asks
 * for them. Inside one closed segment only those keys' output has changed - every other key
 * is already correct - so they are what a keyed replay would follow through the base's
 * posting index instead of reading every row of the segment. The values are the base
 * table's own integer symbol keys, as the repair's pinned reader names them, rather than
 * the WAL's: a WAL symbol integer indexes one transaction's symbol space, while the keyed
 * scan and the keyed replay both follow the posting index of the reader the repair pins.
 * {@link #resolveKey} translates one into the other, and the caller resolves against that
 * same pinned reader, so nothing downstream has to look a key up a second time.
 * <p>
 * The collection is bounded per segment and its overflow is not a denial: a segment past
 * its budget reports {@link #isSegmentKeyDomainComplete(int)} false and reads whole, which
 * costs the same write and only a larger read. A key the pinned reader does not hold is
 * treated the same way. The caller walks no transaction above the pinned reader's own, so
 * every key it resolves is one that reader applied; a key it still cannot find would be a
 * key whose rows a keyed replay would miss, so it leaves the domain incomplete rather than
 * dropping out of it.
 * <p>
 * Worker-owned scratch: {@link #of} clears every field, so one instance serves every repair
 * a refresh worker plans. The keys live in native memory the instance allocates on the
 * first keyed repair and retains across repairs, so collecting a key costs no heap object;
 * {@link #close()} releases it.
 */
public final class LiveViewCheckpointSegmentChangeSet implements QuietCloseable {
    /**
     * How many distinct closed segments one change set may decompose into before the
     * decomposition stops being worth taking. Each segment costs its own replay, its own
     * replacement commit and its own timeline splice, so a change reaching more of them
     * than this is one the union range serves better. The measured workload's mean is
     * 1.68 and its maximum 35.
     */
    public static final int MAX_CLOSED_SEGMENTS = 64;
    // No WAL symbol integer is negative but the null one, which resolveKey answers without
    // the map, so -1 can mark an empty slot.
    private static final int NO_WAL_KEY = -1;
    // The key-set ordinal the open segment's keys are deduplicated under. The closed
    // segments take ordinals 0..MAX_CLOSED_SEGMENTS - 1, in the order they were opened.
    private static final int RESIDUAL_KEY_SET = MAX_CLOSED_SEGMENTS;
    // segmentStart, segmentEndExclusive, minTs, maxTs, keySetIndex,
    // isKeyDomainOverflowed, hasNullKey per entry, ordered by segmentStart ascending.
    private static final int STRIDE = 7;
    // The affected keys of each segment, in the order the segments were opened rather than
    // in segment order: an entry names its list by index, so a segment inserted ahead of
    // another does not have to move anyone's keys. Each list holds distinct base symbol
    // keys in the order they arrived, which keyMembership guarantees. Retained across
    // repairs and cleared rather than dropped, so a worker pays for the growth once; a list
    // allocates its native block on its first key.
    private final ObjList<DirectIntList> keySets = new ObjList<>();
    // The open segment's own affected keys, kept apart from the closed segments' lists
    // because the residual is not a segment: it has no start, no end and no entry, and the
    // repair that reads it is the resume rather than a segment replay.
    private final DirectIntList residualKeys = new DirectIntList(0, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
    // Exact timestamps of rows newly inserted into the open segment while collecting its
    // key domain. On an unfiltered, non-deduplicating base each contributes exactly one
    // live-view output row, so this is the arithmetic replacement for scanning every
    // stored output row merely to recount checkpoint positions.
    private final LongList residualRowTimestamps = new LongList();
    private final LongList segments = new LongList();
    private long activeSegmentStart;
    // Whether the caller asked for the residual's keys. A caller that did not gets the
    // whole-commit shortcut and the empty set below, which isResidualKeyDomainComplete
    // reports as incomplete rather than as an empty domain.
    private boolean collectsResidualKeys;
    private boolean hasResidualNullKey;
    private boolean residualKeyDomainOverflowed;
    private boolean residualRowsSorted;
    // How many distinct keys one segment may collect before the collection stops being
    // worth its memory. Zero collects none, which is what a caller that cannot read the key
    // column asks for. This is not the configured budget's zero: the refresh job maps a
    // configured budget at or below zero, which means unlimited, to Integer.MAX_VALUE.
    private int maxKeysPerSegment;
    // Containment cache for the row loop: consecutive rows of one commit almost always
    // share a segment, and a hit skips both the floor arithmetic and the lookup.
    private long cachedSegmentEndExclusive;
    private long cachedSegmentStart;
    // (keySetOrdinal << 32 | baseKey) for every key any list holds, so one probe answers
    // whether a key is already in its segment's list. One set for every list rather than a
    // set per list: the lists grow and clear together, and a single block is one
    // allocation to retain and one to free. Allocated by the first keyed repair.
    private DirectLongHashSet keyMembership;
    private boolean overflowed;
    private long residualMaxTs;
    private long residualMinTs;
    // One WAL transaction's symbol integer -> the pinned reader's, so a key a transaction
    // repeats row after row costs one reverse lookup rather than one per row. Scoped to a
    // transaction because the WAL writer reuses local symbol ids across them. Opened by the
    // first keyed repair.
    private DirectIntIntHashMap resolvedKeys;

    /**
     * Folds one qualifying base row into the decomposition. A row at or above
     * {@code activeSegmentStart} joins the residual; anything below it lands in - or opens -
     * the closed segment that holds it.
     *
     * @param key the row's partition key as the pinned base reader's symbol integer -
     *            {@link SymbolTable#VALUE_IS_NULL} for the null symbol and
     *            {@link SymbolTable#VALUE_NOT_FOUND} for a value that reader does not hold,
     *            which is what {@link #resolveKey} returns. Ignored unless
     *            {@link #isCollectingKeys()} holds
     * @return false once the change set gives up, after which the decomposition is
     * abandoned and the caller falls back to the union range. It gives up when the row
     * would open more than {@link #MAX_CLOSED_SEGMENTS} closed segments, and when the plan
     * refuses the row's own segment - no representable start, no representable end, or an
     * end above {@code activeSegmentStart}. Every later row is refused too, since
     * {@link #isOverflowed()} latches
     */
    public boolean addRow(long ts, int key, @NotNull LiveViewCheckpointAnchorPlan anchorPlan) {
        if (ts >= activeSegmentStart) {
            widenResidual(ts, ts);
            if (collectsResidualKeys) {
                residualRowTimestamps.add(ts);
                residualRowsSorted = false;
            }
            addResidualKey(key);
            return true;
        }
        if (overflowed) {
            return false;
        }
        if (ts < cachedSegmentStart || ts >= cachedSegmentEndExclusive) {
            final long start = anchorPlan.getSegmentStart(ts);
            final long end = anchorPlan.getSegmentEndExclusive(ts);
            if (start == Long.MIN_VALUE || end == Numbers.LONG_NULL || end > activeSegmentStart) {
                // An open-below start is a refusal rather than a floor - the plan reports it
                // for a row under a non-zero alignment origin and for a zone floor a
                // transition makes non-monotone, and it comes with a finite end often
                // enough that reading the end alone is not enough. Installed as a floor it
                // would swallow every row below that end, however far back, and nest the
                // segments those rows belong to inside it. No representable segment end is
                // H = EOF, which no localized repair can stand on; an end above the active
                // segment's start means the arithmetic does not agree with the runtime's own
                // segmentation. In every case this row has no closed segment of its own, so
                // the decomposition cannot describe it.
                overflowed = true;
                return false;
            }
            cachedSegmentStart = start;
            cachedSegmentEndExclusive = end;
        }
        final int index = indexOf(cachedSegmentStart);
        if (index >= 0) {
            final int base = index * STRIDE;
            segments.setQuick(base + 2, Math.min(segments.getQuick(base + 2), ts));
            segments.setQuick(base + 3, Math.max(segments.getQuick(base + 3), ts));
            addKey(base, key);
            return true;
        }
        if (segments.size() / STRIDE >= MAX_CLOSED_SEGMENTS) {
            overflowed = true;
            return false;
        }
        final int base = insertAt(-index - 1, cachedSegmentStart, cachedSegmentEndExclusive, ts);
        addKey(base, key);
        return true;
    }

    /**
     * Folds a whole commit's span into the residual without visiting its rows. The caller
     * takes this shortcut for a commit whose own minimum already sits at or above the
     * active segment's start, which is every in-order commit and every shallow correction.
     */
    public void addResidual(long minTs, long maxTs) {
        widenResidual(minTs, maxTs);
        // The shortcut folds a commit's span without visiting a row, so whatever keys it
        // carried are keys this change set never saw. A caller collecting the residual's
        // domain must walk those rows instead; one that takes the shortcut anyway gets an
        // incomplete domain rather than a domain short of the keys it skipped.
        residualKeyDomainOverflowed = true;
    }

    @Override
    public void close() {
        Misc.freeObjListAndClear(keySets);
        Misc.free(residualKeys);
        keyMembership = Misc.free(keyMembership);
        resolvedKeys = Misc.free(resolvedKeys);
    }

    /**
     * @return the number of distinct closed anchor segments the change set touches
     */
    public int getClosedSegmentCount() {
        return segments.size() / STRIDE;
    }

    /**
     * @return the highest timestamp the change set touched at or above the active
     * segment's start, or {@link Numbers#LONG_NULL} when it touched nothing there
     */
    public long getResidualMaxTs() {
        return residualMaxTs;
    }

    /**
     * @return the lowest timestamp the change set touched at or above the active
     * segment's start, or {@link Numbers#LONG_NULL} when it touched nothing there.
     * This is the correction floor the residual repair plans from.
     */
    public long getResidualMinTs() {
        return residualMinTs;
    }

    /**
     * Number of newly inserted open-segment rows at or below {@code timestamp}.
     * Equal-timestamp rows are included as a group, matching checkpoint boundary
     * semantics. Meaningful only when the residual key domain is complete.
     */
    public int getResidualRowCountAtOrBelow(long timestamp) {
        if (!residualRowsSorted) {
            residualRowTimestamps.sort();
            residualRowsSorted = true;
        }
        final int index = residualRowTimestamps.binarySearch(timestamp, Vect.BIN_SEARCH_SCAN_DOWN);
        return index >= 0 ? index + 1 : -index - 1;
    }

    /**
     * Total newly inserted rows in the open segment over the classified range.
     */
    public int getResidualRowCount() {
        return residualRowTimestamps.size();
    }

    /**
     * @return the affected keys of the open anchor segment - the pinned base reader's
     * symbol keys the corrections at or above {@code activeSegmentStart} carried, distinct,
     * which a keyed resume follows through the base's posting index instead of reading
     * every row above its anchor. Meaningless unless {@link #isResidualKeyDomainComplete()}
     * holds, and valid until the next {@link #of}.
     * <p>
     * The list carries no null, which {@link #hasResidualNullKey()} reports separately, for
     * the same reason a closed segment's does: a duplicate null in a keyed scan's key list
     * yields the null key's rows twice.
     */
    public @NotNull DirectIntList getResidualKeys() {
        return residualKeys;
    }

    /**
     * @return whether the open anchor segment was touched by a correction carrying a null
     * partition key
     */
    public boolean hasResidualNullKey() {
        return hasResidualNullKey;
    }

    /**
     * @return whether the keys collected for the open anchor segment are all of them.
     * False when the caller collected none, when the collection reached its budget, when a
     * key did not resolve against the pinned reader, and when any commit was folded through
     * {@link #addResidual} without its rows being walked - in every case the resume has to
     * read every row above its anchor, which costs what it always did.
     */
    public boolean isResidualKeyDomainComplete() {
        return collectsResidualKeys && !residualKeyDomainOverflowed;
    }

    /**
     * @return the exclusive end of closed segment {@code index}. Carried alongside the
     * start because the keyed scan's cost model prices a segment off this entry, and
     * re-deriving the end there would need the anchor plan the segment was placed
     * against.
     */
    public long getSegmentEndExclusive(int index) {
        return segments.getQuick(index * STRIDE + 1);
    }

    /**
     * @return the highest in-view timestamp the change set touched inside closed segment
     * {@code index}
     */
    public long getSegmentMaxTs(int index) {
        return segments.getQuick(index * STRIDE + 3);
    }

    /**
     * @return the lowest in-view timestamp the change set touched inside closed segment
     * {@code index}
     */
    public long getSegmentMinTs(int index) {
        return segments.getQuick(index * STRIDE + 2);
    }

    /**
     * @return the affected keys of closed segment {@code index} - the pinned base reader's
     * symbol keys the corrections carried, distinct, which a keyed replay would follow
     * through the base's posting index. Empty when the caller collected none, meaningless
     * unless {@link #isSegmentKeyDomainComplete(int)} holds, and valid until the next
     * {@link #of}.
     * <p>
     * The list carries no null, which {@link #hasSegmentNullKey(int)} reports separately -
     * it is a partition key like any other, and holding it beside the list rather than in
     * it is what lets a caller walk the list by index without testing for one.
     */
    public @NotNull DirectIntList getSegmentKeys(int index) {
        return keySets.getQuick((int) segments.getQuick(index * STRIDE + 4));
    }

    /**
     * @return whether closed segment {@code index} was touched by a correction carrying a
     * null partition key
     */
    public boolean hasSegmentNullKey(int index) {
        return segments.getQuick(index * STRIDE + 6) != 0;
    }

    /**
     * @return whether the keys collected for closed segment {@code index} are all of them.
     * False once the segment reached its key budget, once one of its keys did not resolve
     * against the pinned reader, or when the caller collected no keys at all - in every
     * case a repair of that segment has to read every row of it, which costs the same
     * write and only a larger read.
     */
    public boolean isSegmentKeyDomainComplete(int index) {
        return maxKeysPerSegment > 0 && segments.getQuick(index * STRIDE + 5) == 0;
    }

    /**
     * @return the inclusive start of closed segment {@code index}. Segments come back
     * oldest first, which is the order a repair must take them in: a later segment's
     * cumulative row positions depend on how many rows the earlier ones added.
     */
    public long getSegmentStart(int index) {
        return segments.getQuick(index * STRIDE);
    }

    /**
     * @return whether the repair {@link #of} last bound this scratch to collects keys at
     * all. When it does not, {@link #addRow} reads no row's key, closed segment or
     * residual, so a caller can skip {@link #resolveKey} - and the base symbol lookup it
     * costs - for every row.
     */
    public boolean isCollectingKeys() {
        return maxKeysPerSegment > 0;
    }

    /**
     * @return true once the decomposition gave up - too many distinct closed segments, or a
     * row whose segment has no representable start, has no representable end, or ends above
     * the active segment's start. The caller must then repair the whole change set as one
     * union range.
     */
    public boolean isOverflowed() {
        return overflowed;
    }

    /**
     * Rebinds this scratch to one repair. {@code activeSegmentStart} is the inclusive start
     * of the anchor segment the runtime frontier sits in: everything below it can be
     * repaired independently, everything at or above it is the runtime's own segment and
     * stays with the residual.
     */
    public void of(long activeSegmentStart) {
        of(activeSegmentStart, 0, false);
    }

    /**
     * Rebinds this scratch to one repair that also collects the keys its corrections carry.
     * {@code maxKeysPerSegment} bounds one segment's key domain; a segment that reaches it
     * keeps the keys it has and reports the domain incomplete, which demotes that segment
     * to a whole-segment replay rather than denying it.
     */
    public void of(long activeSegmentStart, int maxKeysPerSegment) {
        of(activeSegmentStart, maxKeysPerSegment, false);
    }

    /**
     * As above, and also collects the keys the corrections at or above
     * {@code activeSegmentStart} carry when {@code collectResidualKeys} holds.
     * <p>
     * A caller that asks for them owes the walk: {@link #addResidual}'s whole-commit
     * shortcut visits no row, so taking it leaves the domain incomplete rather than short.
     * A caller that does not ask keeps the shortcut and the resume reads every row above
     * its anchor, which is what every resume did before the keyed one existed.
     */
    public void of(long activeSegmentStart, int maxKeysPerSegment, boolean collectResidualKeys) {
        this.activeSegmentStart = activeSegmentStart;
        this.maxKeysPerSegment = maxKeysPerSegment;
        this.collectsResidualKeys = collectResidualKeys && maxKeysPerSegment > 0;
        for (int i = 0, n = keySets.size(); i < n; i++) {
            keySets.getQuick(i).clear();
        }
        residualKeys.clear();
        if (keyMembership != null) {
            keyMembership.clear();
        } else if (maxKeysPerSegment > 0) {
            keyMembership = new DirectLongHashSet(16, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        }
        if (maxKeysPerSegment > 0 && resolvedKeys == null) {
            resolvedKeys = new DirectIntIntHashMap(
                    16,
                    0.5,
                    NO_WAL_KEY,
                    SymbolTable.VALUE_NOT_FOUND,
                    MemoryTag.NATIVE_LIVE_VIEW_IN_MEM,
                    false
            );
        }
        residualRowTimestamps.clear();
        residualRowsSorted = true;
        hasResidualNullKey = false;
        residualKeyDomainOverflowed = false;
        segments.clear();
        overflowed = false;
        residualMinTs = Numbers.LONG_NULL;
        residualMaxTs = Numbers.LONG_NULL;
        // An empty cache must miss on the first row whatever it is.
        cachedSegmentStart = Long.MAX_VALUE;
        cachedSegmentEndExclusive = Long.MIN_VALUE;
    }

    /**
     * Starts resolving the keys of one WAL transaction. The WAL writer reuses its local
     * symbol integers across transactions, so a translation {@link #resolveKey} cached for
     * the previous one may name a different value in this one.
     */
    public void ofTransaction() {
        if (resolvedKeys != null) {
            // Back to the initial block rather than a clear of the grown one: a clear costs
            // the whole capacity, and one wide transaction would otherwise charge it to
            // every narrow transaction after it.
            resolvedKeys.restoreInitialCapacity();
        }
    }

    /**
     * Translates one row's WAL symbol integer into the pinned base reader's, which is the
     * key {@link #addRow} takes. The translation goes through the value, since the two
     * integers index different symbol spaces, and is cached for the rest of the
     * transaction {@link #ofTransaction} opened.
     *
     * @param walKey      the row's symbol integer in its WAL transaction's own space
     * @param walSymbols  that transaction's symbol table
     * @param baseSymbols the pinned base reader's symbol table for the same column
     * @return the base reader's key, {@link SymbolTable#VALUE_IS_NULL} for the null symbol,
     * or {@link SymbolTable#VALUE_NOT_FOUND} for a value the base reader does not hold
     */
    public int resolveKey(int walKey, @NotNull StaticSymbolTable walSymbols, @NotNull StaticSymbolTable baseSymbols) {
        if (walKey < 0 || resolvedKeys == null) {
            // The null symbol, and anything else no WAL value is stored under, which
            // valueOf answers with null and keyOf maps to VALUE_IS_NULL. A change set
            // collecting no keys has no cache to consult either.
            return baseSymbols.keyOf(walSymbols.valueOf(walKey));
        }
        resolvedKeys.reopen();
        final long index = resolvedKeys.keyIndex(walKey);
        if (index < 0) {
            return resolvedKeys.valueAt(index);
        }
        final int baseKey = baseSymbols.keyOf(walSymbols.valueOf(walKey));
        resolvedKeys.putAt(index, walKey, baseKey);
        return baseKey;
    }

    /**
     * Binary search over the segment starts. Returns the entry index when found, and
     * {@code -(insertionPoint) - 1} when not, in the {@code Arrays.binarySearch} shape.
     */
    private int indexOf(long segmentStart) {
        int low = 0;
        int high = segments.size() / STRIDE - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midStart = segments.getQuick(mid * STRIDE);
            if (midStart < segmentStart) {
                low = mid + 1;
            } else if (midStart > segmentStart) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    /**
     * Joins one row's key to the open segment's domain, on the same terms
     * {@link #addKey} joins a closed segment's.
     */
    private void addResidualKey(int key) {
        if (!collectsResidualKeys) {
            return;
        }
        if (key == SymbolTable.VALUE_IS_NULL) {
            hasResidualNullKey = true;
            return;
        }
        if (!addDistinctKey(residualKeys, RESIDUAL_KEY_SET, key)) {
            residualKeyDomainOverflowed = true;
        }
    }

    /**
     * Widens the open segment's span without touching its key domain, which is what the
     * row walk and the whole-commit shortcut disagree about.
     */
    private void widenResidual(long minTs, long maxTs) {
        residualMinTs = residualMinTs == Numbers.LONG_NULL ? minTs : Math.min(residualMinTs, minTs);
        residualMaxTs = residualMaxTs == Numbers.LONG_NULL ? maxTs : Math.max(residualMaxTs, maxTs);
    }

    /**
     * Joins one row's key to the segment at {@code base}, and records the budget overflow
     * that leaves the segment's key domain incomplete.
     */
    private void addKey(int base, int key) {
        if (maxKeysPerSegment < 1) {
            return;
        }
        if (key == SymbolTable.VALUE_IS_NULL) {
            // Recorded beside the segment rather than in the list, so it costs no budget
            // and no slot - and, more to the point, so a caller walking the list by index
            // never has to test its entries for null. A duplicate null in a keyed scan's
            // key list yields the null key's rows twice.
            segments.setQuick(base + 6, 1);
            return;
        }
        final int keySetIndex = (int) segments.getQuick(base + 4);
        if (!addDistinctKey(keySets.getQuick(keySetIndex), keySetIndex, key)) {
            segments.setQuick(base + 5, 1);
        }
    }

    /**
     * Appends {@code key} to {@code keys} unless the list already holds it.
     *
     * @return false when the key leaves the list's domain incomplete: it did not resolve
     * against the pinned reader, or it is new and the list already holds its budget
     */
    private boolean addDistinctKey(DirectIntList keys, int keySetOrdinal, int key) {
        if (key < 0) {
            // VALUE_NOT_FOUND. A key missing from a keyed scan is a key whose rows it
            // would not repair, so it demotes the domain rather than dropping out of it.
            return false;
        }
        final long member = ((long) keySetOrdinal << 32) | key;
        if (keys.size() >= maxKeysPerSegment) {
            return keyMembership.contains(member);
        }
        if (keyMembership.add(member)) {
            keys.add(key);
        }
        return true;
    }

    /**
     * @return the entry's base offset in {@link #segments}
     */
    private int insertAt(int index, long segmentStart, long segmentEndExclusive, long ts) {
        // The key list is taken off the pool in discovery order and named by index, so an
        // entry inserted ahead of another leaves every other segment's keys where they are.
        final int keySetIndex = segments.size() / STRIDE;
        if (keySets.size() <= keySetIndex) {
            // Zero capacity allocates nothing until the list's first key, so a segment the
            // caller collects no keys for costs no native block.
            keySets.extendAndSet(keySetIndex, new DirectIntList(0, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM));
        }
        // Inserted in reverse so each add() lands ahead of the ones before it, leaving
        // (segmentStart, segmentEndExclusive, minTs, maxTs, keySetIndex,
        // isKeyDomainOverflowed, hasNullKey) in order at the entry's own base offset.
        final int base = index * STRIDE;
        segments.add(base, 0);
        segments.add(base, 0);
        segments.add(base, keySetIndex);
        segments.add(base, ts);
        segments.add(base, ts);
        segments.add(base, segmentEndExclusive);
        segments.add(base, segmentStart);
        return base;
    }
}
