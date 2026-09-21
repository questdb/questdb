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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SymbolMapReader;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.Chars;
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;

/**
 * Per-live-view eager-interning symbol cache for the in-memory tier's
 * un-flushed lead (Mode A, "eager symbol interning").
 * <p>
 * A SYMBOL output column stores an integer symbol id in the tier. For rows that
 * are already on the LV's on-disk table (the overlap), that id is the LV-table
 * symbol id and a query resolves it against the disk reader's symbol table. The
 * un-flushed lead, however, has no disk row yet, and a value that is new to the
 * lead has no committed LV-table id at all - the apply that would assign one only
 * runs at flush. This cache assigns those new lead values an id that is
 * <em>consistent with</em> the LV-table id the eventual flush will produce, and
 * holds the {@code id -> string} mapping so a query can resolve the lead from RAM.
 * <p>
 * Id assignment keeps a single LV-table id space so that read paths which
 * resolve a SYMBOL by its raw int key against {@code getSymbolTable()} (WHERE
 * filters, GROUP BY, static ORDER BY) stay correct, not just the {@code getSymA}
 * per-record path (printing, HTTP / PGWire):
 * <ul>
 *   <li>An already-committed value resolves to its committed id via the LV
 *     table's {@link SymbolMapReader#keyOf}.</li>
 *   <li>A value new to the un-flushed lead is assigned the next id at or above the
 *     committed symbol count. Because the refresh worker drains base commits in
 *     the same order the flush's apply re-interns them (in-order leads only; an
 *     out-of-order arrival is diverted to {@code o3Replay}, which recomputes from
 *     disk), the assigned id equals the one apply will produce. After flush the
 *     lead becomes overlap and its stored ids already agree with disk.</li>
 * </ul>
 * <p>
 * Threading: the refresh worker is the only writer ({@link #intern},
 * {@link #anchor}, {@link #onFlush}, {@link #onO3}). Cursors read the append-only
 * {@code id -> string} lists through {@link #newSymbolValueOf} and the concurrent
 * {@code string -> id history} maps through {@link #newSymbolKeyOf}. A stored id
 * and its reverse-index entry are assigned before the slot that carries the id is
 * published. The lists and maps publish their entries with release/acquire semantics.
 * <p>
 * A reader must never resolve an assignment at or beyond its pinned slot's symbol
 * horizon - the
 * exclusive id bound stamped on the slot at publish (see
 * {@link LiveViewInMemoryBuffer#newSymbolMaxId}). The reverse index retains a
 * newest-first immutable chain because O3 or replica rebinding can assign the same
 * string a later id while an older pinned slot must still resolve the earlier id.
 * {@link #newSymbolKeyOf} walks past assignments at or beyond {@code toId} and
 * returns the newest one in the requested band. The overlay sources {@code toId}
 * from the slot, never from a live {@link #newSymbolMaxIdExclusive} read.
 * <p>
 * {@link ConcurrentCharSequenceList} provides value safety for id-to-string
 * resolution. A later refresh cycle can grow a store's page index after the reader's
 * slot published, so the store release-stores each new index and acquire-loads it on
 * every read. A reader that observes a new index thus also observes the page
 * references copied into it, never a stale null at an in-bounds id (which a plain
 * {@link ObjList}, storing the array with no fence, would expose as a transient
 * spurious miss).
 * <p>
 * Memory: {@link #idToString} retains an immutable string per lead assignment and
 * the reverse map retains one key per distinct value. Neither is cleared before
 * close, because a cursor pinned on an older slot holds a disk reader whose
 * committed count predates the flush that committed those ids, so it resolves them
 * through {@link #newSymbolValueOf} rather than from disk - the cache cannot tell
 * how far back the oldest such reader sits. The id space itself advances once per
 * assignment, so both grow with the number of assignments, not with cardinality.
 * {@link #anchor} also re-bases it onto the committed symbol count on every drain, so
 * the ids the cache holds are a sparse band of a much larger space;
 * {@link ConcurrentCharSequenceList} pages the {@code id -> string} store so that gap
 * costs one reference per page of ids, not a slot per committed symbol.
 * <p>
 * The reverse index, in contrast, IS pruned: {@link #pruneReverseIndex} drops chain
 * nodes a live slot can no longer reach, which bounds both the node count and the
 * {@link #newSymbolKeyOf} walk. Without it a view taking repeated O3 replays - each
 * re-assigning the same values a fresh id - grows one permanent node per assignment
 * and makes every lookup walk them all.
 */
public class LiveViewSymbolCache implements QuietCloseable {
    // Assignments a column must accumulate before pruneReverseIndex walks its
    // reverse map at all. Keeps a small map (the common case: a handful of lead
    // values per flush window) from being walked twice per refresh cycle for
    // nothing.
    private static final int PRUNE_MIN_ASSIGNMENTS = 1024;
    // Per output column, assignments made since the last successful prune. Gates
    // pruneReverseIndex to at most one map walk per that many assignments, which
    // keeps pruning O(1) amortized per assignment. Writer-side only.
    private final IntList assignmentsSincePrune;
    // Per output column, null for non-SYMBOL columns. Read by cursor overlays;
    // append-only and sparse, indexed by absolute LV-table symbol id (null gaps for
    // ids that only ever existed as committed values, which resolve via disk).
    private final ObjList<ConcurrentCharSequenceList> idToString;
    // Per output column, null for non-SYMBOL columns. Readers use this
    // append-only value -> assigned-id-history index. Lookup is expected O(1)
    // except when an old horizon requires walking repeated assignments.
    private final ObjList<ConcurrentHashMap<SymbolIdChain>> stringToIds;
    // Per output column, the next symbol id to assign to a value new to the lead.
    // Anchored at or above the committed symbol count each drain; advances per
    // new value. Persists across drain ticks within a flush window.
    private final IntList nextNewId;
    private final IntList symbolColumns = new IntList();
    // Per output column, null for non-SYMBOL columns. Writer-side only: the
    // current flush window's value -> id map for O(1) interning of a value seen
    // more than once before it is flushed. Cleared at flush / O3 on the primary;
    // a read-only replica never flushes, so {@link #intern} instead drops an entry
    // lazily once the committed count advances past its id (see the stale-entry
    // note there).
    private final ObjList<CharSequenceIntHashMap> windowNewToId;

    public LiveViewSymbolCache(IntList columnTypes) {
        final int n = columnTypes.size();
        this.idToString = new ObjList<>(n);
        this.stringToIds = new ObjList<>(n);
        this.windowNewToId = new ObjList<>(n);
        this.nextNewId = new IntList(n);
        this.assignmentsSincePrune = new IntList(n);
        for (int i = 0; i < n; i++) {
            if (ColumnType.tagOf(columnTypes.getQuick(i)) == ColumnType.SYMBOL) {
                idToString.add(new ConcurrentCharSequenceList());
                stringToIds.add(new ConcurrentHashMap<>());
                windowNewToId.add(new CharSequenceIntHashMap());
                symbolColumns.add(i);
            } else {
                idToString.add(null);
                stringToIds.add(null);
                windowNewToId.add(null);
            }
            nextNewId.add(0);
            assignmentsSincePrune.add(0);
        }
    }

    /**
     * Raises {@code nextNewId} for {@code col} to at least {@code committedCount}.
     * Called at the start of each drain so a flush (or O3) that advanced the
     * committed symbol count re-anchors the next assigned id past it, while a
     * within-window advance (no flush since the last drain) is preserved.
     */
    public void anchor(int col, int committedCount) {
        if (committedCount > nextNewId.getQuick(col)) {
            nextNewId.setQuick(col, committedCount);
        }
    }

    @Override
    public void close() {
        for (int i = 0, n = idToString.size(); i < n; i++) {
            ConcurrentCharSequenceList list = idToString.getQuick(i);
            if (list != null) {
                list.clear();
            }
            ConcurrentHashMap<SymbolIdChain> reverseMap = stringToIds.getQuick(i);
            if (reverseMap != null) {
                reverseMap.clear();
            }
            CharSequenceIntHashMap map = windowNewToId.getQuick(i);
            if (map != null) {
                map.clear();
            }
        }
    }

    public boolean hasSymbolColumns() {
        return symbolColumns.size() > 0;
    }

    /**
     * Returns true when {@code col} is one of the output schema's SYMBOL columns,
     * i.e. the cache holds an {@code id -> string} list for it.
     */
    public boolean isSymbolColumn(int col) {
        return col >= 0 && col < idToString.size() && idToString.getQuick(col) != null;
    }

    /**
     * Committed-first {@link #intern(int, CharSequence, SymbolMapReader, boolean)}:
     * the safe default that never assumes the window map is authoritative. Used
     * where the caller cannot prove it is the sole, reset-on-flush writer (e.g. a
     * read-only replica's externally-flushed lead).
     */
    public int intern(int col, CharSequence value, SymbolMapReader committedReader) {
        return intern(col, value, committedReader, false);
    }

    /**
     * Returns the LV-table-consistent symbol id for {@code value} in column
     * {@code col}, interning a value new to the lead. {@code committedReader} is
     * the LV table's committed symbol map for the column (used to resolve an
     * already-committed value to its committed id). Writer-side only.
     * <p>
     * When {@code windowMapAuthoritative} is true, the un-flushed window map is
     * probed FIRST and a live entry (id at/above the committed count) is returned
     * without a {@code committedReader.keyOf} - a mmapped symbol-index probe that
     * always misses for a not-yet-committed value. This is safe only when this cache
     * is the sole writer AND the window map is reset on every flush (the primary's
     * {@link #onFlush()} / {@link #onO3()}): then a live window entry is always a
     * not-yet-committed provisional, so committed-first and window-first agree. A
     * read-only replica must pass {@code false}: its flush is external and never
     * resets the window map, so a re-sequencing external commit can leave a stale
     * entry above the committed count whose value has since been committed at a
     * different id - only the committed-first probe resolves it correctly.
     */
    public int intern(int col, CharSequence value, SymbolMapReader committedReader, boolean windowMapAuthoritative) {
        if (value == null) {
            return SymbolTable.VALUE_IS_NULL;
        }
        final CharSequenceIntHashMap windowMap = windowNewToId.getQuick(col);
        if (windowMapAuthoritative) {
            // Primary fast path: a live window entry is authoritative here, so skip the
            // committed keyOf. A stale entry (id below the committed count) cannot occur
            // on the primary; a window miss falls through to the committed-first body.
            final int fastKi = windowMap.keyIndex(value);
            if (fastKi < 0 && windowMap.valueAt(fastKi) >= committedReader.getSymbolCount()) {
                return windowMap.valueAt(fastKi);
            }
        }
        final int committedKey = committedReader.keyOf(value);
        if (committedKey != SymbolTable.VALUE_NOT_FOUND) {
            return committedKey;
        }
        int ki = windowMap.keyIndex(value);
        if (ki < 0) {
            final int cachedId = windowMap.valueAt(ki);
            // A window entry maps a value new to the lead to the provisional id a flush will commit it
            // at. That id is at or above the committed count when assigned. On a read-only replica the
            // flush is external (replicated) and the lead loop never resets the window map (no
            // onFlush/onO3), so a committed flush that re-sequenced the symbol id space - e.g. an O3 or
            // delete stranded this value before it was flushed, and a different value took its id - can
            // leave a stale entry whose id now belongs to an already-committed value (id below the
            // committed count). Serving that id resolves the value to the wrong committed string, so drop
            // the stale entry and re-intern above the committed count.
            if (cachedId >= committedReader.getSymbolCount()) {
                return cachedId;
            }
            windowMap.removeAt(ki);
            ki = windowMap.keyIndex(value);
        }
        final int id = nextNewId.getQuick(col);
        nextNewId.setQuick(col, id + 1);
        final String s = Chars.toString(value);
        windowMap.putAt(ki, s, id);
        idToString.getQuick(col).extendAndSet(id, s);
        final ConcurrentHashMap<SymbolIdChain> reverseMap = stringToIds.getQuick(col);
        reverseMap.put(s, new SymbolIdChain(id, reverseMap.get(s)));
        assignmentsSincePrune.setQuick(col, assignmentsSincePrune.getQuick(col) + 1);
        return id;
    }

    /**
     * Looks up {@code value} in the lead's reverse symbol index and returns its
     * newest assigned id in {@code [fromId, toId)}, or
     * {@link SymbolTable#VALUE_NOT_FOUND}.
     * The overlay calls this only after the disk symbol table failed to find the
     * value, with {@code fromId} the disk reader's committed count and {@code toId}
     * the pinned slot's symbol horizon, so the lookup covers just that slot's
     * un-flushed lead band (committed values resolve via disk).
     * <p>
     * {@code toId} must be the slot horizon stamped at publish, not a live
     * {@link #newSymbolMaxIdExclusive} read, so later assignments remain invisible
     * to the pinned slot. See the class threading note.
     */
    public int newSymbolKeyOf(int col, CharSequence value, int fromId, int toId) {
        final ConcurrentHashMap<SymbolIdChain> reverseMap = stringToIds.getQuick(col);
        if (reverseMap == null || value == null) {
            return SymbolTable.VALUE_NOT_FOUND;
        }
        SymbolIdChain chain = reverseMap.get(value);
        while (chain != null) {
            if (chain.id < toId) {
                return chain.id >= fromId ? chain.id : SymbolTable.VALUE_NOT_FOUND;
            }
            chain = chain.previous;
        }
        return SymbolTable.VALUE_NOT_FOUND;
    }

    /**
     * One past the highest new-symbol id assigned for {@code col}. Read writer-side
     * only - the tier calls it under the writer sentinel to stamp the slot's symbol
     * horizon at publish (see {@link LiveViewInMemoryBuffer#setNewSymbolMaxId}). A
     * reader bounds its lookup to that stamped horizon, never to this live value
     * (see the class threading note).
     */
    public int newSymbolMaxIdExclusive(int col) {
        final ConcurrentCharSequenceList list = idToString.getQuick(col);
        return list == null ? 0 : list.size();
    }

    /**
     * Resolves a new-symbol id of {@code col} to its string, or {@code null} when
     * the id is not one of the lead's new symbols (the overlay then falls back to
     * the disk symbol table). Read by cursors.
     */
    public CharSequence newSymbolValueOf(int col, int id) {
        final ConcurrentCharSequenceList list = idToString.getQuick(col);
        return list != null ? list.valueOf(id) : null;
    }

    /**
     * Clears the current window's value -> id maps after a flush; the just-flushed
     * values are now committed and re-resolve via the disk reader's
     * {@code keyOf}. {@code nextNewId} is left in place: it already equals the new
     * committed count, so the next window's first new value continues from there.
     */
    public void onFlush() {
        clearWindowMaps();
    }

    /**
     * Resets the writer-side window maps after an O3 replay re-sequenced the
     * on-disk symbol ids. {@code idToString} is left intact (a pinned pre-O3
     * cursor still resolves its slot from it); the next drain re-anchors
     * {@code nextNewId} to the post-replay committed count via {@link #anchor}.
     */
    public void onO3() {
        clearWindowMaps();
    }

    /**
     * Drops the reverse-index chain nodes no live slot can reach for {@code col}.
     * {@code minLiveHorizonId} is the LOWEST symbol horizon stamped on any tier
     * slot: the smallest {@code toId} {@link #newSymbolKeyOf} can be called with
     * while those slots stay pinnable.
     * <p>
     * That walk returns at the first node below {@code toId}, so for the oldest
     * reachable {@code toId} only one node below {@code minLiveHorizonId} - the
     * newest - can ever be the answer, and every node older than it is dead. This
     * keeps all nodes at or above the horizon plus that one, and truncates the rest.
     * A view whose O3 replays re-assign the same values over and over therefore
     * retains one node per distinct value plus the live band, instead of one node
     * per assignment for the view's whole life.
     * <p>
     * Writer-side only, and safe against a concurrent {@link #newSymbolKeyOf}: a
     * reader bounded by {@code toId >= minLiveHorizonId} stops at or above the
     * retained node and never dereferences the truncated tail, and one that already
     * loaded a tail reference reads immutable, still-valid nodes.
     * <p>
     * Gated: returns without walking unless the column has taken at least
     * {@link #PRUNE_MIN_ASSIGNMENTS} assignments since the last prune AND at least
     * as many as the reverse map holds keys, which bounds the amortized cost at
     * O(1) per assignment. Allocates one iterator per walk.
     */
    public void pruneReverseIndex(int col, int minLiveHorizonId) {
        if (minLiveHorizonId <= 0) {
            // No slot carries a horizon yet (a freshly cleared slot reports 0), so
            // there is no id band a reader has provably moved past.
            return;
        }
        final ConcurrentHashMap<SymbolIdChain> reverseMap = stringToIds.getQuick(col);
        if (reverseMap == null) {
            return;
        }
        final int assignments = assignmentsSincePrune.getQuick(col);
        if (assignments < PRUNE_MIN_ASSIGNMENTS || assignments < reverseMap.size()) {
            return;
        }
        for (SymbolIdChain head : reverseMap.values()) {
            SymbolIdChain node = head;
            while (node != null && node.id >= minLiveHorizonId) {
                node = node.previous;
            }
            if (node != null && node.previous != null) {
                node.previous = null;
            }
        }
        assignmentsSincePrune.setQuick(col, 0);
    }

    /**
     * Number of SYMBOL output columns the cache holds an {@code id -> string} list
     * for. The tier iterates these to stamp each column's symbol horizon onto a
     * slot at publish (see {@link #symbolColumnIndexAt}).
     */
    public int symbolColumnCount() {
        return symbolColumns.size();
    }

    /**
     * Output-column index of the {@code i}-th SYMBOL column, {@code i} in
     * {@code [0, symbolColumnCount())}. Lets the tier stamp per-column symbol
     * horizons without exposing the backing list.
     */
    public int symbolColumnIndexAt(int i) {
        return symbolColumns.getQuick(i);
    }

    private void clearWindowMaps() {
        for (int i = 0, n = symbolColumns.size(); i < n; i++) {
            windowNewToId.getQuick(symbolColumns.getQuick(i)).clear();
        }
    }

    private static final class SymbolIdChain {
        private final int id;
        // Newest-first link to the same value's previous assignment. Written twice:
        // at construction, and by pruneReverseIndex when it truncates a tail no live
        // slot can reach. Volatile so a reader that loads this node sees either the
        // full tail or the truncation, never a torn intermediate; both answer
        // newSymbolKeyOf identically, since the walk stops before the truncated tail.
        private volatile SymbolIdChain previous;

        private SymbolIdChain(int id, SymbolIdChain previous) {
            this.id = id;
            this.previous = previous;
        }
    }
}
