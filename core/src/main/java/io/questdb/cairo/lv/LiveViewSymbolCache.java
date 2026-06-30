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
 * {@link #anchor}, {@link #onFlush}, {@link #onO3}). Cursors read only the
 * append-only {@code id -> string} lists ({@link #newSymbolValueOf},
 * {@link #newSymbolKeyOf}) through a {@link LiveViewSymbolTable} overlay. The
 * lists are append-only and a stored id is always assigned before the slot that
 * carries it is published, so the tier-slot acquire/release CAS supplies the
 * happens-before edge that makes a concurrent read of an already-assigned id
 * safe.
 * <p>
 * A reader must never scan or index past its pinned slot's symbol horizon - the
 * exclusive id bound stamped on the slot at publish (see
 * {@link LiveViewInMemoryBuffer#newSymbolMaxId}). The {@code id -> string} lists
 * are {@link ConcurrentCharSequenceList}s; {@link #intern} grows one through
 * {@link ConcurrentCharSequenceList#extendAndSet}, which reallocates the backing
 * array. A reader that bounded its scan to the list's <em>live</em> {@code size()}
 * could, on a weak-memory host, observe the bumped size paired with the old,
 * shorter array and index out of bounds. Bounding instead to the slot horizon
 * keeps every index at or below the array length that existed when the slot
 * published (the lists only grow), and the slot-pin CAS publishes that array
 * state to the reader - so the scan stays in bounds without a lock or a volatile
 * size. That is why {@link #newSymbolKeyOf} takes an explicit {@code toId} and the
 * overlay sources it from the slot, never from a live
 * {@link #newSymbolMaxIdExclusive} read (which the tier itself reads writer-side
 * only, to stamp the horizon).
 * <p>
 * The horizon bound gives index safety; {@link ConcurrentCharSequenceList} adds
 * value safety. A later refresh cycle can reallocate a list after the reader's
 * slot published - a reallocation the slot-pin CAS does not order to the reader -
 * so the list release-stores each new array and acquire-loads it on every read.
 * A reader that observes a new array thus also observes the element copies that
 * filled it, never a stale null at an in-bounds id (which a plain {@link ObjList},
 * storing the array with no fence, would expose as a transient spurious miss).
 * <p>
 * Memory: {@link #idToString} accumulates one immutable string per distinct
 * value that has ever passed through the lead (it is never cleared, so a pinned
 * cursor can keep resolving its slot). For a SYMBOL column - low cardinality by
 * design - this is bounded like the symbol table itself; it is a known RAM cost
 * for a very high cardinality column, which should not be typed SYMBOL.
 */
public class LiveViewSymbolCache implements QuietCloseable {
    // Per output column, null for non-SYMBOL columns. Read by cursor overlays;
    // append-only, indexed by absolute LV-table symbol id (null gaps for ids
    // that only ever existed as committed values, which resolve via disk).
    private final ObjList<ConcurrentCharSequenceList> idToString;
    // Per output column, the next symbol id to assign to a value new to the lead.
    // Anchored at or above the committed symbol count each drain; advances per
    // new value. Persists across drain ticks within a flush window.
    private final IntList nextNewId;
    private final IntList symbolColumns = new IntList();
    // Per output column, null for non-SYMBOL columns. Writer-side only: the
    // current flush window's value -> id map for O(1) interning of a value seen
    // more than once before it is flushed. Cleared at flush / O3.
    private final ObjList<CharSequenceIntHashMap> windowNewToId;

    public LiveViewSymbolCache(IntList columnTypes) {
        final int n = columnTypes.size();
        this.idToString = new ObjList<>(n);
        this.windowNewToId = new ObjList<>(n);
        this.nextNewId = new IntList(n);
        for (int i = 0; i < n; i++) {
            if (ColumnType.tagOf(columnTypes.getQuick(i)) == ColumnType.SYMBOL) {
                idToString.add(new ConcurrentCharSequenceList());
                windowNewToId.add(new CharSequenceIntHashMap());
                symbolColumns.add(i);
            } else {
                idToString.add(null);
                windowNewToId.add(null);
            }
            nextNewId.add(0);
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
     * Returns the LV-table-consistent symbol id for {@code value} in column
     * {@code col}, interning a value new to the lead. {@code committedReader} is
     * the LV table's committed symbol map for the column (used to resolve an
     * already-committed value to its committed id). Writer-side only.
     */
    public int intern(int col, CharSequence value, SymbolMapReader committedReader) {
        if (value == null) {
            return SymbolTable.VALUE_IS_NULL;
        }
        final int committedKey = committedReader.keyOf(value);
        if (committedKey != SymbolTable.VALUE_NOT_FOUND) {
            return committedKey;
        }
        final CharSequenceIntHashMap windowMap = windowNewToId.getQuick(col);
        final int ki = windowMap.keyIndex(value);
        if (ki < 0) {
            return windowMap.valueAt(ki);
        }
        final int id = nextNewId.getQuick(col);
        nextNewId.setQuick(col, id + 1);
        final String s = Chars.toString(value);
        windowMap.putAt(ki, s, id);
        idToString.getQuick(col).extendAndSet(id, s);
        return id;
    }

    /**
     * Linear-scans the lead's new-symbol ids of {@code col} in {@code [fromId,
     * toId)} for {@code value}, returning its id or {@link SymbolTable#VALUE_NOT_FOUND}.
     * The overlay calls this only after the disk symbol table failed to find the
     * value, with {@code fromId} the disk reader's committed count and {@code toId}
     * the pinned slot's symbol horizon, so the scan covers just that slot's
     * un-flushed lead band (committed values resolve via disk).
     * <p>
     * {@code toId} must be the slot horizon stamped at publish, not a live
     * {@link #newSymbolMaxIdExclusive} read: it is at most the list size when the
     * slot published, and the slot-pin CAS publishes a backing array at least that
     * long to the reader, so every read for {@code i < toId} stays in bounds even
     * while the writer concurrently grows (and reallocates) the list. See the class
     * threading note.
     */
    public int newSymbolKeyOf(int col, CharSequence value, int fromId, int toId) {
        final ConcurrentCharSequenceList list = idToString.getQuick(col);
        if (list == null) {
            return SymbolTable.VALUE_NOT_FOUND;
        }
        final int id = list.indexOf(value, fromId, toId);
        return id < 0 ? SymbolTable.VALUE_NOT_FOUND : id;
    }

    /**
     * One past the highest new-symbol id assigned for {@code col}. Read writer-side
     * only - the tier calls it under the writer sentinel to stamp the slot's symbol
     * horizon at publish (see {@link LiveViewInMemoryBuffer#setNewSymbolMaxId}). A
     * reader bounds its scan to that stamped horizon, never to this live value (the
     * class threading note explains why a live read is not memory-safe).
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
}
