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

import io.questdb.std.Mutable;

/**
 * Single-writer, multi-reader, append-only sparse {@link CharSequence} store that
 * safely publishes its backing arrays. {@link LiveViewSymbolCache} keeps its
 * per-column {@code id -> string} lead mappings here: the refresh worker writes
 * ({@link #extendAndSet}, {@link #size}), cursors read ({@link #valueOf}).
 * <p>
 * The index is an ABSOLUTE LV-table symbol id, and the ids this store holds are only
 * the lead's provisional ones - a band that starts at the committed symbol count and
 * can start again far higher after an O3 replay or an externally replicated flush
 * re-anchors it. A flat array indexed by that id would materialize a slot per
 * committed symbol the store never holds: a single lead value past a 4M-symbol
 * dictionary would cost 16MB of nulls, retained until the view closes. So the store
 * is two-level and re-based: a page index holds {@value #PAGE_SIZE}-entry pages,
 * {@code pages[0]} covers the page of the first id ever assigned, and a page stays
 * unallocated until an id lands in it. An unheld id band therefore costs one
 * reference per {@value #PAGE_SIZE} ids of gap, not one per id, and no gap at all
 * below the first assignment.
 * <p>
 * Unlike a plain {@link io.questdb.std.ObjList}, a page-index growth stores the
 * reallocated array with a release ({@code volatile} field) and every read loads it
 * with an acquire, so a reader that observes a new page index also observes the page
 * references copied into it - an in-bounds id never reads a stale {@code null}. A
 * plain {@code ObjList} stores the new array with no fence, so on a weak-memory host
 * (ARM) a reader could see the new reference before the copies and read the default
 * {@code null} at an in-bounds id (a transient spurious miss).
 * <p>
 * The page array and its origin travel together in an immutable {@link PageIndex},
 * published through a single volatile reference. Reading them as a pair is what keeps
 * a re-base honest: a reader that combined one snapshot's array with another's origin
 * would index the wrong page and return the wrong string rather than merely miss.
 * <p>
 * Index safety is the caller's job: it bounds every read to a slot horizon published
 * via the slot-pin CAS (see the {@link LiveViewSymbolCache} note). Element stores and
 * page-reference stores stay plain - an id is assigned, and its page allocated, before
 * the slot carrying that id publishes, so the CAS (or a later growth's release)
 * publishes both. {@link #size} is writer-only.
 */
final class ConcurrentCharSequenceList implements Mutable {
    private static final PageIndex EMPTY = new PageIndex(0, new CharSequence[0][]);
    private static final int INITIAL_PAGE_COUNT = 8;
    // Ids per page. Small enough that a lead of a handful of values costs a kilobyte,
    // large enough that the page index stays ~0.4% of a dense array over the same band.
    private static final int PAGE_BITS = 8;
    private static final int PAGE_MASK = (1 << PAGE_BITS) - 1;
    private static final int PAGE_SIZE = 1 << PAGE_BITS;
    // Release-stored whenever the writer re-bases or grows; acquire-loaded on every read.
    private volatile PageIndex pageIndex = EMPTY;
    // Writer-side only: one past the highest assigned id. Readers bound to the horizon.
    private int size;

    @Override
    public void clear() {
        // Teardown only: the cache clears at close, when no cursor is still reading. Even
        // so, swapping in the empty index leaves nothing torn - a reader either resolves
        // against the whole old index or misses against the empty one.
        pageIndex = EMPTY;
        size = 0;
    }

    /**
     * Writer-only. Sets {@code value} at {@code id}, allocating a page and growing the
     * page index as needed. A growth copies the page references into a fresh index that
     * it release-stores, so an acquiring reader sees the copies.
     * <p>
     * {@code id} must be at or above {@link #size} - the caller assigns ids in increasing
     * order, which is what fixes the index origin at the first call. A violation trips the
     * assertion; with assertions off, an id below the origin indexes out of the page
     * array, but one inside the live band silently overwrites a live id's string, which
     * is why the assertion is there.
     */
    void extendAndSet(int id, CharSequence value) {
        assert id >= size : "ids must be assigned in increasing order, got " + id + " at size " + size;
        final int page = id >>> PAGE_BITS;
        PageIndex snap = pageIndex;
        if (snap.pages.length == 0) {
            // The first assignment fixes the origin, so the store pays nothing for the
            // committed ids below it.
            snap = new PageIndex(page, new CharSequence[INITIAL_PAGE_COUNT][]);
            pageIndex = snap; // release
        }
        final int slot = page - snap.basePage;
        if (slot >= snap.pages.length) {
            final CharSequence[][] grown = new CharSequence[Math.max(snap.pages.length << 1, slot + 1)][];
            System.arraycopy(snap.pages, 0, grown, 0, snap.pages.length);
            snap = new PageIndex(snap.basePage, grown);
            pageIndex = snap; // release
        }
        CharSequence[] values = snap.pages[slot];
        if (values == null) {
            values = new CharSequence[PAGE_SIZE];
            snap.pages[slot] = values;
        }
        values[id & PAGE_MASK] = value;
        if (id >= size) {
            size = id + 1;
        }
    }

    /**
     * Writer-only. One past the highest assigned id.
     */
    int size() {
        return size;
    }

    /**
     * Reader. Value at {@code id}, or {@code null} when {@code id} is negative, outside
     * the acquired page index, or in a page no assignment has reached (a committed-only
     * id falls back to disk).
     */
    CharSequence valueOf(int id) {
        if (id < 0) {
            return null;
        }
        final PageIndex snap = pageIndex; // acquire
        final int slot = (id >>> PAGE_BITS) - snap.basePage;
        if (slot < 0 || slot >= snap.pages.length) {
            return null;
        }
        final CharSequence[] values = snap.pages[slot];
        return values != null ? values[id & PAGE_MASK] : null;
    }

    /**
     * Pairing of the page array with the absolute page index {@code pages[0]} covers.
     * Both travel through one volatile reference, so a reader can never combine one of
     * them with the other's origin - not while the writer re-bases an empty store, not
     * while it grows, and not across {@link #clear()}.
     * <p>
     * The PAIR is frozen at construction; the page slots are not. {@link #extendAndSet}
     * fills them in place on an already-published instance, and their visibility comes
     * from the slot-pin CAS, not from final-field semantics - see the class note.
     */
    private static final class PageIndex {
        private final int basePage;
        private final CharSequence[][] pages;

        private PageIndex(int basePage, CharSequence[][] pages) {
            this.basePage = basePage;
            this.pages = pages;
        }
    }
}
