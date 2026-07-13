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

import io.questdb.std.Chars;
import io.questdb.std.Mutable;

import java.util.Arrays;

/**
 * Single-writer, multi-reader, append-only {@link CharSequence} list that safely
 * publishes its backing array. {@link LiveViewSymbolCache} keeps its per-column
 * {@code id -> string} lead mappings here: the refresh worker writes
 * ({@link #extendAndSet}, {@link #size}), cursors read ({@link #indexOf},
 * {@link #valueOf}).
 * <p>
 * Unlike a plain {@link io.questdb.std.ObjList}, a growth stores the reallocated
 * array with a release ({@code volatile} field) and every read loads it with an
 * acquire, so a reader that observes a new array also observes the element copies
 * that filled it - an in-bounds id never reads a stale {@code null}. A plain
 * {@code ObjList} stores the new array with no fence, so on a weak-memory host
 * (ARM) a reader could see the new reference before the copies and read the
 * default {@code null} at an in-bounds id (a transient spurious miss).
 * <p>
 * Index safety is the caller's job: it bounds every read to a slot horizon
 * published via the slot-pin CAS (see the {@link LiveViewSymbolCache} note).
 * Element stores stay plain - an id is assigned before its slot publishes, so the
 * CAS (or a later growth's release) publishes it. {@link #size} is writer-only.
 */
final class ConcurrentCharSequenceList implements Mutable {
    private static final int DEFAULT_CAPACITY = 16;
    // Stored with release on growth; read with acquire - publishes element copies.
    private volatile CharSequence[] buffer;
    // Writer-side only: one past the highest assigned id. Readers bound to the horizon.
    private int size;

    ConcurrentCharSequenceList() {
        this.buffer = new CharSequence[DEFAULT_CAPACITY];
    }

    @Override
    public void clear() {
        // Teardown only: the cache clears at close, when no cursor is still reading.
        Arrays.fill(buffer, null);
        size = 0;
    }

    /**
     * Writer-only. Sets {@code value} at {@code index}, growing if needed. A growth
     * copies the elements then release-stores the new array, so an acquiring reader
     * sees the copies.
     */
    void extendAndSet(int index, CharSequence value) {
        CharSequence[] buf = buffer;
        if (index >= buf.length) {
            final int newCap = Math.max(buf.length << 1, index + 1);
            final CharSequence[] grown = new CharSequence[newCap];
            System.arraycopy(buf, 0, grown, 0, buf.length);
            buffer = grown; // release
            buf = grown;
        }
        buf[index] = value;
        if (index >= size) {
            size = index + 1;
        }
    }

    /**
     * Reader. Id of {@code value} in {@code [fromId, toId)}, or {@code -1} if
     * absent. {@code toId} is a published slot horizon, so it stays within the
     * acquired array. Null slots (committed-only ids) are skipped.
     */
    int indexOf(CharSequence value, int fromId, int toId) {
        final CharSequence[] snap = buffer; // acquire: one snapshot per scan
        for (int i = Math.max(0, fromId); i < toId; i++) {
            final CharSequence s = snap[i];
            if (s != null && Chars.equals(s, value)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Writer-only. One past the highest assigned id.
     */
    int size() {
        return size;
    }

    /**
     * Reader. Value at {@code id}, or {@code null} when {@code id} is negative or
     * outside the acquired array (a committed-only id falls back to disk).
     */
    CharSequence valueOf(int id) {
        if (id < 0) {
            return null;
        }
        final CharSequence[] snap = buffer; // acquire
        return id < snap.length ? snap[id] : null;
    }
}
