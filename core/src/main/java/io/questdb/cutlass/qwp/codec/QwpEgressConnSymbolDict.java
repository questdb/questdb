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

package io.questdb.cutlass.qwp.codec;

import io.questdb.cairo.CairoException;
import io.questdb.std.MemoryTag;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8SequenceIntHashMap;
import io.questdb.std.str.DirectUtf8String;

/**
 * Connection-scoped SYMBOL dictionary used by QWP egress when
 * {@code FLAG_DELTA_SYMBOL_DICT} is enabled. Stores the concatenated UTF-8
 * bytes of every unique symbol value seen so far on the connection plus a
 * parallel end-offsets array; the insertion counter doubles as the public
 * {@code connId} for each entry.
 * <p>
 * Entries are allocated directly by per-column callers via {@link #addEntry}.
 * There is no bytes-keyed hash map here -- per-column lookup from the native
 * symbol-table key to a {@code connId} happens in {@code QwpColumnScratch}
 * using an {@code IntIntHashMap}. That's what makes the SYMBOL hot path cheap:
 * no Utf8 hashCode / equals; just an int probe per row.
 * <p>
 * Because conn-ids are allocated from a single counter shared by all columns,
 * the same byte sequence may appear twice if two columns have different native
 * keys that happen to decode to the same string. That's a small wire-byte
 * tradeoff for avoiding bytes-keyed dedup.
 */
public final class QwpEgressConnSymbolDict implements QuietCloseable {

    private static final int INITIAL_ENTRIES = 512;
    private static final int INITIAL_HEAP_BYTES = 4096;
    // Cross-column, cross-query bytes-keyed dedup. Probed once per first-sight
    // (column, native-key) pair, NOT once per row -- per-row dedup happens via
    // the IntIntHashMap on each {@code QwpColumnScratch} before we ever reach
    // {@link #addEntry}. So this map's Utf8 hashCode / equals cost stays bounded
    // by (total unique byte sequences) rather than (total rows).
    private final Utf8SequenceIntHashMap dedupMap = new Utf8SequenceIntHashMap();
    private final DirectUtf8String dedupProbe = new DirectUtf8String();
    private long entriesAddr;      // i32 end-offset per entry
    private int entriesCapacity;   // bytes
    private long heapAddr;
    private int heapCapacity;
    private int heapPos;
    private int size;

    /**
     * Encodes {@code cs} as UTF-8 and returns its {@code connId}. If the same
     * byte sequence already exists in the dict (from a previous batch, query, or
     * column on this connection), its existing id is returned -- no duplicate
     * entry is added. Otherwise a new entry is committed and the new id returned.
     * <p>
     * The dedup uses a bytes-keyed hash map probed speculatively: encode into
     * the heap first, hash the just-written bytes, either commit and record the
     * end offset or rewind the heap pointer if we found an existing match.
     */
    public int addEntry(CharSequence cs) {
        int startPos = heapPos;
        int charLen = cs.length();
        // Worst case UTF-8 expansion: 4 bytes per BMP pair or surrogate pair.
        ensureHeapCapacity(startPos + 4 * charLen);
        heapPos = encodeUtf8(cs, heapAddr, startPos);
        dedupProbe.of(heapAddr + startPos, heapAddr + heapPos);
        int mapIdx = dedupMap.keyIndex(dedupProbe);
        if (mapIdx < 0) {
            // Seen these bytes already on this connection. Rewind the heap write
            // and reuse the existing id.
            heapPos = startPos;
            return dedupMap.valueAtQuick(mapIdx);
        }
        int id = size;
        ensureEntriesCapacity(4 * (size + 1));
        Unsafe.putInt(entriesAddr + 4L * size, heapPos);
        size++;
        // putAt copies probe's bytes into a fresh Utf8String -- a one-time
        // allocation per unique value per connection.
        dedupMap.putAt(mapIdx, dedupProbe, id);
        return id;
    }

    /**
     * Clears the dict completely. Called on connection drop / reset, never on
     * per-query boundaries (conn-ids allocated in earlier queries stay live so
     * the client's view is consistent).
     */
    public void clear() {
        heapPos = 0;
        size = 0;
        dedupMap.clear();
    }

    @Override
    public void close() {
        if (heapAddr != 0) {
            Unsafe.free(heapAddr, heapCapacity, MemoryTag.NATIVE_HTTP_CONN);
            heapAddr = 0;
            heapCapacity = 0;
            heapPos = 0;
        }
        if (entriesAddr != 0) {
            Unsafe.free(entriesAddr, entriesCapacity, MemoryTag.NATIVE_HTTP_CONN);
            entriesAddr = 0;
            entriesCapacity = 0;
        }
        size = 0;
    }

    /**
     * End offset (exclusive) of entry {@code id}. The entry occupies bytes
     * {@code [entryStart(id)..entryEnd(id))} in {@link #getHeapAddr()}.
     */
    public int entryEnd(int id) {
        return Unsafe.getInt(entriesAddr + 4L * id);
    }

    /**
     * Start offset (inclusive) of entry {@code id}. 0 for {@code id == 0};
     * otherwise the end offset of entry {@code id - 1}.
     */
    public int entryStart(int id) {
        return id == 0 ? 0 : Unsafe.getInt(entriesAddr + 4L * (id - 1));
    }

    public long getHeapAddr() {
        return heapAddr;
    }

    /**
     * Bytes committed to the concatenated-UTF-8 heap so far. Combined with
     * {@link #size()}, lets callers enforce connection-level memory caps and
     * decide when to emit a {@code CACHE_RESET} frame.
     */
    public int heapBytes() {
        return heapPos;
    }

    /**
     * Truncates the dict back to {@code targetSize} entries, restoring both the
     * heap write pointer and the dedup map. Used by the egress server to undo
     * entries that were committed by an aborted batch -- a batch that called
     * {@link #addEntry} during row iteration but then failed to ship its
     * {@code RESULT_BATCH} frame on the wire. Without this rollback, the next
     * batch's {@code emitDeltaSection} would omit those ids from the delta
     * window but the dedup map would still hand them back to the row encoder,
     * surfacing a connId the client has never been taught.
     * <p>
     * {@code targetSize == size} is a no-op. Passing a target outside
     * {@code [0, size]} is a programming error and raises {@link CairoException}
     * so the caller surfaces a categorised internal error rather than relying on
     * {@code -ea}.
     */
    public void rollbackTo(int targetSize) {
        if (targetSize == size) {
            return;
        }
        if (targetSize < 0 || targetSize > size) {
            throw CairoException.critical(0)
                    .put("QWP egress: rollbackTo out of range [targetSize=").put(targetSize)
                    .put(", size=").put(size).put(']');
        }
        for (int i = targetSize; i < size; i++) {
            long start = heapAddr + entryStart(i);
            long end = heapAddr + entryEnd(i);
            dedupProbe.of(start, end);
            int idx = dedupMap.keyIndex(dedupProbe);
            if (idx < 0) {
                dedupMap.removeAt(idx);
            }
        }
        heapPos = targetSize == 0 ? 0 : entryEnd(targetSize - 1);
        size = targetSize;
    }

    public int size() {
        return size;
    }

    private static int encodeUtf8(CharSequence cs, long heapAddr, int pos) {
        final int charLen = cs.length();
        for (int i = 0; i < charLen; i++) {
            char c = cs.charAt(i);
            if (c < 0x80) {
                Unsafe.putByte(heapAddr + pos++, (byte) c);
            } else if (c < 0x800) {
                Unsafe.putByte(heapAddr + pos++, (byte) (0xC0 | (c >> 6)));
                Unsafe.putByte(heapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            } else if (Character.isHighSurrogate(c) && i + 1 < charLen
                    && Character.isLowSurrogate(cs.charAt(i + 1))) {
                int cp = Character.toCodePoint(c, cs.charAt(i + 1));
                i++;
                Unsafe.putByte(heapAddr + pos++, (byte) (0xF0 | (cp >> 18)));
                Unsafe.putByte(heapAddr + pos++, (byte) (0x80 | ((cp >> 12) & 0x3F)));
                Unsafe.putByte(heapAddr + pos++, (byte) (0x80 | ((cp >> 6) & 0x3F)));
                Unsafe.putByte(heapAddr + pos++, (byte) (0x80 | (cp & 0x3F)));
            } else {
                Unsafe.putByte(heapAddr + pos++, (byte) (0xE0 | (c >> 12)));
                Unsafe.putByte(heapAddr + pos++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                Unsafe.putByte(heapAddr + pos++, (byte) (0x80 | (c & 0x3F)));
            }
        }
        return pos;
    }

    private void ensureEntriesCapacity(int required) {
        if (entriesCapacity >= required) return;
        int newCap = Math.max(entriesCapacity * 2, Math.max(INITIAL_ENTRIES * 4, required));
        entriesAddr = Unsafe.realloc(entriesAddr, entriesCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
        entriesCapacity = newCap;
    }

    private void ensureHeapCapacity(int required) {
        if (heapCapacity >= required) return;
        int newCap = Math.max(heapCapacity * 2, Math.max(INITIAL_HEAP_BYTES, required));
        heapAddr = Unsafe.realloc(heapAddr, heapCapacity, newCap, MemoryTag.NATIVE_HTTP_CONN);
        heapCapacity = newCap;
    }
}
