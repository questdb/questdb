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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.Nullable;

final class LiveViewCheckpointMetadata {

    static final byte[] EMPTY_KEY_SCHEMA = {0, 0, 0, 0};
    static final int MAX_BYTE_ARRAY_LENGTH = 1 << 20;
    static final int MAX_ENTRY_COUNT = 1 << 20;
    static final int MAX_STATE_PAGE_REFS = 1 << 16;

    private LiveViewCheckpointMetadata() {
    }

    /**
     * Folds {@code delta} into the sorted {@code (segmentId, useCount)} pairs a
     * root persists, inserting a pair a positive delta needs and dropping one a
     * negative delta empties. Both root kinds keep their data-segment reference
     * counts and their metadata-segment page counts in one such list: the id
     * spaces are disjoint, so a single ordered list serves both.
     */
    static void adjustSegmentUseCount(LongList counts, long segmentId, long delta) {
        if (delta == 0) {
            return;
        }
        int lo = 0;
        int hi = counts.size() / 2;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (counts.getQuick(mid * 2) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < counts.size() / 2 && counts.getQuick(lo * 2) == segmentId) {
            final int countIndex = lo * 2 + 1;
            final long oldCount = counts.getQuick(countIndex);
            if (delta > 0 && oldCount > Long.MAX_VALUE - delta) {
                throw CairoException.critical(0)
                        .put("live view checkpoint root segment use count overflow, segmentId=").put(segmentId);
            }
            final long newCount = oldCount + delta;
            if (newCount < 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint root segment use count underflow, segmentId=").put(segmentId);
            }
            if (newCount == 0) {
                counts.removeIndex(countIndex);
                counts.removeIndex(countIndex - 1);
            } else {
                counts.setQuick(countIndex, newCount);
            }
        } else if (delta > 0) {
            counts.add(lo * 2, segmentId);
            counts.add(lo * 2 + 1, delta);
        } else {
            throw CairoException.critical(0)
                    .put("live view checkpoint root segment use count underflow, segmentId=").put(segmentId);
        }
    }

    static int compareBytes(byte[] left, byte[] right) {
        final int n = Math.min(left.length, right.length);
        for (int i = 0; i < n; i++) {
            final int a = left[i] & 0xff;
            final int b = right[i] & 0xff;
            if (a != b) {
                return a < b ? -1 : 1;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    /**
     * Fills {@code ordinalsOut} with {@code 0 .. keys.size() - 1}, ordered by the
     * unsigned byte order {@link #compareBytes} imposes - which is the order a
     * partition map lays its leaves out in.
     * <p>
     * A seal looks every key it freezes up in the map the previous boundary published,
     * and {@link LiveViewCheckpointPartitionMapReader} memoises one decoded node per
     * depth: consecutive lookups that land in one leaf decode it once, and lookups that
     * arrive in a map cursor's order - hash-slot order, once the key is narrow enough
     * for an unordered map - decode a leaf each, key by key. Walking the ordinals this
     * fills instead makes the lookup cost the leaves the key set touches rather than the
     * keys it holds, whatever order the cursor produced them in.
     * <p>
     * The order is a walk order and nothing else - every caller writes its results by the
     * key's own index and hands its puts to a builder that sorts them again - so a tie
     * this resolves arbitrarily costs nothing but locality.
     * <p>
     * Two arms, picked by whether a key fits a long. A key that does is packed into one
     * as an unsigned big-endian number with its sign bit flipped, so the signed order the
     * native radix sort compares by is the unsigned byte order the tree is laid out in,
     * and {@link Vect#radixSortLongIndexAscChecked} orders the pairs with the ordinal riding
     * along as each pair's index. That is the arm a translated SYMBOL key takes, and it is
     * also the only key narrow enough for the hash-slot-ordered map that makes the sort
     * worth doing; the sort sizes its passes by the key range, so a batch of sequential
     * ids costs two or three linear passes rather than a comparison sort. A wider key falls back to heapsorting the ordinals
     * against the keys themselves - allocating nothing beyond the ordinal list, for the
     * reason {@link LiveViewCheckpointMutationArena}'s own sort does - and skips even that
     * when one linear scan says the keys already arrive in the tree's order, which an
     * {@code OrderedMap} walking its cursor in insertion order often does.
     */
    static void sortKeyOrdinals(ObjList<byte[]> keys, DirectLongList pairScratch, IntList ordinalsOut) {
        final int size = keys.size();
        ordinalsOut.clear();
        if (size > 0 && packKeysIntoPairs(keys, pairScratch)) {
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (int i = 0; i < size; i++) {
                final long packed = pairScratch.get(2L * i);
                min = Math.min(min, packed);
                max = Math.max(max, packed);
            }
            // The native sort sizes its passes by max - min and refuses a range that does
            // not fit a signed long; only an 8-byte key with both sign-bit values can
            // produce one, and that shape takes the comparison arm below instead.
            if (!(min < 0 && max > Long.MAX_VALUE + min)) {
                final long address = pairScratch.getAddress();
                Vect.radixSortLongIndexAscChecked(address, size, address + 2L * size * Long.BYTES, min, max);
                for (int i = 0; i < size; i++) {
                    ordinalsOut.add((int) pairScratch.get(2L * i + 1));
                }
                return;
            }
        }
        for (int i = 0; i < size; i++) {
            ordinalsOut.add(i);
        }
        if (isKeyOrderAscending(keys)) {
            return;
        }
        for (int start = size >>> 1; start-- > 0; ) {
            siftDownKeyOrdinal(keys, ordinalsOut, start, size);
        }
        for (int end = size; --end > 0; ) {
            swapKeyOrdinals(ordinalsOut, 0, end);
            siftDownKeyOrdinal(keys, ordinalsOut, 0, end);
        }
    }

    /**
     * Encodes a partition-key column-type list into the byte string a root
     * persists and a restore compares against the compiled runtime. A null list
     * encodes as a zero column count, which is how a function without a
     * partition map is distinguished from one keyed on no columns at all.
     */
    static byte[] encodeKeySchema(@Nullable ColumnTypes keyTypes) {
        final int count = keyTypes == null ? 0 : keyTypes.getColumnCount();
        if (count == 0) {
            return EMPTY_KEY_SCHEMA;
        }
        final byte[] encoded = new byte[Integer.BYTES + count * Integer.BYTES];
        int offset = putInt(encoded, 0, count);
        for (int i = 0; i < count; i++) {
            offset = putInt(encoded, offset, keyTypes.getColumnType(i));
        }
        return encoded;
    }

    static CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }

    static void putBytes(MemoryA mem, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            mem.putByte(bytes[i]);
        }
    }

    static byte[] encodeUtf8(CharSequence value) {
        final byte[] bytes = new byte[utf8Bytes(value)];
        putUtf8(bytes, 0, value);
        return bytes;
    }

    /**
     * Appends {@code value} to {@code mem} as UTF-8, exactly as {@link #putUtf8(byte[], int,
     * CharSequence)} encodes it, without an intermediate array. {@code utf8Length} is the
     * value's measured UTF-8 length: when it equals the char count the value is ASCII and
     * the copy is a narrowing pass straight into the sink's append address.
     */
    static void putUtf8(MemoryA mem, CharSequence value, int utf8Length) {
        final int n = value.length();
        if (utf8Length == n && mem instanceof MemoryARW arw) {
            Utf8s.strCpyAscii(value, n, arw.appendAddressFor(n));
            return;
        }
        for (int i = 0; i < n; i++) {
            final char ch = value.charAt(i);
            if (ch < 0x80) {
                mem.putByte((byte) ch);
            } else if (ch < 0x800) {
                mem.putByte((byte) (0xc0 | ch >> 6));
                mem.putByte((byte) (0x80 | ch & 0x3f));
            } else if (Character.isHighSurrogate(ch)
                    && i + 1 < n
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                final int codePoint = Character.toCodePoint(ch, value.charAt(++i));
                mem.putByte((byte) (0xf0 | codePoint >> 18));
                mem.putByte((byte) (0x80 | codePoint >> 12 & 0x3f));
                mem.putByte((byte) (0x80 | codePoint >> 6 & 0x3f));
                mem.putByte((byte) (0x80 | codePoint & 0x3f));
            } else if (Character.isSurrogate(ch)) {
                // Matches StandardCharsets.UTF_8's replacement for malformed UTF-16.
                mem.putByte((byte) '?');
            } else {
                mem.putByte((byte) (0xe0 | ch >> 12));
                mem.putByte((byte) (0x80 | ch >> 6 & 0x3f));
                mem.putByte((byte) (0x80 | ch & 0x3f));
            }
        }
    }

    static int putBytes(byte[] sink, int offset, byte[] bytes) {
        System.arraycopy(bytes, 0, sink, offset, bytes.length);
        return offset + bytes.length;
    }

    static int putInt(byte[] sink, int offset, int value) {
        sink[offset] = (byte) (value >>> 24);
        sink[offset + 1] = (byte) (value >>> 16);
        sink[offset + 2] = (byte) (value >>> 8);
        sink[offset + 3] = (byte) value;
        return offset + Integer.BYTES;
    }

    static int putUtf8(byte[] sink, int offset, CharSequence value) {
        for (int i = 0, n = value.length(); i < n; i++) {
            final char ch = value.charAt(i);
            if (ch < 0x80) {
                sink[offset++] = (byte) ch;
            } else if (ch < 0x800) {
                sink[offset++] = (byte) (0xc0 | ch >> 6);
                sink[offset++] = (byte) (0x80 | ch & 0x3f);
            } else if (Character.isHighSurrogate(ch)
                    && i + 1 < n
                    && Character.isLowSurrogate(value.charAt(i + 1))) {
                final int codePoint = Character.toCodePoint(ch, value.charAt(++i));
                sink[offset++] = (byte) (0xf0 | codePoint >> 18);
                sink[offset++] = (byte) (0x80 | codePoint >> 12 & 0x3f);
                sink[offset++] = (byte) (0x80 | codePoint >> 6 & 0x3f);
                sink[offset++] = (byte) (0x80 | codePoint & 0x3f);
            } else if (Character.isSurrogate(ch)) {
                // Matches StandardCharsets.UTF_8's replacement for malformed UTF-16.
                sink[offset++] = '?';
            } else {
                sink[offset++] = (byte) (0xe0 | ch >> 12);
                sink[offset++] = (byte) (0x80 | ch >> 6 & 0x3f);
                sink[offset++] = (byte) (0x80 | ch & 0x3f);
            }
        }
        return offset;
    }

    static int utf8Bytes(CharSequence value) {
        return Utf8s.utf8Bytes(value);
    }

    static void putMetaRef(MemoryA mem, LiveViewCheckpointPageRef ref) {
        mem.putLong(ref.getSegmentId());
        mem.putLong(ref.getOffset());
        mem.putInt(ref.getLength());
    }

    static byte[] readBytes(LiveViewCheckpointMetaSegmentReader reader, long offset, int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = reader.getByte(offset + i);
        }
        return bytes;
    }

    /**
     * Reads {@code length} bytes into an array {@code pool} lends for the caller's
     * current epoch, so a re-read of the same shape reuses the image the previous
     * one filled instead of allocating another.
     */
    static byte[] readBytes(
            LiveViewCheckpointMetaSegmentReader reader,
            long offset,
            int length,
            LiveViewCheckpointByteArrayPool pool
    ) {
        final byte[] bytes = pool.next(length);
        for (int i = 0; i < length; i++) {
            bytes[i] = reader.getByte(offset + i);
        }
        return bytes;
    }

    static void readMetaRef(LiveViewCheckpointMetaSegmentReader reader, long offset, LiveViewCheckpointPageRef out) {
        out.of(reader.getLong(offset), reader.getLong(offset + Long.BYTES), reader.getInt(offset + 2L * Long.BYTES));
    }

    static void validateByteArrayLength(int length, CharSequence field) {
        if (length < 0 || length > MAX_BYTE_ARRAY_LENGTH) {
            throw invalid(field).put(" length out of bounds, length=").put(length);
        }
    }

    static void validateMetaRef(LiveViewCheckpointPageRef ref, boolean nullable, CharSequence field) {
        if (ref.isNull()) {
            if (!nullable || ref.getOffset() != 0 || ref.getLength() != 0) {
                throw invalid(field).put(" metadata page reference invalid");
            }
            return;
        }
        if (ref.getSegmentId() < 0 || ref.getOffset() < LiveViewCheckpointLayout.SEG_HEADER_SIZE
                || ref.getLength() < LiveViewCheckpointLayout.PAGE_HEADER_SIZE) {
            throw invalid(field).put(" metadata page reference invalid")
                    .put(" [segmentId=").put(ref.getSegmentId())
                    .put(", offset=").put(ref.getOffset())
                    .put(", length=").put(ref.getLength()).put(']');
        }
    }

    static void validateStateRef(LiveViewCheckpointStatePageRef ref, boolean nullable, CharSequence field) {
        if (ref.isNull()) {
            if (!nullable || ref.getOffset() != 0 || ref.getStoredLength() != 0 || ref.getDecodedLength() != 0
                    || ref.getPageKind() != 0 || ref.getCodec() != 0 || ref.getRowCount() != 0 || ref.getFlags() != 0) {
                throw invalid(field).put(" state page reference invalid");
            }
            return;
        }
        if (ref.getSegmentId() < 0 || ref.getOffset() < 0 || ref.getStoredLength() <= 0
                || ref.getDecodedLength() < 0 || ref.getPageKind() < 0 || ref.getCodec() < 0 || ref.getRowCount() < 0) {
            throw invalid(field).put(" state page reference invalid")
                    .put(" [segmentId=").put(ref.getSegmentId())
                    .put(", offset=").put(ref.getOffset())
                    .put(", storedLength=").put(ref.getStoredLength())
                    .put(", decodedLength=").put(ref.getDecodedLength())
                    .put(", pageKind=").put(ref.getPageKind())
                    .put(", codec=").put(ref.getCodec())
                    .put(", rowCount=").put(ref.getRowCount()).put(']');
        }
    }

    private static int compareKeyOrdinals(ObjList<byte[]> keys, IntList ordinals, int left, int right) {
        return compareBytes(keys.getQuick(ordinals.getQuick(left)), keys.getQuick(ordinals.getQuick(right)));
    }

    private static boolean isKeyOrderAscending(ObjList<byte[]> keys) {
        for (int i = 1, n = keys.size(); i < n; i++) {
            if (compareBytes(keys.getQuick(i - 1), keys.getQuick(i)) >= 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fills {@code pairScratch} with one {@code (packed key, ordinal)} group per key, most
     * significant byte first and zero-padded on the right, so a key that is a prefix of
     * another sorts ahead of it exactly as {@link #compareBytes} puts it.
     *
     * @return false as soon as a key is found that does not fit a long, leaving
     * {@code pairScratch} half-filled for the next caller to clear
     */
    /**
     * Packs every key into {@code pairScratch} as {@code (sortValue, ordinal)} pairs, the
     * key right-aligned as an unsigned big-endian number with its sign bit flipped, and
     * reserves the copy region the native sort needs behind them. Returns false when a
     * key is wider than a long or the keys differ in length - one key schema never mixes
     * widths, but the byte order of mixed widths is a prefix order this packing cannot
     * reproduce.
     */
    private static boolean packKeysIntoPairs(ObjList<byte[]> keys, DirectLongList pairScratch) {
        final int n = keys.size();
        final int width = keys.getQuick(0).length;
        if (width > Long.BYTES) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            if (keys.getQuick(i).length != width) {
                return false;
            }
        }
        // Two longs per pair, and as many again for the sort's copy region.
        pairScratch.setCapacity(4L * n);
        pairScratch.clear();
        for (int i = 0; i < n; i++) {
            final byte[] key = keys.getQuick(i);
            long packed = 0;
            for (int b = 0; b < width; b++) {
                packed |= (key[b] & 0xffL) << ((width - 1 - b) << 3);
            }
            pairScratch.add(packed ^ Long.MIN_VALUE);
            pairScratch.add(i);
        }
        return true;
    }

    private static void siftDownKeyOrdinal(ObjList<byte[]> keys, IntList ordinals, int root, int end) {
        while (true) {
            final int left = (root << 1) + 1;
            if (left >= end) {
                return;
            }
            int largest = left;
            final int right = left + 1;
            if (right < end && compareKeyOrdinals(keys, ordinals, left, right) < 0) {
                largest = right;
            }
            if (compareKeyOrdinals(keys, ordinals, root, largest) >= 0) {
                return;
            }
            swapKeyOrdinals(ordinals, root, largest);
            root = largest;
        }
    }

    private static void swapKeyOrdinals(IntList ordinals, int left, int right) {
        final int value = ordinals.getQuick(left);
        ordinals.setQuick(left, ordinals.getQuick(right));
        ordinals.setQuick(right, value);
    }
}
