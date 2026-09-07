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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Compares two {@code LiveViewWindow#snapshot} dumps up to partition-entry order.
 * <p>
 * The dump's header (window name, key types, anchor value type, component width, live
 * count) is fixed, but the entries after it come off an {@code UnorderedMap} cursor, whose
 * order is a function of insertion history rather than of the key set alone. A hoist that
 * rebuilds the map (adopting a checkpoint plan) or a restore that replays a persisted leaf
 * inserts the same keys in a different sequence than the live map that grew one row at a
 * time, so a byte-exact comparison of two dumps that agree on every key's state can still
 * fail on entry order alone. Reslicing each dump into its fixed-width entries and sorting
 * them before comparing removes that false signal while keeping every other mismatch -
 * missing keys, extra keys, wrong bytes in a kept key - exactly as loud as a straight
 * {@code assertArrayEquals} would report it.
 */
final class LiveViewWindowSnapshotAssert {

    private LiveViewWindowSnapshotAssert() {
    }

    static void assertEquals(String message, byte[] expected, byte[] actual) {
        Assert.assertArrayEquals(message, canonicalize(expected), canonicalize(actual));
    }

    /**
     * Reorders a snapshot's partition entries into ascending byte order and hands back a
     * same-length array with the header untouched. Falls back to the input unchanged - not
     * to a partial reorder - whenever the layout cannot be parsed with confidence (a
     * variable-width key, a header field that does not add up), so the caller's comparison
     * degrades to exactly today's byte-exact check rather than risking a canonicalization
     * that quietly hides a real mismatch.
     */
    private static byte[] canonicalize(byte[] snapshot) {
        try {
            final ByteBuffer buf = ByteBuffer.wrap(snapshot).order(ByteOrder.LITTLE_ENDIAN);
            int pos = 0;
            final int nameLen = buf.getInt(pos);
            pos += Integer.BYTES + nameLen * Character.BYTES;
            final int keyColumnCount = buf.getInt(pos);
            pos += Integer.BYTES;
            final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            for (int i = 0; i < keyColumnCount; i++) {
                keyTypes.add(buf.getInt(pos));
                pos += Integer.BYTES;
            }
            pos += Integer.BYTES; // anchor value type
            final int componentStateBytes = buf.getInt(pos);
            pos += Integer.BYTES;
            final long liveCount = buf.getLong(pos);
            pos += Long.BYTES;
            if (!LiveViewSnapshotKeyCodec.isAllTypesFixedWidth(keyTypes) || liveCount < 0 || liveCount > snapshot.length) {
                return snapshot;
            }
            final int keyWidth = LiveViewSnapshotKeyCodec.byteSizeOf(keyTypes);
            final int entryWidth = keyWidth + Long.BYTES + componentStateBytes;
            final int headerLen = pos;
            if (entryWidth <= 0 || headerLen + liveCount * (long) entryWidth != snapshot.length) {
                return snapshot;
            }
            final byte[][] entries = new byte[(int) liveCount][];
            for (int i = 0; i < entries.length; i++) {
                final int from = headerLen + i * entryWidth;
                entries[i] = Arrays.copyOfRange(snapshot, from, from + entryWidth);
            }
            Arrays.sort(entries, LiveViewWindowSnapshotAssert::compareUnsigned);
            final byte[] canon = new byte[snapshot.length];
            System.arraycopy(snapshot, 0, canon, 0, headerLen);
            int offset = headerLen;
            for (byte[] entry : entries) {
                System.arraycopy(entry, 0, canon, offset, entry.length);
                offset += entry.length;
            }
            return canon;
        } catch (RuntimeException e) {
            return snapshot;
        }
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        final int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            final int x = a[i] & 0xFF;
            final int y = b[i] & 0xFF;
            if (x != y) {
                return x - y;
            }
        }
        return a.length - b.length;
    }
}
