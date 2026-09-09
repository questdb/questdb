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
import io.questdb.std.LongList;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

final class LiveViewCheckpointMetadata {

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
     * Encodes a partition-key column-type list into the byte string a root
     * persists and a restore compares against the compiled runtime. A null list
     * encodes as a zero column count, which is how a function without a
     * partition map is distinguished from one keyed on no columns at all.
     */
    static byte[] encodeKeySchema(@Nullable ColumnTypes keyTypes) {
        final int count = keyTypes == null ? 0 : keyTypes.getColumnCount();
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + count * Integer.BYTES);
        buffer.putInt(count);
        for (int i = 0; i < count; i++) {
            buffer.putInt(keyTypes.getColumnType(i));
        }
        return buffer.array();
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
}
