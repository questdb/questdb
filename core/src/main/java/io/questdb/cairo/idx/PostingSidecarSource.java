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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

/** Reads a private build's completed sidecars while the writer seals a new file version. */
final class PostingSidecarSource implements QuietCloseable {
    private final int[] requiredCover = new int[1];
    private final ColumnVersionReader versions = new ColumnVersionReader();
    private AbstractPostingIndexReader.AbstractCoveringCursor cursor;
    private PostingIndexFwdReader reader;
    private int strideStart;

    PostingSidecarSource(CairoConfiguration configuration, Path path, CharSequence name, long columnTxn,
                         IntList indices, IntList types) {
        try {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            for (int i = 0; i < indices.size(); i++) {
                metadata.add(new TableColumnMetadata("c" + indices.getQuick(i), types.getQuick(i),
                        IndexType.NONE, 0, false, null, indices.getQuick(i), false));
            }
            reader = new PostingIndexFwdReader(configuration, path, name, columnTxn, 0, 0, metadata, versions, 0);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void close() {
        try {
            cursor = Misc.free(cursor);
        } finally {
            try {
                reader = Misc.free(reader);
            } finally {
                versions.close();
            }
        }
    }

    void appendVar(long row, MemoryMARW destination) {
        next(row);
        cursor.appendCoveredVar(requiredCover[0], destination);
    }

    void copyFixed(long row, long destination, int type) {
        next(row);
        int c = requiredCover[0];
        switch (ColumnType.pow2SizeOf(type)) {
            case 0 -> Unsafe.putByte(destination, cursor.getCoveredByte(c));
            case 1 -> Unsafe.putShort(destination, cursor.getCoveredShort(c));
            case 2 -> Unsafe.putInt(destination, ColumnType.tagOf(type) == ColumnType.FLOAT
                    ? Float.floatToRawIntBits(cursor.getCoveredFloat(c)) : cursor.getCoveredInt(c));
            case 3 -> Unsafe.putLong(destination, ColumnType.tagOf(type) == ColumnType.DOUBLE
                    ? Double.doubleToRawLongBits(cursor.getCoveredDouble(c)) : cursor.getCoveredLong(c));
            case 4 -> {
                Unsafe.putLong(destination, cursor.getCoveredLong128Lo(c));
                Unsafe.putLong(destination + Long.BYTES, cursor.getCoveredLong128Hi(c));
            }
            case 5 -> {
                Unsafe.putLong(destination, cursor.getCoveredLong256_0(c));
                Unsafe.putLong(destination + Long.BYTES, cursor.getCoveredLong256_1(c));
                Unsafe.putLong(destination + 2L * Long.BYTES, cursor.getCoveredLong256_2(c));
                Unsafe.putLong(destination + 3L * Long.BYTES, cursor.getCoveredLong256_3(c));
            }
            default -> throw CairoException.critical(0).put("unsupported fixed cover type");
        }
    }

    void ofKey(int keyInStride, int cover) {
        if (cursor != null && requiredCover[0] != cover) {
            // Sealing visits one cover at a time; do not retain decoded blocks for prior covers.
            cursor.closeCoveringResources();
        }
        cursor = Misc.free(cursor);
        requiredCover[0] = cover;
        cursor = (AbstractPostingIndexReader.AbstractCoveringCursor)
                reader.getCursor(strideStart + keyInStride, 0, Long.MAX_VALUE, requiredCover);
        if (!cursor.isCoveredAvailable(cover)) {
            throw CairoException.critical(0).put("missing sidecar during streaming seal [cover=").put(cover).put(']');
        }
    }

    void ofStride(int strideStart) {
        this.strideStart = strideStart;
    }

    private void next(long expectedRow) {
        if (!cursor.hasNext() || cursor.next() != expectedRow) {
            throw CairoException.critical(0).put("sidecar row mismatch during streaming seal [row=").put(expectedRow).put(']');
        }
    }
}
