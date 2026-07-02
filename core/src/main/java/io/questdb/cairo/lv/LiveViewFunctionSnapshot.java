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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.engine.window.WindowFunction;

/**
 * Framework-owned read/write of a window function's FUNCTION_SNAPSHOT payload.
 * The block prelude ({@code windowName}, {@code factoryName}, {@code formatVersion})
 * is written/consumed by the caller in {@link LiveViewRefreshJob}; this class owns
 * everything after it: the key-shape header, the partition count, and the
 * per-partition key + state iteration. Each window function contributes only ONE
 * partition's state via
 * {@link WindowFunction#snapshotPartitionState(MemoryA, MapValue)} /
 * {@link WindowFunction#restorePartitionState(MemoryR, long, MapValue, int)}.
 * <p>
 * Payload layout (mirrors the WINDOW_ANCHOR block's self-describing header so a
 * stored-vs-running key-shape mismatch is caught before any state byte is decoded):
 * <pre>
 *   partitionKeyColumnCount: INT
 *   per key column: columnType: INT
 *   partitionCount: LONG
 *   per partition:
 *     per key column: keyValue   (LiveViewSnapshotKeyCodec)
 *     state bytes                (function: one partition)
 * </pre>
 * Scalar (no-map) functions write {@code partitionKeyColumnCount=0},
 * {@code partitionCount=1}, then their single state record with a {@code null}
 * partition value.
 */
public final class LiveViewFunctionSnapshot {

    private LiveViewFunctionSnapshot() {
    }

    /**
     * Reads the key-shape header (validating it against the running function), then
     * rehydrates each partition. A header mismatch throws {@link CairoException}
     * with errno 0 (structural corruption that passed CRC) so the caller unlinks the
     * head checkpoint and head-miss-replays rather than invalidating the view -
     * distinct from a version-too-old break, which invalidates.
     *
     * @param source        positioned at the payload start (just past the prelude)
     * @param offset        byte offset within {@code source} of the payload start
     * @param f             the running function the stored block resolved to
     * @param formatVersion the per-function snapshot version recorded in the prelude
     */
    public static void restore(MemoryR source, long offset, WindowFunction f, int formatVersion) {
        final ColumnTypes keyTypes = f.getSnapshotKeyColumnTypes();
        final int expectedKeyColumnCount = keyTypes == null ? 0 : keyTypes.getColumnCount();
        final int storedKeyColumnCount = source.getInt(offset);
        offset += Integer.BYTES;
        if (storedKeyColumnCount != expectedKeyColumnCount) {
            throw CairoException.critical(0)
                    .put("live view function snapshot key column count mismatch [expected=")
                    .put(expectedKeyColumnCount)
                    .put(", got=")
                    .put(storedKeyColumnCount)
                    .put(']');
        }
        for (int i = 0; i < storedKeyColumnCount; i++) {
            final int storedType = source.getInt(offset);
            offset += Integer.BYTES;
            final int expectedType = keyTypes.getColumnType(i);
            if (storedType != expectedType) {
                throw CairoException.critical(0)
                        .put("live view function snapshot key column type mismatch [index=")
                        .put(i)
                        .put(", expected=")
                        .put(ColumnType.nameOf(expectedType))
                        .put(", got=")
                        .put(ColumnType.nameOf(storedType))
                        .put(']');
            }
        }
        final long partitionCount = source.getLong(offset);
        offset += Long.BYTES;

        f.onSnapshotRestoreBegin();

        final Map map = f.getPartitionMap();
        if (map == null) {
            // Scalar no-map function: a single keyless partition.
            f.restorePartitionState(source, offset, null, formatVersion);
            return;
        }
        for (long p = 0; p < partitionCount; p++) {
            final MapKey key = map.withKey();
            offset = LiveViewSnapshotKeyCodec.readKey(key, source, offset, keyTypes);
            final MapValue value = key.createValue();
            offset = f.restorePartitionState(source, offset, value, formatVersion);
        }
    }

    /**
     * Writes the key-shape header, the live partition count, and each live
     * partition's key + state. Tombstoned partitions are skipped. A live-count vs
     * emit-count disagreement throws errno 0 (mirrors the WINDOW_ANCHOR writer).
     *
     * @param sink the FUNCTION_SNAPSHOT block sink, positioned just past the prelude
     * @param f    the function whose per-partition state to serialise
     */
    public static void write(MemoryA sink, WindowFunction f) {
        final Map map = f.getPartitionMap();
        if (map == null) {
            // Scalar no-map function: a single keyless partition.
            sink.putInt(0);
            sink.putLong(1);
            f.snapshotPartitionState(sink, null);
            return;
        }

        final ColumnTypes keyTypes = f.getSnapshotKeyColumnTypes();
        final int keyColumnCount = keyTypes.getColumnCount();
        sink.putInt(keyColumnCount);
        for (int i = 0; i < keyColumnCount; i++) {
            sink.putInt(keyTypes.getColumnType(i));
        }

        final int tombstoneValueIndex = f.getTombstoneValueIndex();
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = map.getRecord();
        final long liveCount;
        if (tombstoneValueIndex < 0 || f.getTombstoneCount() == 0) {
            liveCount = map.size();
        } else {
            long count = 0;
            while (cursor.hasNext()) {
                if (record.getValue().getByte(tombstoneValueIndex) != 1) {
                    count++;
                }
            }
            liveCount = count;
            cursor.toTop();
        }
        sink.putLong(liveCount);

        final int keyStartIndex = f.getSnapshotKeyStartIndex();
        long emitted = 0;
        while (cursor.hasNext()) {
            final MapValue value = record.getValue();
            if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                continue;
            }
            LiveViewSnapshotKeyCodec.writeKey(sink, record, keyTypes, keyStartIndex);
            f.snapshotPartitionState(sink, value);
            emitted++;
        }
        if (emitted != liveCount) {
            throw CairoException.critical(0)
                    .put("live view function snapshot live-count mismatch [expected=")
                    .put(liveCount)
                    .put(", emitted=")
                    .put(emitted)
                    .put(']');
        }
    }
}
