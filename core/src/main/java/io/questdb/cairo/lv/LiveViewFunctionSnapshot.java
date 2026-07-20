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
 * {@link WindowFunction#freezeCheckpointState(LiveViewStatePageWriter, MapValue)} /
 * {@link WindowFunction#restoreCheckpointState(LiveViewStatePageReader, long, MapValue, int)}.
 * <p>
 * Payload layout (mirrors the WINDOW_ANCHOR block's self-describing header so a
 * stored-vs-running key-shape mismatch is caught before any state byte is decoded):
 * <pre>
 *   partitionKeyColumnCount: INT
 *   per key column: columnType: INT
 *   partitionCount: LONG
 *   per partition:
 *     per key column: keyValue   (LiveViewSnapshotKeyCodec)
 *     statePageLength: LONG
 *     statePageBytes            (function: one partition, exact length)
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
     * distinct from a version break (a recorded version outside the function's
     * supported range, in either direction), which invalidates.
     * <p>
     * After the last partition the consumed byte count must reconcile with
     * {@code payloadLength} - the restore-side mirror of the writer's
     * emitted-vs-live-count check. A function-level offset drift (a
     * restoreCheckpointState that reads more or fewer bytes than its writer
     * emitted) would otherwise silently decode the next partition or block
     * from the wrong offset.
     *
     * @param source        read-only memory containing the payload
     * @param offset        byte offset within {@code source} of the payload start
     * @param payloadLength exact byte length of the payload; restore must consume
     *                      all of it
     * @param f             the running function the stored block resolved to
     * @param formatVersion the per-function snapshot version recorded in the prelude
     */
    public static void restore(MemoryR source, long offset, long payloadLength, WindowFunction f, int formatVersion) {
        if (offset < 0 || payloadLength < 0 || offset > source.size() || payloadLength > source.size() - offset) {
            throw CairoException.critical(0)
                    .put("live view function checkpoint payload out of bounds")
                    .put(" [offset=").put(offset)
                    .put(", length=").put(payloadLength)
                    .put(", sourceSize=").put(source.size()).put(']');
        }
        final long payloadStart = offset;
        final long payloadEnd = payloadStart + payloadLength;
        final ColumnTypes keyTypes = f.getCheckpointKeyColumnTypes();
        final int expectedKeyColumnCount = keyTypes == null ? 0 : keyTypes.getColumnCount();
        ensureAvailable(offset, Integer.BYTES, payloadEnd, "key column count");
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
            ensureAvailable(offset, Integer.BYTES, payloadEnd, "key column type");
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
        ensureAvailable(offset, Long.BYTES, payloadEnd, "partition count");
        final long partitionCount = source.getLong(offset);
        offset += Long.BYTES;
        // Reject a negative count BEFORE any state mutation: the map path would clear state
        // then zero-iterate, and a header-only payload crafted to match payloadLength would
        // pass the final length check - silently restoring empty state from a corrupt (but
        // CRC-valid) checkpoint. Guard before onCheckpointRestoreBegin so the running state is
        // untouched on rejection.
        if (partitionCount < 0) {
            throw CairoException.critical(0)
                    .put("live view function snapshot negative partition count [count=")
                    .put(partitionCount)
                    .put(']');
        }
        // Reject a count that cannot fit in the remaining payload BEFORE onCheckpointRestoreBegin
        // mutates state: each entry consumes at least one byte, so a crafted (CRC-valid) count
        // larger than the bytes left would otherwise drive an out-of-bounds / long-running read
        // that only the final length check catches - after the running state was already wiped.
        final long remainingBytes = payloadLength - (offset - payloadStart);
        if (remainingBytes < 0 || partitionCount > remainingBytes / Long.BYTES) {
            throw CairoException.critical(0)
                    .put("live view function snapshot partition count exceeds payload [count=")
                    .put(partitionCount)
                    .put(", remainingBytes=")
                    .put(remainingBytes)
                    .put(']');
        }

        final Map map = f.getPartitionMap();
        if (map == null) {
            // Scalar no-map function: the writer always emits exactly one keyless partition
            // (see write()), so anything else is corruption the count-agnostic restore below
            // would otherwise ignore.
            if (partitionCount != 1) {
                throw CairoException.critical(0)
                        .put("live view function snapshot scalar partition count must be 1 [count=")
                        .put(partitionCount)
                        .put(']');
            }
        }

        // Validate all framework-owned framing before clearing or otherwise mutating the
        // running function. The decoder itself remains bounded independently below.
        validateEntries(source, offset, payloadEnd, map == null ? null : keyTypes, partitionCount);

        final LiveViewStatePageReader pageReader = new LiveViewStatePageReader();
        f.onCheckpointRestoreBegin();
        if (map == null) {
            offset = restoreStatePage(pageReader, source, offset, payloadEnd, f, null, formatVersion);
        } else {
            for (long p = 0; p < partitionCount; p++) {
                final MapKey key = map.withKey();
                offset = LiveViewSnapshotKeyCodec.readKey(key, source, offset, keyTypes);
                final MapValue value = key.createValue();
                offset = restoreStatePage(pageReader, source, offset, payloadEnd, f, value, formatVersion);
            }
        }
        final long consumed = offset - payloadStart;
        if (consumed != payloadLength) {
            throw CairoException.critical(0)
                    .put("live view function snapshot payload length mismatch [expected=")
                    .put(payloadLength)
                    .put(", consumed=")
                    .put(consumed)
                    .put(']');
        }
    }

    private static void ensureAvailable(long offset, long bytes, long limit, CharSequence field) {
        if (offset < 0 || bytes < 0 || offset > limit || bytes > limit - offset) {
            throw CairoException.critical(0)
                    .put("live view function checkpoint ").put(field).put(" exceeds payload")
                    .put(" [offset=").put(offset)
                    .put(", read=").put(bytes)
                    .put(", limit=").put(limit).put(']');
        }
    }

    private static void validateEntries(
            MemoryR source,
            long offset,
            long payloadEnd,
            ColumnTypes keyTypes,
            long partitionCount
    ) {
        for (long p = 0; p < partitionCount; p++) {
            if (keyTypes != null) {
                offset = validateKey(source, offset, payloadEnd, keyTypes);
            }
            ensureAvailable(offset, Long.BYTES, payloadEnd, "state page length");
            final long pageLength = source.getLong(offset);
            offset += Long.BYTES;
            if (pageLength < 0 || pageLength > LiveViewStatePageWriter.MAX_PAGE_SIZE) {
                throw CairoException.critical(0)
                        .put("live view function checkpoint state page length invalid, length=")
                        .put(pageLength);
            }
            ensureAvailable(offset, pageLength, payloadEnd, "state page");
            offset += pageLength;
        }
        if (offset != payloadEnd) {
            throw CairoException.critical(0)
                    .put("live view function snapshot payload length mismatch [expectedEnd=")
                    .put(payloadEnd)
                    .put(", actualEnd=")
                    .put(offset)
                    .put(']');
        }
    }

    private static long restoreStatePage(
            LiveViewStatePageReader pageReader,
            MemoryR source,
            long offset,
            long payloadEnd,
            WindowFunction function,
            MapValue value,
            int formatVersion
    ) {
        ensureAvailable(offset, Long.BYTES, payloadEnd, "state page length");
        final long pageLength = source.getLong(offset);
        offset += Long.BYTES;
        if (pageLength < 0 || pageLength > LiveViewStatePageWriter.MAX_PAGE_SIZE) {
            throw CairoException.critical(0)
                    .put("live view function checkpoint state page length invalid, length=")
                    .put(pageLength);
        }
        ensureAvailable(offset, pageLength, payloadEnd, "state page");
        pageReader.of(source, offset, pageLength);
        final long consumed = function.restoreCheckpointState(pageReader, 0, value, formatVersion);
        if (consumed != pageLength) {
            throw CairoException.critical(0)
                    .put("live view function checkpoint state page length mismatch")
                    .put(" [expected=").put(pageLength)
                    .put(", consumed=").put(consumed).put(']');
        }
        return offset + pageLength;
    }

    private static long validateKey(MemoryR source, long offset, long payloadEnd, ColumnTypes keyTypes) {
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int type = ColumnType.tagOf(keyTypes.getColumnType(i));
            final int bytes;
            switch (type) {
                case ColumnType.BYTE:
                case ColumnType.BOOLEAN:
                case ColumnType.GEOBYTE:
                    bytes = Byte.BYTES;
                    break;
                case ColumnType.SHORT:
                case ColumnType.CHAR:
                case ColumnType.GEOSHORT:
                    bytes = Short.BYTES;
                    break;
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                case ColumnType.IPv4:
                case ColumnType.GEOINT:
                case ColumnType.FLOAT:
                    bytes = Integer.BYTES;
                    break;
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                case ColumnType.GEOLONG:
                case ColumnType.DOUBLE:
                    bytes = Long.BYTES;
                    break;
                case ColumnType.STRING:
                    ensureAvailable(offset, Integer.BYTES, payloadEnd, "string key length");
                    final int strLen = source.getInt(offset);
                    offset += Integer.BYTES;
                    if (strLen >= 0) {
                        final long stringBytes = (long) strLen * Character.BYTES;
                        ensureAvailable(offset, stringBytes, payloadEnd, "string key");
                        offset += stringBytes;
                    }
                    continue;
                default:
                    throw CairoException.critical(0)
                            .put("live view function checkpoint key type unsupported, type=")
                            .put(ColumnType.nameOf(type));
            }
            ensureAvailable(offset, bytes, payloadEnd, "key");
            offset += bytes;
        }
        return offset;
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
        final LiveViewStatePageWriter pageWriter = new LiveViewStatePageWriter();
        final Map map = f.getPartitionMap();
        if (map == null) {
            // Scalar no-map function: a single keyless partition.
            sink.putInt(0);
            sink.putLong(1);
            writeStatePage(pageWriter, sink, f, null);
            return;
        }

        final ColumnTypes keyTypes = f.getCheckpointKeyColumnTypes();
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

        final int keyStartIndex = f.getCheckpointKeyStartIndex();
        long emitted = 0;
        while (cursor.hasNext()) {
            final MapValue value = record.getValue();
            if (tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) == 1) {
                continue;
            }
            LiveViewSnapshotKeyCodec.writeKey(sink, record, keyTypes, keyStartIndex);
            writeStatePage(pageWriter, sink, f, value);
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

    private static void writeStatePage(
            LiveViewStatePageWriter pageWriter,
            MemoryA sink,
            WindowFunction function,
            MapValue value
    ) {
        final long lengthOffset = sink.getAppendOffset();
        sink.putLong(0);
        pageWriter.of(sink);
        function.freezeCheckpointState(pageWriter, value);
        final long pageLength = pageWriter.size();
        final long appendOffset = sink.getAppendOffset();
        sink.jumpTo(lengthOffset);
        sink.putLong(pageLength);
        sink.jumpTo(appendOffset);
    }
}
