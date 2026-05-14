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
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;

/**
 * Typed read/write of partition-key columns for live-view checkpoint
 * snapshot/restore. Each key column is dispatched by {@link ColumnType#tagOf}
 * to a fixed-width primitive serialiser; variable-width key types
 * (STRING, VARCHAR, UUID, etc.) are not supported in Phase 2a and force the
 * containing function or anchor map onto the head-miss path. Callers should
 * gate {@code supportsSnapshot()} on {@link #isAllTypesSupported(ColumnTypes)}.
 * <p>
 * Format is type-dispatched and grows from {@code offset} byte-by-byte:
 * <pre>
 *     BYTE        - 1 byte
 *     BOOLEAN     - 1 byte (0/1)
 *     SHORT       - 2 bytes
 *     CHAR        - 2 bytes
 *     INT         - 4 bytes
 *     SYMBOL      - 4 bytes (the int id, not the resolved string)
 *     FLOAT       - 4 bytes
 *     LONG        - 8 bytes
 *     TIMESTAMP   - 8 bytes
 *     DATE        - 8 bytes
 *     DOUBLE      - 8 bytes
 * </pre>
 * No length prefix on the entry itself - callers know the key shape from the
 * function's stored {@link ColumnTypes}.
 */
public final class LiveViewSnapshotKeyCodec {

    private LiveViewSnapshotKeyCodec() {
    }

    /**
     * @return total byte size of a key row across all columns in
     * {@code keyTypes}, or -1 if any column type is not supported (callers
     * should check {@link #isAllTypesSupported(ColumnTypes)} first).
     */
    public static int byteSizeOf(ColumnTypes keyTypes) {
        int total = 0;
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int slotSize = byteSizeOfType(keyTypes.getColumnType(i));
            if (slotSize < 0) {
                return -1;
            }
            total += slotSize;
        }
        return total;
    }

    public static boolean isAllTypesSupported(ColumnTypes keyTypes) {
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            if (byteSizeOfType(keyTypes.getColumnType(i)) < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Reads one key row (all {@code keyTypes.getColumnCount()} columns) from
     * {@code source} starting at {@code offset} and pushes the typed values
     * into {@code dst} via {@link MapKey} putXxx accessors. Returns the new
     * offset just past the consumed bytes.
     */
    public static long readKey(MapKey dst, MemoryR source, long offset, ColumnTypes keyTypes) {
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int type = ColumnType.tagOf(keyTypes.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                    dst.putByte(source.getByte(offset));
                    offset += Byte.BYTES;
                    break;
                case ColumnType.BOOLEAN:
                    dst.putBool(source.getByte(offset) != 0);
                    offset += Byte.BYTES;
                    break;
                case ColumnType.SHORT:
                    dst.putShort(source.getShort(offset));
                    offset += Short.BYTES;
                    break;
                case ColumnType.CHAR:
                    dst.putChar(source.getChar(offset));
                    offset += Character.BYTES;
                    break;
                case ColumnType.INT:
                    dst.putInt(source.getInt(offset));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.SYMBOL:
                    dst.putInt(source.getInt(offset));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.FLOAT:
                    dst.putFloat(source.getFloat(offset));
                    offset += Float.BYTES;
                    break;
                case ColumnType.LONG:
                    dst.putLong(source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.TIMESTAMP:
                    dst.putLong(source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DATE:
                    dst.putDate(source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DOUBLE:
                    dst.putDouble(source.getDouble(offset));
                    offset += Double.BYTES;
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
        return offset;
    }

    /**
     * Writes one key row from {@code record} (which exposes its key columns at
     * indexes {@code [keyStartIndex, keyStartIndex + keyTypes.getColumnCount())})
     * into {@code sink}, in the same byte order {@link #readKey} consumes.
     * <p>
     * Hash {@link io.questdb.cairo.map.Map} implementations
     * ({@link io.questdb.cairo.map.Unordered4Map},
     * {@link io.questdb.cairo.map.Unordered8Map}, {@code OrderedMap}, etc.) lay
     * out their records as {@code [value0, ..., valueN, key0, ..., keyM]}, so
     * the caller passes {@code valueCount} as {@code keyStartIndex} when
     * iterating the Map's cursor.
     */
    public static void writeKey(MemoryA sink, Record record, ColumnTypes keyTypes, int keyStartIndex) {
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int columnIndex = keyStartIndex + i;
            final int type = ColumnType.tagOf(keyTypes.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                    sink.putByte(record.getByte(columnIndex));
                    break;
                case ColumnType.BOOLEAN:
                    sink.putByte((byte) (record.getBool(columnIndex) ? 1 : 0));
                    break;
                case ColumnType.SHORT:
                    sink.putShort(record.getShort(columnIndex));
                    break;
                case ColumnType.CHAR:
                    sink.putChar(record.getChar(columnIndex));
                    break;
                case ColumnType.INT:
                    sink.putInt(record.getInt(columnIndex));
                    break;
                case ColumnType.SYMBOL:
                    sink.putInt(record.getInt(columnIndex));
                    break;
                case ColumnType.FLOAT:
                    sink.putFloat(record.getFloat(columnIndex));
                    break;
                case ColumnType.LONG:
                    sink.putLong(record.getLong(columnIndex));
                    break;
                case ColumnType.TIMESTAMP:
                    sink.putLong(record.getTimestamp(columnIndex));
                    break;
                case ColumnType.DATE:
                    sink.putLong(record.getDate(columnIndex));
                    break;
                case ColumnType.DOUBLE:
                    sink.putDouble(record.getDouble(columnIndex));
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
    }

    private static int byteSizeOfType(int columnType) {
        switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BYTE:
            case ColumnType.BOOLEAN:
                return Byte.BYTES;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
                return Short.BYTES;
            case ColumnType.INT:
            case ColumnType.SYMBOL:
            case ColumnType.FLOAT:
                return Integer.BYTES;
            case ColumnType.LONG:
            case ColumnType.TIMESTAMP:
            case ColumnType.DATE:
            case ColumnType.DOUBLE:
                return Long.BYTES;
            default:
                return -1;
        }
    }

    private static CairoException unsupportedType(int type) {
        return CairoException.nonCritical()
                .put("live view snapshot codec does not support key column type: ")
                .put(ColumnType.nameOf(type));
    }
}
