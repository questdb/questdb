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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;

/**
 * Typed read/write of fixed-width column slots for live-view checkpoint
 * snapshot/restore. Each column is dispatched by {@link ColumnType#tagOf} to a
 * fixed-width primitive serialiser. Used for two distinct slot kinds:
 * <ul>
 *     <li>Partition-key columns of a window function's hash map - the
 *     classic key codec.</li>
 *     <li>Value-slot ranges in the same map (e.g. the rank function's
 *     chain-prefix that {@code RecordComparator} reads from
 *     {@link MapValue}), since {@link MapValue} extends {@link Record} and
 *     exposes the same {@code getXxx(columnIndex)} accessors.</li>
 * </ul>
 * Variable-width column types other than STRING (VARCHAR, BINARY, UUID,
 * LONG256, DECIMAL128, DECIMAL256, etc.) are not supported and force the
 * containing function or anchor map onto the head-miss path. STRING is
 * supported because a live-view partition-by RecordSink that does not translate
 * rewrites its SYMBOL partition columns as resolved STRING values (see
 * {@code LiveViewWindow.build} and the live-view path in
 * {@code SqlCodeGenerator.generateSelectWindow}), so such a view's
 * partition-key key types end up as STRING. Callers should gate
 * {@code supportsCheckpointState()} on {@link #isAllTypesSupported(ColumnTypes)}.
 * <p>
 * Format is type-dispatched and grows from {@code offset} byte-by-byte:
 * <pre>
 *     BYTE       / GEOBYTE  / BOOLEAN   - 1 byte (BOOLEAN as 0/1)
 *     SHORT      / GEOSHORT / CHAR      - 2 bytes
 *     INT        / IPv4     / GEOINT    - 4 bytes
 *     FLOAT                             - 4 bytes
 *     SYMBOL                            - 4 bytes, byte-reversed - see {@link #encodeSymbolKey}
 *     LONG       / DATE     / TIMESTAMP - 8 bytes
 *     GEOLONG    / DOUBLE               - 8 bytes
 *     STRING                            - 4 byte length prefix + 2 bytes per char
 * </pre>
 * No length prefix on the entry itself - callers know the slot shape from the
 * function's stored {@link ColumnTypes}.
 */
public final class LiveViewSnapshotKeyCodec {

    private LiveViewSnapshotKeyCodec() {
    }

    /**
     * @return total byte size of a key row across all columns in
     * {@code keyTypes}, or -1 if the type set has any unsupported column
     * (also returns -1 for keys that contain a STRING column — STRING keys
     * are encoded variable-width so the row size is per-value, not derivable
     * from the type set). Callers should check
     * {@link #isAllTypesSupported(ColumnTypes)} first.
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

    /**
     * Like {@link #isAllTypesSupported(ColumnTypes)} but without the STRING
     * exception. {@link MapValue} exposes no STRING slot setter, so value-slot
     * ranges restored via {@link #readValueSlots} must be fixed-width only;
     * callers that snapshot value slots (e.g. the rank function's
     * chain-prefix) must gate {@code supportsCheckpointState()} on this method, not
     * on {@link #isAllTypesSupported(ColumnTypes)}.
     */
    public static boolean isAllTypesFixedWidth(ColumnTypes types) {
        return byteSizeOf(types) >= 0;
    }

    /**
     * Decodes what {@link #encodeSymbolKey} wrote back into the id the map keys by.
     */
    public static int decodeSymbolKey(int encoded) {
        return Integer.reverseBytes(encoded);
    }

    /**
     * Encodes a SYMBOL id into the four bytes a persisted key carries.
     * <p>
     * Every other fixed-width slot goes out in the platform's own byte order, which is
     * little-endian everywhere QuestDB runs. A SYMBOL slot is byte-reversed instead, so
     * that its four bytes read big-endian and the unsigned byte comparison a checkpoint
     * partition map orders its pages by puts the ids in numeric order. That matters
     * because the dictionary behind a translated SYMBOL key hands ids out in first-seen
     * order: with the bytes reversed, a batch of new keys is an append at the right edge
     * of the tree, which is the cheapest shape a copy-on-write B+ tree has. Written in
     * the native order the low byte is the one that moves, so a run of sequential ids
     * scatters across every leaf and the seal rewrites the whole tree.
     * <p>
     * NULL is {@link io.questdb.cairo.sql.SymbolTable#VALUE_IS_NULL}, whose reversed
     * image sorts after every non-negative id. The order carries no meaning beyond
     * locality and determinism, so where NULL lands is free.
     * <p>
     * Only SYMBOL is reversed: an INT key's encoding is released format and stays as it
     * is. A SYMBOL key reaches a persisted map only where a live view translates its
     * partition term to an LV-private id, which no released build wrote.
     */
    public static int encodeSymbolKey(int id) {
        return Integer.reverseBytes(id);
    }

    public static boolean isAllTypesSupported(ColumnTypes keyTypes) {
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            if (!isSupportedKeyType(keyTypes.getColumnType(i))) {
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
                case ColumnType.GEOBYTE:
                    dst.putByte(source.getByte(offset));
                    offset += Byte.BYTES;
                    break;
                case ColumnType.BOOLEAN:
                    dst.putBool(source.getByte(offset) != 0);
                    offset += Byte.BYTES;
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    dst.putShort(source.getShort(offset));
                    offset += Short.BYTES;
                    break;
                case ColumnType.CHAR:
                    dst.putChar(source.getChar(offset));
                    offset += Character.BYTES;
                    break;
                case ColumnType.SYMBOL:
                    dst.putInt(decodeSymbolKey(source.getInt(offset)));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.INT:
                case ColumnType.IPv4:
                case ColumnType.GEOINT:
                    dst.putInt(source.getInt(offset));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.FLOAT:
                    dst.putFloat(source.getFloat(offset));
                    offset += Float.BYTES;
                    break;
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.GEOLONG:
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
                case ColumnType.STRING:
                    final int strLen = source.getInt(offset);
                    offset += Integer.BYTES;
                    if (strLen < 0) {
                        dst.putStr(null);
                    } else {
                        dst.putStr(source.getStrA(offset - Integer.BYTES));
                        offset += (long) strLen * Character.BYTES;
                    }
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
        return offset;
    }

    /**
     * Page-bounded counterpart used by the versioned checkpoint store.
     */
    public static long readKey(MapKey dst, LiveViewStatePageReader source, long offset, ColumnTypes keyTypes) {
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int type = ColumnType.tagOf(keyTypes.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    dst.putByte(source.getByte(offset));
                    offset += Byte.BYTES;
                    break;
                case ColumnType.BOOLEAN:
                    dst.putBool(source.getByte(offset) != 0);
                    offset += Byte.BYTES;
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    dst.putShort(source.getShort(offset));
                    offset += Short.BYTES;
                    break;
                case ColumnType.CHAR:
                    dst.putChar((char) source.getShort(offset));
                    offset += Character.BYTES;
                    break;
                case ColumnType.SYMBOL:
                    dst.putInt(decodeSymbolKey(source.getInt(offset)));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.INT:
                case ColumnType.IPv4:
                case ColumnType.GEOINT:
                    dst.putInt(source.getInt(offset));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.FLOAT:
                    dst.putFloat(Float.intBitsToFloat(source.getInt(offset)));
                    offset += Float.BYTES;
                    break;
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.GEOLONG:
                    dst.putLong(source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DATE:
                    dst.putDate(source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DOUBLE:
                    dst.putDouble(Double.longBitsToDouble(source.getLong(offset)));
                    offset += Double.BYTES;
                    break;
                case ColumnType.STRING:
                    final int strLen = source.getInt(offset);
                    final CharSequence value = source.getStrA(offset);
                    dst.putStr(value);
                    offset += Integer.BYTES + (strLen < 0 ? 0L : (long) strLen * Character.BYTES);
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
        return offset;
    }

    /**
     * Structurally decodes one bounded key without inserting it into a map.
     * Returns the first byte after the key.
     */
    public static long validateKey(LiveViewStatePageReader source, long offset, ColumnTypes keyTypes) {
        for (int i = 0, n = keyTypes.getColumnCount(); i < n; i++) {
            final int type = ColumnType.tagOf(keyTypes.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    source.getByte(offset);
                    offset += Byte.BYTES;
                    break;
                case ColumnType.BOOLEAN:
                    final byte bool = source.getByte(offset);
                    if (bool != 0 && bool != 1) {
                        throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                .put("live view checkpoint boolean key value invalid, value=").put(bool);
                    }
                    offset += Byte.BYTES;
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                case ColumnType.CHAR:
                    source.getShort(offset);
                    offset += Short.BYTES;
                    break;
                case ColumnType.INT:
                case ColumnType.SYMBOL:
                case ColumnType.IPv4:
                case ColumnType.GEOINT:
                case ColumnType.FLOAT:
                    source.getInt(offset);
                    offset += Integer.BYTES;
                    break;
                case ColumnType.LONG:
                case ColumnType.TIMESTAMP:
                case ColumnType.DATE:
                case ColumnType.GEOLONG:
                case ColumnType.DOUBLE:
                    source.getLong(offset);
                    offset += Long.BYTES;
                    break;
                case ColumnType.STRING:
                    final int strLen = source.getInt(offset);
                    source.getStrA(offset);
                    offset += Integer.BYTES + (strLen < 0 ? 0L : (long) strLen * Character.BYTES);
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
        return offset;
    }

    /**
     * Reads one slot row (all {@code slotTypes.getColumnCount()} columns) from
     * {@code source} starting at {@code offset} and pushes the typed values into
     * {@code dst} at slot indexes {@code [slotStartIndex, slotStartIndex +
     * slotTypes.getColumnCount())}. Returns the new offset just past the
     * consumed bytes.
     * <p>
     * Mirrors {@link #readKey} but writes into {@link MapValue}'s indexed slot
     * accessors. Used to restore a window function's value-side prefix (e.g.
     * the rank function's chain-prefix bytes that
     * {@link io.questdb.griffin.engine.RecordComparator} reads back).
     * <p>
     * Unlike {@link #readKey}, STRING is not supported here: {@link MapValue}
     * has no STRING slot setter. Callers must reject STRING slot types up
     * front via {@link #isAllTypesFixedWidth(ColumnTypes)} — otherwise
     * {@link #writeKey} would happily serialise the STRING slot and this
     * method would fail the restore.
     */
    public static long readValueSlots(MapValue dst, int slotStartIndex, MemoryR source, long offset, ColumnTypes slotTypes) {
        for (int i = 0, n = slotTypes.getColumnCount(); i < n; i++) {
            final int slotIndex = slotStartIndex + i;
            final int type = ColumnType.tagOf(slotTypes.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    dst.putByte(slotIndex, source.getByte(offset));
                    offset += Byte.BYTES;
                    break;
                case ColumnType.BOOLEAN:
                    dst.putBool(slotIndex, source.getByte(offset) != 0);
                    offset += Byte.BYTES;
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    dst.putShort(slotIndex, source.getShort(offset));
                    offset += Short.BYTES;
                    break;
                case ColumnType.CHAR:
                    dst.putChar(slotIndex, source.getChar(offset));
                    offset += Character.BYTES;
                    break;
                case ColumnType.SYMBOL:
                    dst.putInt(slotIndex, decodeSymbolKey(source.getInt(offset)));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.INT:
                case ColumnType.IPv4:
                case ColumnType.GEOINT:
                    dst.putInt(slotIndex, source.getInt(offset));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.FLOAT:
                    dst.putFloat(slotIndex, source.getFloat(offset));
                    offset += Float.BYTES;
                    break;
                case ColumnType.LONG:
                case ColumnType.GEOLONG:
                    dst.putLong(slotIndex, source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.TIMESTAMP:
                    dst.putTimestamp(slotIndex, source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DATE:
                    dst.putDate(slotIndex, source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DOUBLE:
                    dst.putDouble(slotIndex, source.getDouble(offset));
                    offset += Double.BYTES;
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
        return offset;
    }

    /**
     * Writes one slot row from {@code record} (which exposes its columns at
     * indexes {@code [startIndex, startIndex + types.getColumnCount())}) into
     * {@code sink}, in the same byte order {@link #readKey} consumes.
     * <p>
     * Hash {@link io.questdb.cairo.map.Map} implementations
     * ({@link io.questdb.cairo.map.Unordered4Map},
     * {@link io.questdb.cairo.map.Unordered8Map}, {@code OrderedMap}, etc.) lay
     * out their records as {@code [value0, ..., valueN, key0, ..., keyM]}, so:
     * <ul>
     *     <li>Iterating the Map's cursor as a {@link io.questdb.cairo.map.MapRecord}
     *     and writing the partition key: pass {@code valueCount} as
     *     {@code startIndex} and the partition-key {@code ColumnTypes}.</li>
     *     <li>Writing a value-slot range (e.g. the rank function's chain-prefix)
     *     from a {@link MapValue} (which extends {@link Record}): pass the
     *     slot's start index and the slot {@code ColumnTypes}.</li>
     * </ul>
     */
    public static void writeKey(MemoryA sink, Record record, ColumnTypes types, int startIndex) {
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            final int columnIndex = startIndex + i;
            final int type = ColumnType.tagOf(types.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    sink.putByte(record.getByte(columnIndex));
                    break;
                case ColumnType.BOOLEAN:
                    sink.putByte((byte) (record.getBool(columnIndex) ? 1 : 0));
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    sink.putShort(record.getShort(columnIndex));
                    break;
                case ColumnType.CHAR:
                    sink.putChar(record.getChar(columnIndex));
                    break;
                case ColumnType.SYMBOL:
                    sink.putInt(encodeSymbolKey(record.getInt(columnIndex)));
                    break;
                case ColumnType.INT:
                    sink.putInt(record.getInt(columnIndex));
                    break;
                case ColumnType.IPv4:
                    sink.putInt(record.getIPv4(columnIndex));
                    break;
                case ColumnType.GEOINT:
                    sink.putInt(record.getGeoInt(columnIndex));
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
                case ColumnType.GEOLONG:
                    sink.putLong(record.getGeoLong(columnIndex));
                    break;
                case ColumnType.DOUBLE:
                    sink.putDouble(record.getDouble(columnIndex));
                    break;
                case ColumnType.STRING:
                    sink.putStr(record.getStrA(columnIndex));
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
    }

    public static long readValueSlots(
            MapValue dst,
            int slotStartIndex,
            LiveViewStatePageReader source,
            long offset,
            ColumnTypes slotTypes
    ) {
        for (int i = 0, n = slotTypes.getColumnCount(); i < n; i++) {
            final int slotIndex = slotStartIndex + i;
            final int type = ColumnType.tagOf(slotTypes.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    dst.putByte(slotIndex, source.getByte(offset));
                    offset += Byte.BYTES;
                    break;
                case ColumnType.BOOLEAN:
                    dst.putBool(slotIndex, source.getByte(offset) != 0);
                    offset += Byte.BYTES;
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    dst.putShort(slotIndex, source.getShort(offset));
                    offset += Short.BYTES;
                    break;
                case ColumnType.CHAR:
                    dst.putChar(slotIndex, (char) source.getShort(offset));
                    offset += Character.BYTES;
                    break;
                case ColumnType.SYMBOL:
                    dst.putInt(slotIndex, decodeSymbolKey(source.getInt(offset)));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.INT:
                case ColumnType.IPv4:
                case ColumnType.GEOINT:
                    dst.putInt(slotIndex, source.getInt(offset));
                    offset += Integer.BYTES;
                    break;
                case ColumnType.FLOAT:
                    dst.putFloat(slotIndex, Float.intBitsToFloat(source.getInt(offset)));
                    offset += Float.BYTES;
                    break;
                case ColumnType.LONG:
                case ColumnType.GEOLONG:
                    dst.putLong(slotIndex, source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.TIMESTAMP:
                    dst.putTimestamp(slotIndex, source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DATE:
                    dst.putDate(slotIndex, source.getLong(offset));
                    offset += Long.BYTES;
                    break;
                case ColumnType.DOUBLE:
                    dst.putDouble(slotIndex, source.getDouble(offset));
                    offset += Double.BYTES;
                    break;
                default:
                    throw unsupportedType(type);
            }
        }
        return offset;
    }

    /**
     * Page-aware fixed-width value-slot writer used by ranking state.
     */
    public static void writeKey(LiveViewStatePageWriter sink, Record record, ColumnTypes types, int startIndex) {
        for (int i = 0, n = types.getColumnCount(); i < n; i++) {
            final int columnIndex = startIndex + i;
            final int type = ColumnType.tagOf(types.getColumnType(i));
            switch (type) {
                case ColumnType.BYTE:
                case ColumnType.GEOBYTE:
                    sink.putByte(record.getByte(columnIndex));
                    break;
                case ColumnType.BOOLEAN:
                    sink.putByte((byte) (record.getBool(columnIndex) ? 1 : 0));
                    break;
                case ColumnType.SHORT:
                case ColumnType.GEOSHORT:
                    sink.putShort(record.getShort(columnIndex));
                    break;
                case ColumnType.CHAR:
                    sink.putShort((short) record.getChar(columnIndex));
                    break;
                case ColumnType.SYMBOL:
                    sink.putInt(encodeSymbolKey(record.getInt(columnIndex)));
                    break;
                case ColumnType.INT:
                    sink.putInt(record.getInt(columnIndex));
                    break;
                case ColumnType.IPv4:
                    sink.putInt(record.getIPv4(columnIndex));
                    break;
                case ColumnType.GEOINT:
                    sink.putInt(record.getGeoInt(columnIndex));
                    break;
                case ColumnType.FLOAT:
                    sink.putInt(Float.floatToRawIntBits(record.getFloat(columnIndex)));
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
                case ColumnType.GEOLONG:
                    sink.putLong(record.getGeoLong(columnIndex));
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
            case ColumnType.GEOBYTE:
                return Byte.BYTES;
            case ColumnType.SHORT:
            case ColumnType.CHAR:
            case ColumnType.GEOSHORT:
                return Short.BYTES;
            case ColumnType.INT:
            case ColumnType.SYMBOL:
            case ColumnType.IPv4:
            case ColumnType.GEOINT:
            case ColumnType.FLOAT:
                return Integer.BYTES;
            case ColumnType.LONG:
            case ColumnType.TIMESTAMP:
            case ColumnType.DATE:
            case ColumnType.GEOLONG:
            case ColumnType.DOUBLE:
                return Long.BYTES;
            default:
                return -1;
        }
    }

    /**
     * Returns {@code true} for column types this codec can read and write at
     * the partition-key slot. Mirrors {@link #byteSizeOfType} for fixed-width
     * types, with STRING admitted as a variable-width exception (the live-view
     * partition-by RecordSink rewrites SYMBOL columns as resolved STRING so
     * SYMBOL-partitioned LVs ride STRING keys end-to-end).
     */
    private static boolean isSupportedKeyType(int columnType) {
        if (ColumnType.tagOf(columnType) == ColumnType.STRING) {
            return true;
        }
        return byteSizeOfType(columnType) >= 0;
    }

    private static CairoException unsupportedType(int type) {
        return CairoException.nonCritical()
                .put("live view snapshot codec does not support key column type: ")
                .put(ColumnType.nameOf(type));
    }
}
