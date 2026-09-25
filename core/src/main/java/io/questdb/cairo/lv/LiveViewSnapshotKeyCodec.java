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
import io.questdb.cairo.ColumnTypeTag;
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
 * supported because live-view partition-by RecordSinks rewrite SYMBOL
 * partition columns as resolved STRING values (see {@code LiveViewWindow.build}
 * and the live-view path in {@code SqlCodeGenerator.generateSelectWindow}),
 * so the live-view partition-key key types end up as STRING for any LV that
 * partitions by SYMBOL. Callers should gate {@code supportsCheckpointState()} on
 * {@link #isAllTypesSupported(ColumnTypes)}.
 * <p>
 * Format is type-dispatched and grows from {@code offset} byte-by-byte:
 * <pre>
 *     BYTE       / GEOBYTE  / BOOLEAN   - 1 byte (BOOLEAN as 0/1)
 *     SHORT      / GEOSHORT / CHAR      - 2 bytes
 *     INT        / SYMBOL   / IPv4      - 4 bytes (SYMBOL is the int id, not the resolved string)
 *     GEOINT     / FLOAT                - 4 bytes
 *     LONG       / DATE     / TIMESTAMP - 8 bytes
 *     GEOLONG    / DOUBLE               - 8 bytes
 *     STRING                            - 4 byte length prefix + 2 bytes per char
 * </pre>
 * No length prefix on the entry itself - callers know the slot shape from the
 * function's stored {@link ColumnTypes}.
 * <p>
 * {@link #byteSizeOfType} is the one relation behind the type gates
 * ({@link #isAllTypesSupported}, {@link #isAllTypesFixedWidth},
 * {@link LiveViewFunctionSnapshot}'s key validation): a column reaches the
 * per-row switches below only after a gate admitted its type, so their
 * {@code default} arms are tripwires for an ungated caller.
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
                case ColumnType.INT:
                case ColumnType.SYMBOL:
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
                case ColumnType.INT:
                case ColumnType.SYMBOL:
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
                case ColumnType.INT:
                case ColumnType.SYMBOL:
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
                    sink.putByte(record.getByte(columnIndex));
                    break;
                case ColumnType.GEOBYTE:
                    // getByte, not getGeoByte: every Map record and MapValue that reaches this
                    // codec implements getGeoByte as getByte, so the two read the same slot.
                    sink.putByte(record.getByte(columnIndex));
                    break;
                case ColumnType.BOOLEAN:
                    sink.putByte((byte) (record.getBool(columnIndex) ? 1 : 0));
                    break;
                case ColumnType.SHORT:
                    sink.putShort(record.getShort(columnIndex));
                    break;
                case ColumnType.GEOSHORT:
                    // getShort, not getGeoShort; see GEOBYTE above.
                    sink.putShort(record.getShort(columnIndex));
                    break;
                case ColumnType.CHAR:
                    sink.putChar(record.getChar(columnIndex));
                    break;
                case ColumnType.INT:
                case ColumnType.SYMBOL:
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
                case ColumnType.INT:
                case ColumnType.SYMBOL:
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
                    sink.putByte(record.getByte(columnIndex));
                    break;
                case ColumnType.GEOBYTE:
                    // getByte, not getGeoByte; see writeKey(MemoryA, ...).
                    sink.putByte(record.getByte(columnIndex));
                    break;
                case ColumnType.BOOLEAN:
                    sink.putByte((byte) (record.getBool(columnIndex) ? 1 : 0));
                    break;
                case ColumnType.SHORT:
                    sink.putShort(record.getShort(columnIndex));
                    break;
                case ColumnType.GEOSHORT:
                    // getShort, not getGeoShort; see writeKey(MemoryA, ...).
                    sink.putShort(record.getShort(columnIndex));
                    break;
                case ColumnType.CHAR:
                    sink.putShort((short) record.getChar(columnIndex));
                    break;
                case ColumnType.INT:
                case ColumnType.SYMBOL:
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

    /**
     * The wire width of a fixed-width codec slot, or -1 when the codec has no fixed-width arm
     * for the type. This is the relation the type gates read; the per-row switches above have
     * an arm for exactly the types it sizes, plus STRING. STRING is -1 here because its slot
     * is variable-width; {@link #isSupportedKeyType} admits it separately. The other -1 types
     * are either not column types or fixed-width types without a codec arm (LONG256, UUID,
     * LONG128, the DECIMALs, INTERVAL): a function or anchor map that keys on one of them
     * takes the head-miss path instead of a checkpoint.
     */
    static int byteSizeOfType(int columnType) {
        return switch (ColumnTypeTag.of(columnType)) {
            case BYTE, BOOLEAN, GEOBYTE -> Byte.BYTES;
            case SHORT, CHAR, GEOSHORT -> Short.BYTES;
            case INT, SYMBOL, IPv4, GEOINT, FLOAT -> Integer.BYTES;
            case LONG, TIMESTAMP, DATE, GEOLONG, DOUBLE -> Long.BYTES;
            case UNDEFINED, STRING, LONG256, BINARY, UUID, CURSOR, VAR_ARG, RECORD, GEOHASH, LONG128, VARCHAR, ARRAY,
                 DECIMAL8, DECIMAL16, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256, DECIMAL, REGCLASS, REGPROCEDURE,
                 ARRAY_STRING, PARAMETER, INTERVAL, VARCHAR_SLICE, NULL, UNKNOWN -> -1;
        };
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
