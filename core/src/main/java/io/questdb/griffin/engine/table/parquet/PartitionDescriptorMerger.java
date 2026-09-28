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

package io.questdb.griffin.engine.table.parquet;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.std.DirectLongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8s;

/**
 * Materialises descriptors with one schema into a single descriptor ordered by
 * designated timestamp. This is deliberately a full-copy correctness path for
 * clustered O3 rewrites: clustered parquet is key-major, so its source row
 * groups cannot be fed to the ordinary globally-time-ordered O3 merge planner.
 */
public final class PartitionDescriptorMerger {
    // O3PartitionJob's timestamp descriptor points at 16-byte (timestamp,row-id)
    // entries instead of a dense eight-byte timestamp column.
    private static final int TIMESTAMP_STRIDED_16 = 0x4000_0000;

    private PartitionDescriptorMerger() {
    }

    public static void mergeTimestampOrdered(
            ObjList<? extends PartitionDescriptor> sources,
            int timestampColumnIndex,
            OwnedMemoryPartitionDescriptor destination
    ) {
        mergeTimestampOrdered(sources, timestampColumnIndex, destination, false);
    }

    /**
     * @return rows removed by timestamp-only deduplication
     */
    public static long mergeTimestampOrdered(
            ObjList<? extends PartitionDescriptor> sources,
            int timestampColumnIndex,
            OwnedMemoryPartitionDescriptor destination,
            boolean deduplicateTimestamps
    ) {
        if (sources.size() < 1) {
            throw CairoException.nonCritical().put("clustered rewrite has no source rows");
        }
        final PartitionDescriptor schema = sources.getQuick(0);
        final int columnCount = schema.getColumnCount();
        final DirectLongList sourceOffsets = new DirectLongList(sources.size() + 1L, MemoryTag.NATIVE_O3);
        DirectLongList timestampIndex = null;
        DirectLongList timestampScratch = null;
        long rowCount = 0;
        try {
            sourceOffsets.add(0);
            for (int sourceIndex = 0, n = sources.size(); sourceIndex < n; sourceIndex++) {
                final PartitionDescriptor source = sources.getQuick(sourceIndex);
                validateSchema(schema, source, columnCount);
                rowCount = checkedAdd(rowCount, source.getPartitionRowCount(), "clustered rewrite row count");
                sourceOffsets.add(rowCount);
            }
            if (timestampColumnIndex < 0 || timestampColumnIndex >= columnCount) {
                throw CairoException.nonCritical().put("invalid clustered rewrite timestamp column");
            }
            checkedSize(rowCount, 2L * Long.BYTES, "clustered timestamp index");
            timestampIndex = new DirectLongList(checkedAdd(rowCount, rowCount, "clustered timestamp index"), MemoryTag.NATIVE_O3);
            timestampIndex.setPos(rowCount * 2);
            timestampScratch = new DirectLongList(checkedAdd(rowCount, rowCount, "clustered timestamp scratch"), MemoryTag.NATIVE_O3);
            timestampScratch.setPos(rowCount * 2);

            long globalRow = 0;
            for (int sourceIndex = 0, n = sources.size(); sourceIndex < n; sourceIndex++) {
                final PartitionDescriptor source = sources.getQuick(sourceIndex);
                final long raw = (long) timestampColumnIndex * PartitionDescriptor.COLUMN_ENTRY_SIZE;
                final int type = (int) source.columnData.get(raw + PartitionDescriptor.COLUMN_ID_AND_TYPE_OFFSET);
                if (!ColumnType.isTimestamp(type)) {
                    throw CairoException.nonCritical().put("clustered rewrite timestamp column has invalid type");
                }
                final long top = source.columnData.get(raw + 2);
                final long address = source.columnData.get(raw + PartitionDescriptor.COLUMN_ADDR_OFFSET);
                final long rows = source.getPartitionRowCount();
                for (long row = 0; row < rows; row++, globalRow++) {
                    if (row < top) {
                        throw CairoException.nonCritical().put("designated timestamp has a column top in clustered rewrite");
                    }
                    final long physicalRow = row - top;
                    final long timestamp = Unsafe.getLong(address + physicalRow * ((type & TIMESTAMP_STRIDED_16) != 0 ? 16L : Long.BYTES));
                    timestampIndex.set(globalRow * 2, timestamp);
                    timestampIndex.set(globalRow * 2 + 1, globalRow);
                }
            }
            Vect.radixSortLongIndexAscInPlace(timestampIndex.getAddress(), rowCount, timestampScratch.getAddress());

            final long sourceRowCount = rowCount;
            if (deduplicateTimestamps && rowCount > 1) {
                long write = 0;
                long read = 0;
                while (read < rowCount) {
                    final long timestamp = timestampIndex.get(read * 2);
                    long winnerRow = timestampIndex.get(read * 2 + 1);
                    long next = read + 1;
                    while (next < rowCount && timestampIndex.get(next * 2) == timestamp) {
                        // Sources are appended old-data first and O3 last. The
                        // largest global source row is therefore the newest row
                        // for timestamp-only deduplication, independent of the
                        // radix sort's tie order.
                        winnerRow = Math.max(winnerRow, timestampIndex.get(next * 2 + 1));
                        next++;
                    }
                    timestampIndex.set(write * 2, timestamp);
                    timestampIndex.set(write * 2 + 1, winnerRow);
                    write++;
                    read = next;
                }
                rowCount = write;
            }

            destination.of(schema.getTableName().toString(), rowCount, schema.getTimestampIndex());
            long nameOffset = 0;
            for (int column = 0; column < columnCount; column++) {
                final long raw = (long) column * PartitionDescriptor.COLUMN_ENTRY_SIZE;
                final int nameSize = (int) schema.columnData.get(raw);
                final String name = Utf8s.stringFromUtf8Bytes(
                        schema.columnNames.ptr() + nameOffset,
                        schema.columnNames.ptr() + nameOffset + nameSize
                );
                nameOffset += nameSize;
                gatherColumn(sources, sourceOffsets, timestampIndex, schema, raw, name, rowCount, destination);
            }
            return sourceRowCount - rowCount;
        } catch (Throwable th) {
            destination.clear();
            throw th;
        } finally {
            Misc.free(timestampScratch);
            Misc.free(timestampIndex);
            sourceOffsets.close();
        }
    }

    private static long checkedAdd(long left, long right, CharSequence what) {
        if (right > Long.MAX_VALUE - left) {
            throw CairoException.nonCritical().put(what).put(" overflow");
        }
        return left + right;
    }

    private static long checkedSize(long count, long width, CharSequence what) {
        if (count < 0 || width < 0 || (count != 0 && width > Long.MAX_VALUE / count)) {
            throw CairoException.nonCritical().put(what).put(" overflow");
        }
        return count * width;
    }

    private static int findSource(DirectLongList offsets, long globalRow) {
        int lo = 0;
        int hi = (int) offsets.size() - 2;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (globalRow < offsets.get(mid)) {
                hi = mid - 1;
            } else if (globalRow >= offsets.get(mid + 1)) {
                lo = mid + 1;
            } else {
                return mid;
            }
        }
        throw new IndexOutOfBoundsException();
    }

    private static void gatherColumn(
            ObjList<? extends PartitionDescriptor> sources,
            DirectLongList sourceOffsets,
            DirectLongList order,
            PartitionDescriptor schema,
            long raw,
            CharSequence name,
            long rowCount,
            OwnedMemoryPartitionDescriptor destination
    ) {
        final long idAndType = schema.columnData.get(raw + PartitionDescriptor.COLUMN_ID_AND_TYPE_OFFSET);
        final int sourceType = (int) idAndType;
        final int columnType = sourceType & ~TIMESTAMP_STRIDED_16;
        final int columnId = (int) (idAndType >>> 32);
        final int tag = ColumnType.tagOf(columnType);
        final int encoding = (int) schema.columnData.get(raw + PartitionDescriptor.PARQUET_ENCODING_CONFIG_OFFSET);
        if (tag == ColumnType.STRING || tag == ColumnType.BINARY) {
            gatherLegacyVar(sources, sourceOffsets, order, raw, name, columnType, columnId, encoding, rowCount, destination);
        } else if (tag == ColumnType.VARCHAR) {
            gatherVarchar(sources, sourceOffsets, order, raw, name, columnType, columnId, encoding, rowCount, destination);
        } else {
            final int width = ColumnType.sizeOf(columnType);
            if (width < 1) {
                throw CairoException.nonCritical().put("unsupported clustered rewrite type [type=").put(ColumnType.nameOf(columnType)).put(']');
            }
            gatherFixed(sources, sourceOffsets, order, schema, raw, name, columnType, columnId, encoding, width, rowCount, destination);
        }
    }

    private static void gatherFixed(
            ObjList<? extends PartitionDescriptor> sources,
            DirectLongList sourceOffsets,
            DirectLongList order,
            PartitionDescriptor schema,
            long raw,
            CharSequence name,
            int columnType,
            int columnId,
            int encoding,
            int width,
            long rowCount,
            OwnedMemoryPartitionDescriptor destination
    ) {
        final long size = checkedSize(rowCount, width, name);
        long address = 0;
        try {
            address = Unsafe.malloc(size, MemoryTag.NATIVE_O3);
            if (size > 0) {
                TableUtils.setNull(columnType, address, rowCount);
            }
            for (long destinationRow = 0; destinationRow < rowCount; destinationRow++) {
                final long globalRow = order.get(destinationRow * 2 + 1);
                final int sourceIndex = findSource(sourceOffsets, globalRow);
                final PartitionDescriptor source = sources.getQuick(sourceIndex);
                final long sourceRow = globalRow - sourceOffsets.get(sourceIndex);
                final long top = source.columnData.get(raw + 2);
                if (sourceRow >= top) {
                    final int actualType = (int) source.columnData.get(raw + PartitionDescriptor.COLUMN_ID_AND_TYPE_OFFSET);
                    final long stride = (actualType & TIMESTAMP_STRIDED_16) != 0 ? 16L : width;
                    Vect.memcpy(
                            address + destinationRow * width,
                            source.columnData.get(raw + PartitionDescriptor.COLUMN_ADDR_OFFSET) + (sourceRow - top) * stride,
                            width
                    );
                }
            }
            final long symbolValues = schema.columnData.get(raw + PartitionDescriptor.COLUMN_SECONDARY_ADDR_OFFSET);
            final long symbolValuesSize = schema.columnData.get(raw + PartitionDescriptor.COLUMN_SECONDARY_SIZE_OFFSET);
            final long symbolOffsets = schema.columnData.get(raw + PartitionDescriptor.SYMBOL_OFFSET_ADDR_OFFSET);
            final long symbolCount = schema.columnData.get(raw + PartitionDescriptor.SYMBOL_OFFSET_COUNT_OFFSET);
            final long owned = address;
            address = 0;
            destination.addColumn(name, columnType, columnId, 0, owned, size,
                    symbolValues, symbolValuesSize, symbolOffsets, symbolCount, encoding);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_O3);
        }
    }

    private static void gatherLegacyVar(
            ObjList<? extends PartitionDescriptor> sources,
            DirectLongList sourceOffsets,
            DirectLongList order,
            long raw,
            CharSequence name,
            int columnType,
            int columnId,
            int encoding,
            long rowCount,
            OwnedMemoryPartitionDescriptor destination
    ) {
        final boolean binary = ColumnType.tagOf(columnType) == ColumnType.BINARY;
        final int nullSize = binary ? Long.BYTES : Integer.BYTES;
        long dataSize = 0;
        for (long destinationRow = 0; destinationRow < rowCount; destinationRow++) {
            final long globalRow = order.get(destinationRow * 2 + 1);
            final int sourceIndex = findSource(sourceOffsets, globalRow);
            final PartitionDescriptor source = sources.getQuick(sourceIndex);
            final long sourceRow = globalRow - sourceOffsets.get(sourceIndex);
            final long top = source.columnData.get(raw + 2);
            if (sourceRow < top) {
                dataSize = checkedAdd(dataSize, nullSize, name);
            } else {
                final long aux = source.columnData.get(raw + PartitionDescriptor.COLUMN_SECONDARY_ADDR_OFFSET);
                final long row = sourceRow - top;
                dataSize = checkedAdd(dataSize, Unsafe.getLong(aux + (row + 1) * Long.BYTES) - Unsafe.getLong(aux + row * Long.BYTES), name);
            }
        }
        final long auxSize = checkedSize(rowCount + 1, Long.BYTES, name);
        long dataAddress = 0;
        long auxAddress = 0;
        try {
            dataAddress = Unsafe.malloc(dataSize, MemoryTag.NATIVE_O3);
            auxAddress = Unsafe.malloc(auxSize, MemoryTag.NATIVE_O3);
            long dataOffset = 0;
            Unsafe.putLong(auxAddress, 0);
            for (long destinationRow = 0; destinationRow < rowCount; destinationRow++) {
                final long globalRow = order.get(destinationRow * 2 + 1);
                final int sourceIndex = findSource(sourceOffsets, globalRow);
                final PartitionDescriptor source = sources.getQuick(sourceIndex);
                final long sourceRow = globalRow - sourceOffsets.get(sourceIndex);
                final long top = source.columnData.get(raw + 2);
                if (sourceRow < top) {
                    if (binary) {
                        Unsafe.putLong(dataAddress + dataOffset, TableUtils.NULL_LEN);
                    } else {
                        Unsafe.putInt(dataAddress + dataOffset, TableUtils.NULL_LEN);
                    }
                    dataOffset += nullSize;
                } else {
                    final long row = sourceRow - top;
                    final long sourceData = source.columnData.get(raw + PartitionDescriptor.COLUMN_ADDR_OFFSET);
                    final long sourceAux = source.columnData.get(raw + PartitionDescriptor.COLUMN_SECONDARY_ADDR_OFFSET);
                    final long lo = Unsafe.getLong(sourceAux + row * Long.BYTES);
                    final long hi = Unsafe.getLong(sourceAux + (row + 1) * Long.BYTES);
                    Vect.memcpy(dataAddress + dataOffset, sourceData + lo, hi - lo);
                    dataOffset += hi - lo;
                }
                Unsafe.putLong(auxAddress + (destinationRow + 1) * Long.BYTES, dataOffset);
            }
            final long ownedData = dataAddress;
            final long ownedAux = auxAddress;
            dataAddress = 0;
            auxAddress = 0;
            destination.addColumn(name, columnType, columnId, 0, ownedData, dataSize, ownedAux, auxSize, 0, 0, encoding);
        } finally {
            Unsafe.free(dataAddress, dataSize, MemoryTag.NATIVE_O3);
            Unsafe.free(auxAddress, auxSize, MemoryTag.NATIVE_O3);
        }
    }

    private static void gatherVarchar(
            ObjList<? extends PartitionDescriptor> sources,
            DirectLongList sourceOffsets,
            DirectLongList order,
            long raw,
            CharSequence name,
            int columnType,
            int columnId,
            int encoding,
            long rowCount,
            OwnedMemoryPartitionDescriptor destination
    ) {
        long dataSize = 0;
        for (long destinationRow = 0; destinationRow < rowCount; destinationRow++) {
            final long globalRow = order.get(destinationRow * 2 + 1);
            final int sourceIndex = findSource(sourceOffsets, globalRow);
            final PartitionDescriptor source = sources.getQuick(sourceIndex);
            final long sourceRow = globalRow - sourceOffsets.get(sourceIndex);
            final long top = source.columnData.get(raw + 2);
            if (sourceRow >= top) {
                final long aux = source.columnData.get(raw + PartitionDescriptor.COLUMN_SECONDARY_ADDR_OFFSET);
                final int valueSize = VarcharTypeDriver.getValueSize(aux, sourceRow - top);
                if (valueSize > VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                    dataSize = checkedAdd(dataSize, valueSize, name);
                }
            }
        }
        final long auxSize = checkedSize(rowCount, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, name);
        if (dataSize >= VarcharTypeDriver.VARCHAR_MAX_COLUMN_SIZE) {
            throw CairoException.nonCritical().put("clustered VARCHAR data is too large [size=").put(dataSize).put(']');
        }
        long dataAddress = 0;
        long auxAddress = 0;
        try {
            dataAddress = Unsafe.malloc(dataSize, MemoryTag.NATIVE_O3);
            auxAddress = Unsafe.malloc(auxSize, MemoryTag.NATIVE_O3);
            long dataOffset = 0;
            for (long destinationRow = 0; destinationRow < rowCount; destinationRow++) {
                final long globalRow = order.get(destinationRow * 2 + 1);
                final int sourceIndex = findSource(sourceOffsets, globalRow);
                final PartitionDescriptor source = sources.getQuick(sourceIndex);
                final long sourceRow = globalRow - sourceOffsets.get(sourceIndex);
                final long top = source.columnData.get(raw + 2);
                final long destinationAux = auxAddress + destinationRow * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                final long valueOffset = dataOffset;
                if (sourceRow < top) {
                    Vect.memset(destinationAux, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, 0);
                    Unsafe.putInt(destinationAux, VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL);
                } else {
                    final long row = sourceRow - top;
                    final long sourceData = source.columnData.get(raw + PartitionDescriptor.COLUMN_ADDR_OFFSET);
                    final long sourceAuxBase = source.columnData.get(raw + PartitionDescriptor.COLUMN_SECONDARY_ADDR_OFFSET);
                    final long sourceAux = sourceAuxBase + row * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                    Vect.memcpy(destinationAux, sourceAux, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
                    final int header = Unsafe.getInt(sourceAux);
                    final int valueSize = VarcharTypeDriver.getValueSize(sourceAuxBase, row);
                    if (valueSize > VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                        final long valueAddress = VarcharTypeDriver.getValueByteAddress(header, sourceAux, sourceData);
                        Vect.memcpy(dataAddress + dataOffset, valueAddress, valueSize);
                        dataOffset += valueSize;
                    }
                }
                Unsafe.putShort(destinationAux + 10, (short) valueOffset);
                Unsafe.putInt(destinationAux + 12, (int) (valueOffset >>> 16));
            }
            final long ownedData = dataAddress;
            final long ownedAux = auxAddress;
            dataAddress = 0;
            auxAddress = 0;
            destination.addColumn(name, columnType, columnId, 0, ownedData, dataSize, ownedAux, auxSize, 0, 0, encoding);
        } finally {
            Unsafe.free(dataAddress, dataSize, MemoryTag.NATIVE_O3);
            Unsafe.free(auxAddress, auxSize, MemoryTag.NATIVE_O3);
        }
    }

    private static void validateSchema(PartitionDescriptor expected, PartitionDescriptor actual, int columnCount) {
        if (actual.getColumnCount() != columnCount || actual.getTimestampIndex() != expected.getTimestampIndex()) {
            throw CairoException.nonCritical().put("clustered rewrite source schema mismatch");
        }
        for (int column = 0; column < columnCount; column++) {
            final int expectedType = expected.getColumnType(column) & ~TIMESTAMP_STRIDED_16;
            final int actualType = actual.getColumnType(column) & ~TIMESTAMP_STRIDED_16;
            if (expectedType != actualType
                    && !(ColumnType.isTimestamp(expectedType) && ColumnType.isTimestamp(actualType))) {
                throw CairoException.nonCritical().put("clustered rewrite source column type mismatch [column=").put(column)
                        .put(", expected=").put(expectedType).put(", actual=").put(actualType).put(']');
            }
        }
    }
}
