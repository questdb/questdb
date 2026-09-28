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
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.Utf8s;

/**
 * A stable dense-SYMBOL counting-sort permutation and the packing information
 * derived from it. Stored SYMBOL keys use the bitmap-index normalization, so
 * key zero represents null and non-null symbol id {@code n} becomes {@code n + 1}.
 */
public final class StableSymbolKeyPermutation implements QuietCloseable {
    private DirectLongList keyOffsets;
    private final int keySpaceSize;
    private DirectLongList permutation;
    private DirectLongList rowGroupBoundaries;
    private DirectLongList rowGroupFirstKeys;
    private DirectLongList rowGroupLastKeys;
    private final long rowCount;

    private StableSymbolKeyPermutation(int keySpaceSize, long rowCount) {
        this.keySpaceSize = keySpaceSize;
        this.rowCount = rowCount;
    }

    public static StableSymbolKeyPermutation build(
            long symbolAddress,
            long columnTop,
            long rowCount,
            int keySpaceSize,
            long targetRowGroupRows
    ) {
        if (rowCount < 0 || columnTop < 0 || columnTop > rowCount || keySpaceSize < 1 || targetRowGroupRows < 1) {
            throw CairoException.nonCritical()
                    .put("invalid clustered permutation arguments [rowCount=").put(rowCount)
                    .put(", columnTop=").put(columnTop)
                    .put(", keySpaceSize=").put(keySpaceSize)
                    .put(", targetRowGroupRows=").put(targetRowGroupRows).put(']');
        }
        if (rowCount - columnTop > 0 && symbolAddress == 0) {
            throw CairoException.nonCritical().put("missing SYMBOL data for clustered permutation");
        }

        checkedSize(rowCount, Long.BYTES, "clustered permutation");
        final StableSymbolKeyPermutation result = new StableSymbolKeyPermutation(keySpaceSize, rowCount);
        DirectLongList counts = null;
        try {
            counts = new DirectLongList(keySpaceSize, MemoryTag.NATIVE_O3);
            counts.setPos(keySpaceSize);
            Vect.memset(counts.getAddress(), (long) keySpaceSize * Long.BYTES, 0);

            for (long row = 0; row < rowCount; row++) {
                final int key = normalizedKey(symbolAddress, columnTop, row);
                checkKey(key, keySpaceSize, row);
                counts.set(key, counts.get(key) + 1);
            }

            result.keyOffsets = new DirectLongList((long) keySpaceSize + 1, MemoryTag.NATIVE_O3);
            result.keyOffsets.setPos((long) keySpaceSize + 1);
            long running = 0;
            for (int key = 0; key < keySpaceSize; key++) {
                result.keyOffsets.set(key, running);
                final long count = counts.get(key);
                if (count > Long.MAX_VALUE - running) {
                    throw CairoException.nonCritical().put("clustered key count overflow");
                }
                running += count;
                counts.set(key, result.keyOffsets.get(key));
            }
            result.keyOffsets.set(keySpaceSize, running);

            result.permutation = new DirectLongList(rowCount, MemoryTag.NATIVE_O3);
            result.permutation.setPos(rowCount);
            // Walking source rows in ascending order makes the counting sort stable.
            for (long row = 0; row < rowCount; row++) {
                final int key = normalizedKey(symbolAddress, columnTop, row);
                final long destination = counts.get(key);
                result.permutation.set(destination, row);
                counts.set(key, destination + 1);
            }
            result.planRowGroups(targetRowGroupRows);
            return result;
        } catch (Throwable th) {
            result.close();
            throw th;
        } finally {
            Misc.free(counts);
        }
    }

    @Override
    public void close() {
        keyOffsets = Misc.free(keyOffsets);
        permutation = Misc.free(permutation);
        rowGroupBoundaries = Misc.free(rowGroupBoundaries);
        rowGroupFirstKeys = Misc.free(rowGroupFirstKeys);
        rowGroupLastKeys = Misc.free(rowGroupLastKeys);
    }

    /**
     * Materializes a reordered descriptor. Column tops become zero because rows
     * below a source top become explicit nulls after permutation.
     */
    public void gather(PartitionDescriptor source, OwnedMemoryPartitionDescriptor destination) {
        if (source.getPartitionRowCount() != rowCount) {
            throw CairoException.nonCritical().put("partition row count does not match permutation");
        }
        destination.of(source.getTableName().toString(), rowCount, source.getTimestampIndex());
        long nameOffset = 0;
        try {
            for (int column = 0, n = source.getColumnCount(); column < n; column++) {
                final long rawIndex = (long) column * PartitionDescriptor.COLUMN_ENTRY_SIZE;
                final int nameSize = (int) source.columnData.get(rawIndex);
                final String name = Utf8s.stringFromUtf8Bytes(
                        source.columnNames.ptr() + nameOffset,
                        source.columnNames.ptr() + nameOffset + nameSize
                );
                nameOffset += nameSize;
                gatherColumn(source, rawIndex, name, destination);
            }
        } catch (Throwable th) {
            destination.clear();
            throw th;
        }
    }

    public long getAddress() {
        return permutation.getAddress();
    }

    public long getKeyOffset(int key) {
        if (key < 0 || key > keySpaceSize) {
            throw new IndexOutOfBoundsException();
        }
        return keyOffsets.get(key);
    }

    public int getKeySpaceSize() {
        return keySpaceSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getRowGroupBoundary(int index) {
        return rowGroupBoundaries.get(index);
    }

    public long getRowGroupBoundariesAddress() {
        return rowGroupBoundaries.getAddress();
    }

    public int getRowGroupCount() {
        return (int) rowGroupFirstKeys.size() - 1;
    }

    public int getRowGroupFirstKey(int index) {
        return (int) rowGroupFirstKeys.get(index);
    }

    /**
     * Returns one row-group-local key-directory entry. {@code directoryIndex}
     * ranges from zero through {@link #getRowGroupKeyCount(int)}, inclusive.
     */
    public long getRowGroupKeyOffset(int rowGroup, int directoryIndex) {
        final int firstKey = (int) rowGroupFirstKeys.get(rowGroup);
        final int keyCount = getRowGroupKeyCount(rowGroup);
        if (directoryIndex < 0 || directoryIndex > keyCount) {
            throw new IndexOutOfBoundsException();
        }
        final long groupLo = rowGroupBoundaries.get(rowGroup);
        final long groupHi = rowGroupBoundaries.get(rowGroup + 1);
        final long globalOffset = keyOffsets.get(firstKey + directoryIndex);
        return Math.max(groupLo, Math.min(groupHi, globalOffset)) - groupLo;
    }

    public int getRowGroupKeyCount(int rowGroup) {
        return (int) (rowGroupLastKeys.get(rowGroup) - rowGroupFirstKeys.get(rowGroup) + 1);
    }

    public long getSourceRow(long destinationRow) {
        if (destinationRow < 0 || destinationRow >= rowCount) {
            throw new IndexOutOfBoundsException();
        }
        return permutation.get(destinationRow);
    }

    private static void checkKey(int key, int keySpaceSize, long row) {
        if (key < 0 || key >= keySpaceSize) {
            throw CairoException.nonCritical()
                    .put("SYMBOL key outside clustered key space [row=").put(row)
                    .put(", key=").put(key)
                    .put(", keySpaceSize=").put(keySpaceSize).put(']');
        }
    }

    private static long checkedAdd(long left, long right, CharSequence what) {
        if (right > Long.MAX_VALUE - left) {
            throw CairoException.nonCritical().put(what).put(" size overflow");
        }
        return left + right;
    }

    private static long checkedSize(long count, long width, CharSequence what) {
        if (count < 0 || width < 0 || (count != 0 && width > Long.MAX_VALUE / count)) {
            throw CairoException.nonCritical().put(what).put(" size overflow");
        }
        return count * width;
    }

    private static int normalizedKey(long symbolAddress, long columnTop, long row) {
        if (row < columnTop) {
            return 0;
        }
        final int rawKey = Unsafe.getInt(symbolAddress + (row - columnTop) * Integer.BYTES);
        if (rawKey == Integer.MAX_VALUE) {
            return Integer.MIN_VALUE;
        }
        return TableUtils.toIndexKey(rawKey);
    }

    private void gatherColumn(
            PartitionDescriptor source,
            long rawIndex,
            CharSequence name,
            OwnedMemoryPartitionDescriptor destination
    ) {
        final long idAndType = source.columnData.get(rawIndex + PartitionDescriptor.COLUMN_ID_AND_TYPE_OFFSET);
        final int columnType = (int) idAndType;
        final int columnId = (int) (idAndType >>> 32);
        final int tag = ColumnType.tagOf(columnType);
        final long columnTop = source.columnData.get(rawIndex + 2);
        final long sourceAddress = source.columnData.get(rawIndex + PartitionDescriptor.COLUMN_ADDR_OFFSET);
        final long sourceSecondaryAddress = source.columnData.get(rawIndex + PartitionDescriptor.COLUMN_SECONDARY_ADDR_OFFSET);
        final long sourceSecondarySize = source.columnData.get(rawIndex + PartitionDescriptor.COLUMN_SECONDARY_SIZE_OFFSET);
        final long symbolOffsetsAddress = source.columnData.get(rawIndex + PartitionDescriptor.SYMBOL_OFFSET_ADDR_OFFSET);
        final long symbolOffsetsCount = source.columnData.get(rawIndex + PartitionDescriptor.SYMBOL_OFFSET_COUNT_OFFSET);
        final int encoding = (int) source.columnData.get(rawIndex + PartitionDescriptor.PARQUET_ENCODING_CONFIG_OFFSET);

        if (tag == ColumnType.STRING || tag == ColumnType.BINARY) {
            gatherLegacyVar(name, columnType, columnId, columnTop, sourceAddress, sourceSecondaryAddress, encoding, destination);
        } else if (tag == ColumnType.VARCHAR) {
            gatherVarchar(name, columnType, columnId, columnTop, sourceAddress, sourceSecondaryAddress, encoding, destination);
        } else {
            final int width = ColumnType.sizeOf(columnType);
            if (width < 1) {
                throw CairoException.nonCritical().put("unsupported clustered gather type [type=").put(ColumnType.nameOf(columnType)).put(']');
            }
            gatherFixed(name, columnType, columnId, columnTop, sourceAddress, sourceSecondaryAddress, sourceSecondarySize,
                    symbolOffsetsAddress, symbolOffsetsCount, encoding, width, destination);
        }
    }

    private void gatherFixed(
            CharSequence name,
            int columnType,
            int columnId,
            long columnTop,
            long sourceAddress,
            long sourceSecondaryAddress,
            long sourceSecondarySize,
            long symbolOffsetsAddress,
            long symbolOffsetsCount,
            int encoding,
            int width,
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
                final long sourceRow = permutation.get(destinationRow);
                if (sourceRow >= columnTop) {
                    Vect.memcpy(
                            address + destinationRow * width,
                            sourceAddress + (sourceRow - columnTop) * width,
                            width
                    );
                }
            }
            final long ownedAddress = address;
            address = 0;
            destination.addColumn(name, columnType, columnId, 0, ownedAddress, size,
                    sourceSecondaryAddress, sourceSecondarySize, symbolOffsetsAddress, symbolOffsetsCount, encoding);
        } finally {
            Unsafe.free(address, size, MemoryTag.NATIVE_O3);
        }
    }

    private void gatherLegacyVar(
            CharSequence name,
            int columnType,
            int columnId,
            long columnTop,
            long sourceDataAddress,
            long sourceAuxAddress,
            int encoding,
            OwnedMemoryPartitionDescriptor destination
    ) {
        final boolean binary = ColumnType.tagOf(columnType) == ColumnType.BINARY;
        final int nullSize = binary ? Long.BYTES : Integer.BYTES;
        long dataSize = 0;
        for (long destinationRow = 0; destinationRow < rowCount; destinationRow++) {
            final long sourceRow = permutation.get(destinationRow);
            final long entrySize;
            if (sourceRow < columnTop) {
                entrySize = nullSize;
            } else {
                final long physicalRow = sourceRow - columnTop;
                entrySize = Unsafe.getLong(sourceAuxAddress + (physicalRow + 1) * Long.BYTES)
                        - Unsafe.getLong(sourceAuxAddress + physicalRow * Long.BYTES);
                if (entrySize < nullSize) {
                    throw CairoException.nonCritical().put("invalid variable-size column offsets [column=").put(name).put(']');
                }
            }
            dataSize = checkedAdd(dataSize, entrySize, name);
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
                final long sourceRow = permutation.get(destinationRow);
                if (sourceRow < columnTop) {
                    if (binary) {
                        Unsafe.putLong(dataAddress + dataOffset, TableUtils.NULL_LEN);
                    } else {
                        Unsafe.putInt(dataAddress + dataOffset, TableUtils.NULL_LEN);
                    }
                    dataOffset += nullSize;
                } else {
                    final long physicalRow = sourceRow - columnTop;
                    final long lo = Unsafe.getLong(sourceAuxAddress + physicalRow * Long.BYTES);
                    final long hi = Unsafe.getLong(sourceAuxAddress + (physicalRow + 1) * Long.BYTES);
                    Vect.memcpy(dataAddress + dataOffset, sourceDataAddress + lo, hi - lo);
                    dataOffset += hi - lo;
                }
                Unsafe.putLong(auxAddress + (destinationRow + 1) * Long.BYTES, dataOffset);
            }
            final long ownedDataAddress = dataAddress;
            final long ownedAuxAddress = auxAddress;
            dataAddress = 0;
            auxAddress = 0;
            destination.addColumn(name, columnType, columnId, 0, ownedDataAddress, dataSize,
                    ownedAuxAddress, auxSize, 0, 0, encoding);
        } finally {
            Unsafe.free(dataAddress, dataSize, MemoryTag.NATIVE_O3);
            Unsafe.free(auxAddress, auxSize, MemoryTag.NATIVE_O3);
        }
    }

    private void gatherVarchar(
            CharSequence name,
            int columnType,
            int columnId,
            long columnTop,
            long sourceDataAddress,
            long sourceAuxAddress,
            int encoding,
            OwnedMemoryPartitionDescriptor destination
    ) {
        long dataSize = 0;
        for (long destinationRow = 0; destinationRow < rowCount; destinationRow++) {
            final long sourceRow = permutation.get(destinationRow);
            if (sourceRow >= columnTop) {
                final long physicalRow = sourceRow - columnTop;
                final int size = VarcharTypeDriver.getValueSize(sourceAuxAddress, physicalRow);
                if (size > VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                    dataSize = checkedAdd(dataSize, size, name);
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
                final long sourceRow = permutation.get(destinationRow);
                final long destinationAux = auxAddress + destinationRow * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                final long valueOffset = dataOffset;
                if (sourceRow < columnTop) {
                    Vect.memset(destinationAux, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, 0);
                    Unsafe.putInt(destinationAux, VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL);
                } else {
                    final long physicalRow = sourceRow - columnTop;
                    final long sourceAux = sourceAuxAddress + physicalRow * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                    Vect.memcpy(destinationAux, sourceAux, VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES);
                    final int header = Unsafe.getInt(sourceAux);
                    final int size = VarcharTypeDriver.getValueSize(sourceAuxAddress, physicalRow);
                    if (size > VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                        final long valueAddress = VarcharTypeDriver.getValueByteAddress(header, sourceAux, sourceDataAddress);
                        Vect.memcpy(dataAddress + dataOffset, valueAddress, size);
                        dataOffset += size;
                    }
                }
                // The trailing six bytes contain the little-endian data offset.
                Unsafe.putShort(destinationAux + 10, (short) valueOffset);
                Unsafe.putInt(destinationAux + 12, (int) (valueOffset >>> 16));
            }
            final long ownedDataAddress = dataAddress;
            final long ownedAuxAddress = auxAddress;
            dataAddress = 0;
            auxAddress = 0;
            destination.addColumn(name, columnType, columnId, 0, ownedDataAddress, dataSize,
                    ownedAuxAddress, auxSize, 0, 0, encoding);
        } finally {
            Unsafe.free(dataAddress, dataSize, MemoryTag.NATIVE_O3);
            Unsafe.free(auxAddress, auxSize, MemoryTag.NATIVE_O3);
        }
    }

    private void planRowGroups(long targetRows) {
        rowGroupBoundaries = new DirectLongList(16, MemoryTag.NATIVE_O3);
        rowGroupFirstKeys = new DirectLongList(16, MemoryTag.NATIVE_O3);
        rowGroupBoundaries.add(0);
        long groupStart = 0;
        int groupFirstKey = -1;
        for (int key = 0; key < keySpaceSize; key++) {
            long keyLo = keyOffsets.get(key);
            final long keyHi = keyOffsets.get(key + 1);
            if (keyLo == keyHi) {
                continue;
            }
            if (keyHi - keyLo > targetRows) {
                if (groupStart < keyLo) {
                    rowGroupFirstKeys.add(groupFirstKey);
                    rowGroupBoundaries.add(keyLo);
                }
                while (keyLo < keyHi) {
                    rowGroupFirstKeys.add(key);
                    keyLo = Math.min(keyHi, keyLo + targetRows);
                    rowGroupBoundaries.add(keyLo);
                }
                groupStart = keyHi;
                groupFirstKey = -1;
                continue;
            }
            if (groupFirstKey < 0) {
                groupFirstKey = key;
            }
            if (groupStart < keyLo && keyHi - groupStart > targetRows) {
                rowGroupFirstKeys.add(groupFirstKey);
                rowGroupBoundaries.add(keyLo);
                groupStart = keyLo;
                groupFirstKey = key;
            }
            if (keyHi - groupStart == targetRows) {
                rowGroupFirstKeys.add(groupFirstKey);
                rowGroupBoundaries.add(keyHi);
                groupStart = keyHi;
                groupFirstKey = -1;
            }
        }
        if (groupStart < rowCount) {
            rowGroupFirstKeys.add(groupFirstKey);
            rowGroupBoundaries.add(rowCount);
        }
        rowGroupFirstKeys.add(keySpaceSize);

        rowGroupLastKeys = new DirectLongList(16, MemoryTag.NATIVE_O3);
        for (int group = 0, n = getRowGroupCount(); group < n; group++) {
            int lastKey = (int) rowGroupFirstKeys.get(group);
            final long groupHi = rowGroupBoundaries.get(group + 1);
            while (lastKey + 1 < keySpaceSize && keyOffsets.get(lastKey + 1) < groupHi) {
                lastKey++;
            }
            rowGroupLastKeys.add(lastKey);
        }
    }
}
