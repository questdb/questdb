/*******************************************************************************
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

package io.questdb.cairo.sql;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.GeoHashes;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.arr.BorrowedArray;
import io.questdb.cairo.vm.NullMemoryCMR;
import io.questdb.std.BinarySequence;
import io.questdb.std.BoolList;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.IntHashSet;
import io.questdb.std.Long256;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectString;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import org.jetbrains.annotations.Nullable;

/**
 * A specialized {@link PageFrameMemoryRecord} for late materialization scenarios where some columns
 * contain only filtered (compacted) data while others contain full unfiltered data.
 * <p>
 * <b>Important:</b> This record only supports sequential access for performance reasons.
 * The internal cursor is incremented on each {@link #setRowIndex(long)} call, regardless of
 * the actual rowIndex value passed. This design enables O(1) row index translation for filtered
 * columns without the overhead of binary search or index lookup.
 * <p>
 * <b>Column types:</b>
 * <ul>
 *   <li><b>Filtered columns</b> (marked in {@code filterIndexes}): contain compacted data where
 *       only rows passing the filter are present. These columns use the original {@code rowIndex}
 *       for data access.</li>
 *   <li><b>Non-filtered columns</b>: contain data corresponding to the filtered row set. These
 *       columns use the internal sequential {@code cursor} for data access.</li>
 * </ul>
 * <p>
 * <b>Usage constraints:</b>
 * <ul>
 *   <li>Rows must be accessed sequentially in ascending order</li>
 *   <li>Random access (e.g., jumping to arbitrary row positions) is NOT supported and will
 *       result in incorrect data being read</li>
 * </ul>
 * <p>
 * For scenarios requiring random access, use {@link PageFrameMemoryRecord} instead.
 *
 * @see PageFrameMemoryRecord
 */
public class PageFrameFilteredNoRandomAccessMemoryRecord extends PageFrameMemoryRecord {
    private final BoolList filteredColumns = new BoolList();
    private long cursor = -2;

    @Override
    public void clear() {
        super.clear();
        cursor = -2;
        filteredColumns.clear();
    }

    @Override
    public ArrayView getArray(int columnIndex, int columnType) {
        final BorrowedArray array = borrowedArray(columnIndex);
        final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxPageAddress != 0) {
            final long auxPageLim = auxPageAddress + auxPageSizes.get(columnOffset + columnIndex);
            final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
            final long dataPageLim = dataPageAddress + pageSizes.get(columnOffset + columnIndex);
            array.of(
                    columnType,
                    auxPageAddress,
                    auxPageLim,
                    dataPageAddress,
                    dataPageLim,
                    rowIndex(columnIndex)
            );
        } else {
            array.ofNull();
        }
        return array;
    }

    @Override
    public BinarySequence getBin(int columnIndex) {
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex(columnIndex) << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            return getBin(dataPageAddress, dataOffset, dataPageLim, bsView(columnIndex));
        }
        return NullMemoryCMR.INSTANCE.getBin(0);
    }

    // Note: This method is only called by {`@link` PageFrameMemoryPool#preTouchColumns},
    // which only operates on native format partitions (not Parquet). As a result,
    // this code path is not reachable and cannot be test covered.
    @Override
    public long getBinLen(int columnIndex) {
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex(columnIndex) << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            if (dataPageLim < dataOffset + 8) {
                throw CairoException.critical(0)
                        .put("binary is outside of file boundary [dataOffset=")
                        .put(dataOffset)
                        .put(", dataPageLim=")
                        .put(dataPageLim)
                        .put(']');
            }
            return Unsafe.getUnsafe().getLong(dataPageAddress + dataOffset);
        }
        return NullMemoryCMR.INSTANCE.getBinLen(0);
    }

    @Override
    public boolean getBool(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex(columnIndex)) == 1;
        }
        return NullMemoryCMR.INSTANCE.getBool(0);
    }

    @Override
    public byte getByte(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex(columnIndex));
        }
        return NullMemoryCMR.INSTANCE.getByte(0);
    }

    @Override
    public char getChar(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getChar(address + (rowIndex(columnIndex) << 1));
        }
        return NullMemoryCMR.INSTANCE.getChar(0);
    }

    @Override
    public void getDecimal128(int columnIndex, Decimal128 sink) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            address += (rowIndex(columnIndex) << 4);
            sink.ofRaw(
                    Unsafe.getUnsafe().getLong(address),
                    Unsafe.getUnsafe().getLong(address + 8L)
            );
        } else {
            sink.ofRawNull();
        }
    }

    @Override
    public short getDecimal16(int columnIndex) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex(columnIndex) << 1));
        }
        return NullMemoryCMR.INSTANCE.getDecimal16(0);
    }

    @Override
    public void getDecimal256(int columnIndex, Decimal256 sink) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            sink.ofRawAddress(address + (rowIndex(columnIndex) << 5));
        } else {
            sink.ofRawNull();
        }
    }

    @Override
    public int getDecimal32(int columnIndex) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex(columnIndex) << 2));
        }
        return NullMemoryCMR.INSTANCE.getDecimal32(0);
    }

    @Override
    public long getDecimal64(int columnIndex) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex(columnIndex) << 3));
        }
        return NullMemoryCMR.INSTANCE.getDecimal64(0);
    }

    @Override
    public byte getDecimal8(int columnIndex) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex(columnIndex));
        }
        return NullMemoryCMR.INSTANCE.getDecimal8(0);
    }

    @Override
    public double getDouble(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getDouble(address + (rowIndex(columnIndex) << 3));
        }
        return NullMemoryCMR.INSTANCE.getDouble(0);
    }

    @Override
    public float getFloat(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getFloat(address + (rowIndex(columnIndex) << 2));
        }
        return NullMemoryCMR.INSTANCE.getFloat(0);
    }

    @Override
    public byte getGeoByte(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getByte(address + rowIndex(columnIndex));
        }
        return GeoHashes.BYTE_NULL;
    }

    @Override
    public int getGeoInt(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex(columnIndex) << 2));
        }
        return GeoHashes.INT_NULL;
    }

    @Override
    public long getGeoLong(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex(columnIndex) << 3));
        }
        return GeoHashes.NULL;
    }

    @Override
    public short getGeoShort(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex(columnIndex) << 1));
        }
        return GeoHashes.SHORT_NULL;
    }

    @Override
    public int getIPv4(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex(columnIndex) << 2));
        }
        return NullMemoryCMR.INSTANCE.getIPv4(0);
    }

    @Override
    public int getInt(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getInt(address + (rowIndex(columnIndex) << 2));
        }
        return NullMemoryCMR.INSTANCE.getInt(0);
    }

    @Override
    public long getLong(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex(columnIndex) << 3));
        }
        return NullMemoryCMR.INSTANCE.getLong(0);
    }

    @Override
    public long getLong128Hi(int columnIndex) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex(columnIndex) << 4) + Long.BYTES);
        }
        return NullMemoryCMR.INSTANCE.getLong128Hi();
    }

    @Override
    public long getLong128Lo(int columnIndex) {
        long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getLong(address + (rowIndex(columnIndex) << 4));
        }
        return NullMemoryCMR.INSTANCE.getLong128Lo();
    }

    @Override
    public void getLong256(int columnIndex, CharSink<?> sink) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            getLong256(address + rowIndex(columnIndex) * Long256.BYTES, sink);
            return;
        }
        NullMemoryCMR.INSTANCE.getLong256(0, sink);
    }

    @Override
    public short getShort(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            return Unsafe.getUnsafe().getShort(address + (rowIndex(columnIndex) << 1));
        }
        return NullMemoryCMR.INSTANCE.getShort(0);
    }

    @Override
    public int getStrLen(int columnIndex) {
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex(columnIndex) << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            if (dataPageLim < dataOffset + 4) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [dataOffset=")
                        .put(dataOffset)
                        .put(", dataPageLim=")
                        .put(dataPageLim)
                        .put(']');
            }
            return Unsafe.getUnsafe().getInt(dataPageAddress + dataOffset);
        }
        return NullMemoryCMR.INSTANCE.getStrLen(0);
    }

    // Note: Currently not used because PageFrameFilteredNoRandomAccessMemoryRecord
    // is only used by aggregate functions, which access symbols via getInt() + SymbolTable.
    @Override
    public CharSequence getSymA(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            int key = Unsafe.getUnsafe().getInt(address + (rowIndex(columnIndex) << 2));
            return getSymbolTable(columnIndex).valueOf(key);
        }
        return null;
    }

    // Note: Currently not used because PageFrameFilteredNoRandomAccessMemoryRecord
    // is only used by aggregate functions, which access symbols via getInt() + SymbolTable.
    @Override
    public CharSequence getSymB(int columnIndex) {
        final long address = pageAddresses.get(columnOffset + columnIndex);
        if (address != 0) {
            int key = Unsafe.getUnsafe().getInt(address + (rowIndex(columnIndex) << 2));
            return getSymbolTable(columnIndex).valueBOf(key);
        }
        return null;
    }

    @Override
    public int getVarcharSize(int columnIndex) {
        final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxPageAddress != 0) {
            return VarcharTypeDriver.getValueSize(auxPageAddress, rowIndex(columnIndex));
        }
        return TableUtils.NULL_LEN;
    }

    public PageFrameFilteredNoRandomAccessMemoryRecord of(PageFrameMemory memory, PageFrameMemoryRecord other, IntHashSet filterIndexes) {
        super.init(memory);
        this.symbolTableSource = other.symbolTableSource;
        this.rowIndex = other.rowIndex;
        this.letter = other.letter;
        this.stableStrings = other.stableStrings;
        filteredColumns.clear();
        cursor = -2;
        for (int i = 0, n = memory.getColumnCount(); i < n; i++) {
            filteredColumns.add(filterIndexes.contains(i));
        }
        return this;
    }

    @Override
    public void setRowIndex(long rowIndex) {
        this.rowIndex = rowIndex;
        cursor++;
    }

    private long rowIndex(int columnIndex) {
        if (filteredColumns.get(columnIndex)) {
            return rowIndex;
        }
        return cursor;
    }

    @Override
    protected CharSequence getStr0(int columnIndex, DirectString csView) {
        final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
        if (dataPageAddress != 0) {
            final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
            final long auxPageLim = auxPageSizes.get(columnOffset + columnIndex);
            final long auxOffset = rowIndex(columnIndex) << 3;
            if (auxPageLim < auxOffset + 8) {
                throw CairoException.critical(0)
                        .put("string is outside of file boundary [auxOffset=")
                        .put(auxOffset)
                        .put(", auxPageLim=")
                        .put(auxPageLim)
                        .put(']');
            }
            final long dataPageLim = pageSizes.get(columnOffset + columnIndex);
            final long dataOffset = Unsafe.getUnsafe().getLong(auxPageAddress + auxOffset);
            return getStr(dataPageAddress, dataOffset, dataPageLim, csView);
        }
        return NullMemoryCMR.INSTANCE.getStrB(0);
    }

    @Override
    @Nullable
    protected Utf8Sequence getVarchar(int columnIndex, Utf8SplitString utf8View) {
        final long auxPageAddress = auxPageAddresses.get(columnOffset + columnIndex);
        if (auxPageAddress != 0) {
            final long auxPageLim = auxPageAddress + auxPageSizes.get(columnOffset + columnIndex);
            final long dataPageAddress = pageAddresses.get(columnOffset + columnIndex);
            final long dataPageLim = dataPageAddress + pageSizes.get(columnOffset + columnIndex);
            return VarcharTypeDriver.getSplitValue(
                    auxPageAddress,
                    auxPageLim,
                    dataPageAddress,
                    dataPageLim,
                    rowIndex(columnIndex),
                    utf8View
            );
        }
        return null;
    }
}
