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

package io.questdb.cairo;

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMR;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.cairo.vm.api.MemoryW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class ColumnVersionReader implements Closeable, Mutable {
    public static final int BLOCK_SIZE = 4;
    public static final int BLOCK_SIZE_BYTES = BLOCK_SIZE * Long.BYTES;
    public static final int BLOCK_SIZE_MSB = Numbers.msb(BLOCK_SIZE);
    // PARTITION_TIMESTAMP_OFFSET = 0;
    public static final int COLUMN_INDEX_OFFSET = 1;
    public static final int COLUMN_NAME_TXN_OFFSET = 2;
    public static final int COLUMN_TOP_OFFSET = 3;
    public static final long COL_TOP_DEFAULT_PARTITION = Long.MIN_VALUE;
    public static final int OFFSET_VERSION_64 = 0;
    public static final int OFFSET_OFFSET_A_64 = OFFSET_VERSION_64 + 8;
    public static final int OFFSET_SIZE_A_64 = OFFSET_OFFSET_A_64 + 8;
    public static final int OFFSET_OFFSET_B_64 = OFFSET_SIZE_A_64 + 8;
    public static final int OFFSET_SIZE_B_64 = OFFSET_OFFSET_B_64 + 8;
    public static final int HEADER_SIZE = OFFSET_SIZE_B_64 + 8;
    public static final long SYMBOL_TABLE_VERSION_PARTITION = COL_TOP_DEFAULT_PARTITION + 1;
    static final int TIMESTAMP_ADDED_PARTITION_OFFSET = COLUMN_TOP_OFFSET;
    private final static Log LOG = LogFactory.getLog(ColumnVersionReader.class);
    protected final LongList cachedColumnVersionList = new LongList();
    private MemoryCMR mem;
    private boolean ownMem;
    private long version;

    @Override
    public void clear() {
        if (ownMem) {
            mem.close();
        }
        cachedColumnVersionList.clear();
        version = -1;
    }

    @Override
    public void close() {
        clear();
    }

    public void dumpTo(MemoryW mem) {
        mem.putLong(OFFSET_VERSION_64, version);
        boolean areaA = (version & 1L) == 0L;
        final long offset = HEADER_SIZE;
        mem.putLong(areaA ? OFFSET_OFFSET_A_64 : OFFSET_OFFSET_B_64, offset);
        final long size = (long) (cachedColumnVersionList.size() / BLOCK_SIZE) * BLOCK_SIZE_BYTES;
        mem.putLong(areaA ? OFFSET_SIZE_A_64 : OFFSET_SIZE_B_64, size);

        int i = 0;
        long p = offset;
        long lim = offset + size;

        while (p < lim) {
            mem.putLong(p, cachedColumnVersionList.getQuick(i));
            mem.putLong(p + COLUMN_INDEX_OFFSET * Long.BYTES, cachedColumnVersionList.getQuick(i + COLUMN_INDEX_OFFSET));
            mem.putLong(p + COLUMN_NAME_TXN_OFFSET * Long.BYTES, cachedColumnVersionList.getQuick(i + COLUMN_NAME_TXN_OFFSET));
            mem.putLong(p + COLUMN_TOP_OFFSET * Long.BYTES, cachedColumnVersionList.getQuick(i + COLUMN_TOP_OFFSET));
            i += BLOCK_SIZE;
            p += BLOCK_SIZE_BYTES;
        }
    }

    public LongList getCachedColumnVersionList() {
        return cachedColumnVersionList;
    }

    /**
     * Packs a composite-partitioning cell key into the spare high 32 bits of the {@code
     * COLUMN_INDEX_OFFSET} slot, alongside the (low 32 bit) column index. {@link #BLOCK_SIZE} does not
     * change for {@code _cv} -- unlike the {@code _txn} attached-partition record (which widens its
     * stride for a composite table), {@code _cv} keeps its 4-long record and repurposes spare bits of
     * the existing column-index slot instead.
     * <p>
     * For a plain table {@code cellKey} is always 0, so {@code packColIndex(0, columnIndex) ==
     * columnIndex} exactly -- the packed value, and therefore the on-disk bytes, are byte-identical to
     * the pre-composite-partitioning layout.
     *
     * @param cellKey     dense per-(timePartition, dimension-tuple) cell ordinal; 0 for plain tables
     *                    and for the dormant cellKey-0 write path (multi-cell routing is Plan 4)
     * @param columnIndex writer column index
     * @return the packed value to store at {@code COLUMN_INDEX_OFFSET}
     */
    public static long packColIndex(int cellKey, int columnIndex) {
        return ((long) cellKey << 32) | (columnIndex & 0xFFFF_FFFFL);
    }

    /**
     * Recovers the column index from a value packed by {@link #packColIndex(int, int)}.
     */
    public static int unpackColumnIndex(long packedColumnIndex) {
        return (int) packedColumnIndex;
    }

    /**
     * Recovers the cell key from a value packed by {@link #packColIndex(int, int)}.
     */
    public static int unpackCellKey(long packedColumnIndex) {
        return (int) (packedColumnIndex >>> 32);
    }

    public long getColumnNameTxn(long partitionTimestamp, int columnIndex) {
        int versionRecordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        return versionRecordIndex > -1 ? cachedColumnVersionList.getQuick(versionRecordIndex + COLUMN_NAME_TXN_OFFSET) : getDefaultColumnNameTxn(columnIndex);
    }

    /**
     * Plan 4b Task 2: {@code cellKey}-aware counterpart of {@link #getColumnNameTxn(long, int)}, using
     * the cell-aware {@link #getRecordIndex(long, int, int)} lookup instead of the cellKey-0-only 2-arg
     * one. The DEFAULT-partition fallback ({@link #getDefaultColumnNameTxn(int)}) is deliberately NOT
     * made cellKey-aware here -- it answers a genuinely table-wide, cellKey-independent question ("what
     * txn did ALTER TABLE ADD COLUMN assign this column"), not a per-cell one; see {@link
     * TableWriter#addColumn} for the write-side half of this fix, which ensures every cell already
     * populated at ADD COLUMN time gets its OWN explicit {@code (ts, cellKey, col)} record so this
     * fallback is only ever consulted for a cell that genuinely never had pre-existing data at that
     * timestamp. For a plain table (or dormant composite, {@code cellKey == 0}) this is byte-identical to
     * the 2-arg overload.
     */
    public long getColumnNameTxn(long partitionTimestamp, int cellKey, int columnIndex) {
        int versionRecordIndex = getRecordIndex(partitionTimestamp, cellKey, columnIndex);
        return versionRecordIndex > -1 ? cachedColumnVersionList.getQuick(versionRecordIndex + COLUMN_NAME_TXN_OFFSET) : getDefaultColumnNameTxn(columnIndex);
    }

    public long getColumnNameTxnByIndex(int versionRecordIndex) {
        return versionRecordIndex > -1 ? cachedColumnVersionList.getQuick(versionRecordIndex + COLUMN_NAME_TXN_OFFSET) : -1L;
    }

    /**
     * Checks that column exists in the partition and returns the column top
     *
     * @param partitionTimestamp timestamp of the partition
     * @param columnIndex        column index
     * @return column top in the partition or -1 if column does not exist in the partition
     */
    public long getColumnTop(long partitionTimestamp, int columnIndex) {
        // Check if there is explicit record for this partitionTimestamp / columnIndex combination
        int recordIndex = getRecordIndex(partitionTimestamp, columnIndex);
        return getColumnTopByIndexOrDefault(recordIndex, partitionTimestamp, columnIndex, -1L);
    }

    /**
     * Plan 4b Task 2: {@code cellKey}-aware counterpart of {@link #getColumnTop(long, int)} -- see that
     * method's own docs. Used by the composite O3 merge path ({@code O3OpenColumnJob#appendMidPartition}/
     * {@code #mergeMidPartition}, via {@link TableWriter#getColumnTop(long, int, int, long)}) so a cell's
     * own pre-existing column top is never aliased to a DIFFERENT cell's record sharing the same
     * timestamp. For a plain table (or dormant composite, {@code cellKey == 0}) byte-identical to the
     * 2-arg overload.
     */
    public long getColumnTop(long partitionTimestamp, int cellKey, int columnIndex) {
        int recordIndex = getRecordIndex(partitionTimestamp, cellKey, columnIndex);
        return getColumnTopByIndexOrDefault(recordIndex, partitionTimestamp, columnIndex, -1L);
    }

    public long getColumnTopByIndex(int versionRecordIndex) {
        return versionRecordIndex > -1 ? cachedColumnVersionList.getQuick(versionRecordIndex + COLUMN_TOP_OFFSET) : 0L;
    }

    public long getColumnTopByIndexOrDefault(int recordIndex, long partitionTimestamp, int columnIndex, long defaultValue) {
        if (recordIndex > -1L) {
            return cachedColumnVersionList.getQuick(recordIndex + COLUMN_TOP_OFFSET);
        }

        // Check if column has been already added before this partition
        long columnTopDefaultPartition = getColumnTopPartitionTimestamp(columnIndex);
        if (columnTopDefaultPartition <= partitionTimestamp) {
            return 0;
        }

        // This column does not exist in the partition
        return defaultValue;
    }

    /**
     * Get partition when the column was added first into the table.
     * All partitions before that one should not have any data in the column
     * All partitions after that will have 0 column top (column fully exists)
     * Exception is when O3 commit can overwrite column top for any partition where the column did not exist
     * with concrete column top value
     *
     * @param columnIndex column index
     * @return the partition timestamp where column added or Long.MIN_VALUE if column was present from table creation
     */
    public long getColumnTopPartitionTimestamp(int columnIndex) {
        int index = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
        return index > -1 ? getColumnTopByIndex(index) : Long.MIN_VALUE;
    }

    /**
     * Returns the column top without checking that column exists in the partition
     *
     * @param partitionTimestamp timestamp of the partition
     * @param columnIndex        column index
     * @return column top in the partition or 0 if column does not exist in the partition or column exists with no column top
     */
    public long getColumnTopQuick(long partitionTimestamp, int columnIndex) {
        int index = getRecordIndex(partitionTimestamp, columnIndex);
        return getColumnTopByIndex(index);
    }

    public long getDefaultColumnNameTxn(int columnIndex) {
        int index = getRecordIndex(COL_TOP_DEFAULT_PARTITION, columnIndex);
        return index > -1 ? getColumnNameTxnByIndex(index) : -1L;
    }

    public long getMaxPartitionVersion(long partitionTimestamp) {
        return getMaxPartitionVersion(partitionTimestamp, 0);
    }

    /**
     * Plan 3 Task 5: {@code cellKey}-aware counterpart of {@link #getMaxPartitionVersion(long)}. A
     * partition "version" (max column name txn at a timestamp) is scoped per {@code (ts, cellKey)} --
     * the plain/dormant overload above delegates here with {@code cellKey = 0}.
     */
    public long getMaxPartitionVersion(long partitionTimestamp, int cellKey) {
        long maxVersion = -1;
        int index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index > -1) {
            final int sz = cachedColumnVersionList.size();
            for (; index < sz && cachedColumnVersionList.getQuick(index) == partitionTimestamp; index += BLOCK_SIZE) {
                final long thisTimestamp = cachedColumnVersionList.getQuick(index);
                if (thisTimestamp != partitionTimestamp) {
                    break;
                }
                final int thisCellKey = unpackCellKey(cachedColumnVersionList.getQuick(index + COLUMN_INDEX_OFFSET));
                if (thisCellKey == cellKey) {
                    final long columnVersion = cachedColumnVersionList.getQuick(index + COLUMN_NAME_TXN_OFFSET);
                    maxVersion = Math.max(maxVersion, columnVersion);
                } else if (thisCellKey > cellKey) {
                    // packed values are ascending within a ts run (cellKey is the high-order term),
                    // so no later record in this run can match cellKey either.
                    break;
                }
            }
        }
        return maxVersion;
    }

    public int getRecordIndex(long partitionTimestamp, int columnIndex) {
        return getRecordIndex(partitionTimestamp, 0, columnIndex);
    }

    /**
     * Plan 3 Task 5: {@code cellKey}-aware counterpart of {@link #getRecordIndex(long, int)}. The ts
     * binary search is unchanged (timestamp remains the primary sort key, independent of the cellKey
     * packed into the column-index slot); within that ts run, the scan compares the FULL packed
     * {@code (cellKey, columnIndex)} value, so a lookup can only match a record with the SAME cellKey
     * -- it cannot alias a same-column record belonging to a different cell.
     */
    public int getRecordIndex(long partitionTimestamp, int cellKey, int columnIndex) {
        int index = cachedColumnVersionList.binarySearchBlock(BLOCK_SIZE_MSB, partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (index > -1) {
            final long packedColumnIndex = packColIndex(cellKey, columnIndex);
            final int sz = cachedColumnVersionList.size();
            for (; index < sz && cachedColumnVersionList.getQuick(index) == partitionTimestamp; index += BLOCK_SIZE) {
                final long thisPacked = cachedColumnVersionList.getQuick(index + COLUMN_INDEX_OFFSET);
                final long thisTimestamp = cachedColumnVersionList.getQuick(index);
                if (thisTimestamp != partitionTimestamp) {
                    break;
                }

                if (thisPacked == packedColumnIndex) {
                    return index;
                }

                if (thisPacked > packedColumnIndex) {
                    break;
                }
            }
        }
        return -1;
    }

    /**
     * Symbol table files name txn - this is name suffix used to version the file group.
     * Whenever symbol table capacity changes, its version is increased. Separate column version
     * entry is used to store this version. Thus, decoupled from version of the columns.
     * <p>
     * Separate column version is optional however, when it is not present, this method will fall back to
     * {@link #getDefaultColumnNameTxn}.
     *
     * @param columnIndex symbol column index
     * @return version suffix
     */
    public long getSymbolTableNameTxn(int columnIndex) {
        int index = getRecordIndex(SYMBOL_TABLE_VERSION_PARTITION, columnIndex);
        return index > -1 ? getColumnNameTxnByIndex(index) : getDefaultColumnNameTxn(columnIndex);
    }

    public long getVersion() {
        return version;
    }

    public ColumnVersionReader ofRO(FilesFacade ff, LPSZ fileName) {
        version = -1;
        if (this.mem == null || !ownMem) {
            this.mem = Vm.getCMRInstance();
        }
        this.mem.of(ff, fileName, 0, HEADER_SIZE, MemoryTag.MMAP_TABLE_READER);
        ownMem = true;
        return this;
    }

    public void ofRO(MemoryCMR mem) {
        if (this.mem != null && ownMem) {
            this.mem.close();
        }
        this.mem = mem;
        ownMem = false;
        version = -1;
    }

    /**
     * Copies column versions from the given reader.
     */
    public void readFrom(ColumnVersionReader columnVersionReader) {
        this.version = columnVersionReader.version;
        cachedColumnVersionList.clear();
        cachedColumnVersionList.addAll(columnVersionReader.cachedColumnVersionList);
    }

    /**
     * Spins until the file reads consistently, or until {@code spinLockTimeout} milliseconds
     * have passed since the call. The timeout is a duration measured from here - not an
     * absolute deadline; a caller that hands over {@code clock.getTicks() + timeout} instead
     * buys a budget of roughly the current epoch, which no spin can ever exhaust.
     */
    public void readSafe(MillisecondClock microsecondClock, long spinLockTimeout) {
        final long tick = microsecondClock.getTicks();
        while (true) {
            if (readSafe()) {
                return;
            }

            if (microsecondClock.getTicks() - tick > spinLockTimeout) {
                LOG.error().$("Column Version read timeout [timeout=").$(spinLockTimeout).$("ms]").$();
                throw CairoException.critical(0).put("Column Version read timeout");
            }
            Os.pause();
            LOG.debug().$("read dirty version ").$(version).$(", retrying").$();
        }
    }

    public boolean readSafe() {
        long version = unsafeGetVersion();
        if (version == this.version) {
            return true;
        }
        Unsafe.loadFence();

        final long offset;
        final long size;

        final boolean areaA = (version & 1L) == 0;
        if (areaA) {
            offset = mem.getLong(OFFSET_OFFSET_A_64);
            size = mem.getLong(OFFSET_SIZE_A_64);
        } else {
            offset = mem.getLong(OFFSET_OFFSET_B_64);
            size = mem.getLong(OFFSET_SIZE_B_64);
        }

        Unsafe.loadFence();
        if (version == unsafeGetVersion()) {
            mem.resize(offset + size);
            readUnsafe(offset, size, cachedColumnVersionList, mem);

            Unsafe.loadFence();
            if (version == unsafeGetVersion()) {
                this.version = version;
                LOG.debug().$("read clean version ").$(version).$(", offset ").$(offset).$(", size ").$(size).$();
                return true;
            }
        }
        return false;
    }

    public long readUnsafe() {
        long version = mem.getLong(OFFSET_VERSION_64);

        boolean areaA = (version & 1L) == 0L;
        long offset = areaA ? mem.getLong(OFFSET_OFFSET_A_64) : mem.getLong(OFFSET_OFFSET_B_64);
        long size = areaA ? mem.getLong(OFFSET_SIZE_A_64) : mem.getLong(OFFSET_SIZE_B_64);
        mem.resize(offset + size);
        readUnsafe(offset, size, cachedColumnVersionList, mem);
        return version;
    }

    @Override
    public String toString() {
        // Used for debugging, don't use Misc.getThreadLocalSink() to not mess with other debugging values
        StringSink sink = new StringSink();
        sink.put("{[");
        for (int i = 0; i < cachedColumnVersionList.size(); i += BLOCK_SIZE) {
            long timestamp = cachedColumnVersionList.getQuick(i);
            int columnIndex = (int) cachedColumnVersionList.getQuick(i + COLUMN_INDEX_OFFSET);
            long columnNameTxn = cachedColumnVersionList.getQuick(i + COLUMN_NAME_TXN_OFFSET);
            long columnTop = cachedColumnVersionList.getQuick(i + COLUMN_TOP_OFFSET);

            if (i > 0) {
                sink.put(",");
            }
            sink.put("\n{columnIndex: ").put(columnIndex).put(", ");
            if (timestamp == COL_TOP_DEFAULT_PARTITION) {
                sink.putAscii("defaultNameTxn: ").put(columnNameTxn).putAscii(", ");
                sink.putAscii("addedPartition: ");
                sink.put(columnTop);
            } else if (timestamp == SYMBOL_TABLE_VERSION_PARTITION) {
                sink.putAscii("symbolTableTxn: ").put(columnNameTxn);
            } else {
                sink.putAscii("nameTxn: ").put(columnNameTxn).putAscii(", ");
                sink.putAscii("partition: ");
                sink.put(timestamp);
                sink.putAscii(", ");
                sink.putAscii("columnTop: ").put(columnTop);
            }
            sink.putAscii('}');
        }
        sink.putAscii("\n]}");
        return sink.toString();
    }

    private static void readUnsafe(long offset, long areaSize, LongList cachedList, MemoryR mem) {
        long lim = offset + areaSize;
        mem.extend(lim);
        int i = 0;
        long p = offset;

        assert areaSize % BLOCK_SIZE_BYTES == 0;

        cachedList.setPos((int) ((areaSize / BLOCK_SIZE_BYTES) * BLOCK_SIZE));

        while (p < lim) {
            cachedList.setQuick(i, mem.getLong(p));
            cachedList.setQuick(i + COLUMN_INDEX_OFFSET, mem.getLong(p + COLUMN_INDEX_OFFSET * Long.BYTES));
            cachedList.setQuick(i + COLUMN_NAME_TXN_OFFSET, mem.getLong(p + COLUMN_NAME_TXN_OFFSET * Long.BYTES));
            cachedList.setQuick(i + COLUMN_TOP_OFFSET, mem.getLong(p + COLUMN_TOP_OFFSET * Long.BYTES));
            i += BLOCK_SIZE;
            p += BLOCK_SIZE_BYTES;
        }
    }

    private long unsafeGetVersion() {
        return mem.getLong(OFFSET_VERSION_64);
    }
}
