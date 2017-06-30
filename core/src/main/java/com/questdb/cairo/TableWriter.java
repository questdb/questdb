/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.misc.*;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.Dates;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

public class TableWriter implements Closeable {

    private static final int _16M = 16 * 1024 * 1024;
    private static final long OFFSET_TXN = 0;
    private static final long OFFSET_TRANSIENT_ROW_COUNT = 8;
    private static final long OFFSET_FIXED_ROW_COUNT = 16;
    private static final long OFFSET_MAX_TIMESTAMP = 24;
    private static CharSequenceHashSet truncateIgnores = new CharSequenceHashSet();
    private final ObjList<AppendMemory> columns = new ObjList<>();
    private final CompositePath path;
    private final LongList refs = new LongList();
    private final Row row = new Row();
    private final int rootLen;
    private final ReadWriteMemory txMem;
    private final ReadOnlyMemory metaMem;
    private final VirtualMemory columnSizeMem;
    private final int columnCount;
    private final int partitionBy;
    private final RowFunction switchPartitionFunction = new SwitchPartitionRowFunction();
    private final RowFunction openPartitionFunction = new OpenPartitionRowFunction();
    private final RowFunction noPartitionFunction = new NoPartitionFunction();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final FilesFacade ff;
    private Runnable[] nullers;
    private int mode = 509;
    private long fixedRowCount = 0;
    private long txn;

    private RowFunction rowFunction = openPartitionFunction;
    private long prevTimestamp;
    private long prevTransientRowCount;
    private long maxTimestamp;
    private long partitionLo;
    private long partitionHi;
    private int txPartitionCount = 0;
    private long transientRowCount = 0;
    private long masterRef = 0;
    private boolean removeDirOnCancelRow = true;

    public TableWriter(FilesFacade ff, CharSequence root, CharSequence name) {
        this.ff = ff;
        this.path = new CompositePath().of(root).concat(name);
        this.rootLen = path.length();
        try {
            path.concat("_meta").$();
            if (!ff.exists(path)) {
                throw CairoException.instance(0).put("File does not exist: ").put(path);
            }
            this.metaMem = new ReadOnlyMemory(ff);
            metaMem.of(path, ff.getPageSize(), ff.length(path));
            this.txMem = new ReadWriteMemory(ff);
            this.columnCount = metaMem.getInt(0);
            this.partitionBy = metaMem.getInt(4);
            this.columnSizeMem = new VirtualMemory(ff.getPageSize());
            this.refs.extendAndSet(columnCount, 0);
            this.nullers = new Runnable[columnCount];
            path.trimTo(rootLen);
            configureColumnMemory();
            configureAppendPosition();
        } catch (CairoException e) {
            close0();
            throw e;
        }
    }

    @Override
    public void close() {
        close0();
    }

    public void commit() {
        if ((masterRef & 1) != 0) {
            cancelRow();
        }

        txMem.jumpTo(OFFSET_TRANSIENT_ROW_COUNT);
        txMem.putLong(transientRowCount);

        if (txPartitionCount > 1) {
            commitPendingPartitions();
            txMem.putLong(fixedRowCount);
            txMem.putLong(maxTimestamp);
        }

        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(OFFSET_TXN);
        txMem.putLong(txn++);
        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(32);
    }

    public Row newRow(long timestamp) {
        return rowFunction.newRow(timestamp);
    }

    public long size() {
        return fixedRowCount + transientRowCount;
    }

    /**
     * Truncates table. When operation is unsuccessful it throws CairoException. With that truncate can be
     * retried or alternatively table can be closed. Outcome of any other operation with the table is undefined
     * and likely to cause segmentation fault. When table re-opens any partial truncate will be retried.
     * <p>
     * todo: table must be able to recover if closed after unsuccessful truncate
     */
    public void truncate() {

        if (size() == 0) {
            return;
        }

        for (int i = 0; i < columnCount; i++) {
            getPrimaryColumn(i).truncate();
            AppendMemory mem = getSecondaryColumn(i);
            if (mem != null) {
                mem.truncate();
            }
        }

        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirectories();
            rowFunction = openPartitionFunction;
        }

        prevTimestamp = Long.MIN_VALUE;
        maxTimestamp = Long.MIN_VALUE;
        partitionLo = Long.MIN_VALUE;
        prevTransientRowCount = 0;
        transientRowCount = 0;
        fixedRowCount = 0;
        txn = 0;
        txPartitionCount = 1;

        txMem.jumpTo(OFFSET_TXN);
        TableUtils.resetTxn(txMem);
    }

    private void bumpMasterRef() {
        if ((masterRef & 1) != 0) {
            cancelRow();
        }
        masterRef++;
    }

    private void cancelRow() {

        if ((masterRef & 1) == 0) {
            return;
        }

        if (transientRowCount == 0) {
            if (partitionBy != PartitionBy.NONE) {
                // we have to undo creation of partition
                try {
                    if (removeDirOnCancelRow) {
                        setStateForTimestamp(maxTimestamp, false);
                        if (!ff.rmdir(path.$())) {
                            throw CairoException.instance(Os.errno()).put("Cannot remove directory: ").put(path);
                        }
                        removeDirOnCancelRow = false;
                    }

                    // open old partition
                    if (prevTimestamp > Long.MIN_VALUE) {
                        columnSizeMem.jumpTo((txPartitionCount - 2) * 16);
                        setStateForTimestamp(prevTimestamp, true);
                        setAppendPosition(prevTransientRowCount);
                        txPartitionCount--;
                    } else {
                        rowFunction = openPartitionFunction;
                    }

                    // undo counts
                    transientRowCount = prevTransientRowCount;
                    fixedRowCount -= prevTransientRowCount;
                    maxTimestamp = prevTimestamp;
                    removeDirOnCancelRow = true;
                } finally {
                    path.trimTo(rootLen);
                }
            } else {
                // we only have one partition, jump to start on every column
                for (int i = 0; i < columnCount; i++) {
                    getPrimaryColumn(i).jumpTo(0);
                    AppendMemory mem = getSecondaryColumn(i);
                    if (mem != null) {
                        mem.jumpTo(0);
                    }
                }
            }
        } else {
            maxTimestamp = prevTimestamp;
            // we are staying within same partition, prepare append positions for row count
            boolean rowChanged = false;
            // verify if any of the columns have been changed
            // if not - we don't have to do
            for (int i = 0; i < columnCount; i++) {
                if (refs.getQuick(i) == masterRef) {
                    rowChanged = true;
                    break;
                }
            }

            // is no column has been changed we take easy option and do nothing
            if (rowChanged) {
                setAppendPosition(transientRowCount);
            }
        }
        masterRef--;
    }

    private void close0() {
        closeColumns();
        Misc.free(txMem);
        Misc.free(metaMem);
        Misc.free(columnSizeMem);
        Misc.free(path);
    }

    private void closeColumns() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            AppendMemory m = columns.getQuick(i);
            if (m != null) {
                m.close();
            }
        }
    }

    private void commitPendingPartitions() {
        long offset = 8;
        for (int i = 0; i < txPartitionCount - 1; i++) {
            try {
                long partitionTimestamp = columnSizeMem.getLong(offset);
                offset += 8;
                setStateForTimestamp(partitionTimestamp, false);
                path.concat("_archive").$();

                long fd = ff.openAppend(path);

                if (fd == -1) {
                    throw CairoException.instance(ff.errno()).put("Cannot open for append: ").put(path);
                }
                try {
                    int len = 8;
                    while (len > 0) {
                        long l = Math.min(len, columnSizeMem.pageRemaining(offset));
                        if (ff.write(fd, columnSizeMem.addressOf(offset), l, 0) == -1) {
                            throw CairoException.instance(ff.errno()).put("Commit failed, file=").put(path);
                        }
                        len -= l;
                        offset += l;
                    }
                } finally {
                    ff.close(fd);
                }
            } finally {
                path.trimTo(rootLen);
            }
        }
        columnSizeMem.jumpTo(0);
    }

    private void configureAppendPosition() {
        path.concat("_txi").$();
        try {
            if (ff.exists(path)) {
                txMem.of(path, ff.getPageSize(), 0, ff.getPageSize());
                path.trimTo(rootLen);
                this.txn = txMem.getLong(OFFSET_TXN);
                this.transientRowCount = txMem.getLong(OFFSET_TRANSIENT_ROW_COUNT);
                this.fixedRowCount = txMem.getLong(OFFSET_FIXED_ROW_COUNT);
                long timestamp = txMem.getLong(OFFSET_MAX_TIMESTAMP);
                if (timestamp > Long.MIN_VALUE || partitionBy == PartitionBy.NONE) {
                    openFirstPartition(timestamp);
                    if (partitionBy == PartitionBy.NONE) {
                        rowFunction = noPartitionFunction;
                    } else {
                        rowFunction = switchPartitionFunction;
                    }
                } else {
                    maxTimestamp = timestamp;
                    rowFunction = openPartitionFunction;
                }
            } else {
                throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void configureColumnMemory() {
        for (int i = 0; i < columnCount; i++) {
            final int type = getColumnType(i);
            final AppendMemory primary = new AppendMemory(ff);
            final AppendMemory secondary;
            switch (getColumnType(i)) {
                case ColumnType.BINARY:
                case ColumnType.SYMBOL:
                case ColumnType.STRING:
                    secondary = new AppendMemory(ff);
                    break;
                default:
                    secondary = null;
                    break;
            }
            columns.add(primary);
            columns.add(secondary);
            configureNuller(i, type, primary, secondary);
        }
    }

    private void configureNuller(int index, int type, AppendMemory mem1, AppendMemory mem2) {
        switch (type) {
            case ColumnType.STRING:
                nullers[index] = () -> mem2.putLong(mem1.putNullStr());
                break;
            case ColumnType.SYMBOL:
                nullers[index] = () -> mem1.putInt(SymbolTable.VALUE_IS_NULL);
                break;
            case ColumnType.INT:
                nullers[index] = () -> mem1.putInt(Numbers.INT_NaN);
                break;
            case ColumnType.FLOAT:
                nullers[index] = () -> mem1.putFloat(Float.NaN);
                break;
            case ColumnType.DOUBLE:
                nullers[index] = () -> mem1.putDouble(Double.NaN);
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
                nullers[index] = () -> mem1.putLong(Numbers.LONG_NaN);
                break;
            case ColumnType.BINARY:
                nullers[index] = () -> mem2.putLong(mem1.putNullBin());
                break;
            default:
                break;
        }
    }

    private LPSZ dFile(CharSequence columnName) {
        return path.concat(columnName).put(".d").$();
    }

    private long getColumnNameOffset() {
        return 8 + columnCount * 4;
    }

    private int getColumnType(int columnIndex) {
        return metaMem.getInt(8 + columnIndex * 4);
    }

    private long getMapPageSize() {
        long pageSize = ff.getPageSize() * ff.getPageSize();
        if (pageSize < 0 || pageSize > _16M) {
            if (_16M % ff.getPageSize() == 0) {
                return _16M;
            }
            return ff.getPageSize();
        } else {
            return pageSize;
        }
    }

    private AppendMemory getPrimaryColumn(int column) {
        return columns.getQuick(column * 2);
    }

    private AppendMemory getSecondaryColumn(int column) {
        return columns.getQuick(column * 2 + 1);
    }

    private LPSZ iFile(CharSequence columnName) {
        return path.concat(columnName).put(".i").$();
    }

    private void openFirstPartition(long timestamp) {
        openPartition(timestamp);
        setAppendPosition(transientRowCount);
        txPartitionCount = 1;
    }

    private void openPartition(long timestamp) {
        try {
            setStateForTimestamp(timestamp, true);
            int plen = path.length();
            if (ff.mkdirs(path.put(Path.SEPARATOR).$(), mode) != 0) {
                path.trimTo(plen);
                throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
            }
            assert columnCount > 0;
            long nameOffset = getColumnNameOffset();
            for (int i = 0; i < columnCount; i++) {
                AppendMemory mem1 = getPrimaryColumn(i);
                AppendMemory mem2 = getSecondaryColumn(i);

                CharSequence name = metaMem.getStr(nameOffset);
                nameOffset += VirtualMemory.getStorageLength(name);

                path.trimTo(plen);
                mem1.of(dFile(name), getMapPageSize());

                if (mem2 != null) {
                    path.trimTo(plen);
                    mem2.of(iFile(name), getMapPageSize());
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirectories() {
        for (int i = 0; i < columnCount; i++) {
            getPrimaryColumn(i).close();
            AppendMemory mem = getSecondaryColumn(i);
            if (mem != null) {
                mem.close();
            }
        }

        try {
            long p = ff.findFirst(path.$());
            if (p > 0) {
                try {
                    do {
                        long pName = ff.findName(p);
                        path.trimTo(rootLen);
                        path.concat(pName).$();
                        nativeLPSZ.of(pName);
                        if (!truncateIgnores.contains(nativeLPSZ) && !ff.rmdir(path)) {
                            throw CairoException.instance(ff.errno()).put("Cannot remove directory: ").put(path);
                        }
                    } while (ff.findNext(p));
                } finally {
                    ff.findClose(p);
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void setAppendPosition(long position) {
        long pSz = Unsafe.malloc(8);
        try {
            for (int i = 0; i < columnCount; i++) {
                AppendMemory mem1 = getPrimaryColumn(i);
                AppendMemory mem2 = getSecondaryColumn(i);

                int type = getColumnType(i);
                long offset;

                if (position > 0) {
                    switch (type) {
                        case ColumnType.BINARY:
                            assert mem2 != null;
                            if (ff.read(mem2.getFd(), pSz, 8, (position - 1) * 8) != 8) {
                                throw CairoException.instance(ff.errno()).put("Cannot read offset, fd=").put(mem2.getFd()).put(", offset=").put((position - 1) * 8);
                            }
                            offset = Unsafe.getUnsafe().getLong(pSz);
                            if (ff.read(mem1.getFd(), pSz, 8, offset) != 8) {
                                throw CairoException.instance(ff.errno()).put("Cannot read length, fd=").put(mem1.getFd()).put(", offset=").put(offset);
                            }
                            mem1.setSize(offset + Unsafe.getUnsafe().getLong(pSz));
                            break;
                        case ColumnType.STRING:
                        case ColumnType.SYMBOL:
                            assert mem2 != null;
                            if (ff.read(mem2.getFd(), pSz, 8, (position - 1) * 8) != 8) {
                                throw CairoException.instance(ff.errno()).put("Cannot read offset, fd=").put(mem2.getFd()).put(", offset=").put((position - 1) * 8);
                            }
                            offset = Unsafe.getUnsafe().getLong(pSz);
                            if (ff.read(mem1.getFd(), pSz, 4, offset) != 4) {
                                throw CairoException.instance(ff.errno()).put("Cannot read length, fd=").put(mem1.getFd()).put(", offset=").put(offset);
                            }
                            mem1.setSize(offset + Unsafe.getUnsafe().getInt(pSz));
                            break;
                        default:
                            mem1.setSize(position * ColumnType.sizeOf(type));
                            break;
                    }
                } else {
                    mem1.setSize(0);
                    if (mem2 != null) {
                        mem2.setSize(0);
                    }
                }
            }
        } finally {
            Unsafe.free(pSz, 8);
        }
    }

    /**
     * Sets path member variable to partition directory for the given timestamp and
     * partitionLo and partitionHi to partition interval in millis. These values are
     * determined based on input timestamp and value of partitionBy. For any given
     * timestamp this method will determine either day, month or year interval timestamp falls to.
     * Partition directory name is ISO string of interval start.
     * <p>
     * Because this method modifies "path" member variable, be sure path is trimmed to original
     * state withing try..finally block.
     *
     * @param timestamp               to determine interval for
     * @param updatePartitionInterval flag indicating that partition interval partitionLo and
     *                                partitionHi have to be updated as well.
     */
    private void setStateForTimestamp(long timestamp, boolean updatePartitionInterval) {
        int y, m, d;
        boolean leap;
        path.put(Path.SEPARATOR);
        switch (partitionBy) {
            case PartitionBy.DAY:
                y = Dates.getYear(timestamp);
                leap = Dates.isLeapYear(y);
                m = Dates.getMonthOfYear(timestamp, y, leap);
                d = Dates.getDayOfMonth(timestamp, y, m, leap);
                DateFormatUtils.append000(path, y);
                path.put('-');
                DateFormatUtils.append0(path, m);
                path.put('-');
                DateFormatUtils.append0(path, d);

                if (updatePartitionInterval) {
                    partitionLo = Dates.yearMillis(y, leap);
                    partitionLo += Dates.monthOfYearMillis(m, leap);
                    partitionLo += (d - 1) * Dates.DAY_MILLIS;
                    partitionHi = partitionLo + 24 * Dates.HOUR_MILLIS;
                }
                break;
            case PartitionBy.MONTH:
                y = Dates.getYear(timestamp);
                leap = Dates.isLeapYear(y);
                m = Dates.getMonthOfYear(timestamp, y, leap);
                DateFormatUtils.append000(path, y);
                path.put('-');
                DateFormatUtils.append0(path, m);

                if (updatePartitionInterval) {
                    partitionLo = Dates.yearMillis(y, leap);
                    partitionLo += Dates.monthOfYearMillis(m, leap);
                    partitionHi = partitionLo + Dates.getDaysPerMonth(m, leap) * 24L * Dates.HOUR_MILLIS;
                }
                break;
            case PartitionBy.YEAR:
                y = Dates.getYear(timestamp);
                leap = Dates.isLeapYear(y);
                DateFormatUtils.append000(path, y);
                if (updatePartitionInterval) {
                    partitionLo = Dates.yearMillis(y, leap);
                    partitionHi = Dates.addYear(partitionLo, 1);
                }
                break;
            default:
                path.put("default");
                partitionLo = Long.MIN_VALUE;
                partitionHi = Long.MAX_VALUE;
        }
    }

    private void switchPartition(long timestamp) {
        // we need to store reference on partition so that archive
        // file can be created in appropriate directory
        // for simplicity use partitionLo, which can be
        // translated to directory name when needed
        if (txPartitionCount++ > 0) {
            columnSizeMem.putLong(transientRowCount);
            columnSizeMem.putLong(maxTimestamp);
        }
        fixedRowCount += transientRowCount;
        prevTransientRowCount = transientRowCount;
        transientRowCount = 0;
        openPartition(timestamp);
        setAppendPosition(0);
    }

    private void updateMaxTimestamp(long timestamp) {
        this.prevTimestamp = maxTimestamp;
        this.maxTimestamp = timestamp;
    }

    /**
     *
     */
    @FunctionalInterface
    public interface RowFunction {
        Row newRow(long timestamp);
    }

    private class OpenPartitionRowFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            if (maxTimestamp == Long.MIN_VALUE) {
                openFirstPartition(timestamp);
            }
            return (rowFunction = switchPartitionFunction).newRow(timestamp);
        }
    }

    private class NoPartitionFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            if (timestamp < maxTimestamp) {
                throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
            }
            updateMaxTimestamp(timestamp);
            return row;
        }
    }

    private class SwitchPartitionRowFunction implements RowFunction {
        @NotNull
        private Row newRow0(long timestamp) {
            if (timestamp < maxTimestamp) {
                throw CairoException.instance(ff.errno()).put("Cannot insert rows out of order. Table=").put(path);
            }

            if (timestamp >= partitionHi && partitionBy != PartitionBy.NONE) {
                switchPartition(timestamp);
            }

            updateMaxTimestamp(timestamp);
            return row;
        }

        @Override
        public Row newRow(long timestamp) {
            bumpMasterRef();
            if (timestamp < partitionHi && timestamp >= maxTimestamp) {
                updateMaxTimestamp(timestamp);
                return row;
            }
            return newRow0(timestamp);
        }
    }

    public class Row {
        public void append() {
            if ((masterRef & 1) == 0) {
                return;
            }

            for (int i = 0; i < columnCount; i++) {
                if (refs.getQuick(i) < masterRef) {
                    Unsafe.arrayGet(nullers, i).run();
                }
            }
            transientRowCount++;
            masterRef++;
        }

        public void cancel() {
            cancelRow();
        }

        public void putDate(int index, long value) {
            putLong(index, value);
        }

        public void putDouble(int index, double value) {
            getPrimaryColumn(index).putDouble(value);
            notNull(index);
        }

        public void putInt(int index, int value) {
            getPrimaryColumn(index).putInt(value);
            notNull(index);
        }

        public void putLong(int index, long value) {
            getPrimaryColumn(index).putLong(value);
            notNull(index);
        }

        public void putStr(int index, CharSequence value) {
            getSecondaryColumn(index).putLong(getPrimaryColumn(index).putStr(value));
            notNull(index);
        }

        private void notNull(int index) {
            refs.setQuick(index, masterRef);
        }
    }

    static {
        truncateIgnores.add("..");
        truncateIgnores.add(".");
        truncateIgnores.add("_meta");
        truncateIgnores.add("_txi");
    }
}
