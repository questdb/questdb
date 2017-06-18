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
import com.questdb.misc.Files;
import com.questdb.misc.Numbers;
import com.questdb.misc.Unsafe;
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
    private static CharSequenceHashSet truncateIgnores = new CharSequenceHashSet();
    private final ObjList<AppendMemory> columns = new ObjList<>();
    private final CompositePath path;
    private final LongList refs = new LongList();
    private final Row row = new Row();
    private final int rootLen;
    private final ReadWriteMemory txMem = new ReadWriteMemory();
    private final ReadOnlyMemory metaMem = new ReadOnlyMemory();
    private final VirtualMemory columnSizeMem = new VirtualMemory(Files.PAGE_SIZE);
    private final int columnCount;
    private final int partitionBy;
    private final RowFunction switchPartitionFunction = new SwitchPartitionRowFunction();
    private final RowFunction openPartitionFunction = new OpenPartitionRowFunction();
    private final RowFunction noPartitionFunction = new NoPartitionFunction();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private long masterRef = 0;
    private Runnable[] nullers;
    private int mode = 509;
    private long partitionLo;
    private long partitionHi;
    private int txPartitionCount = 0;
    private long transientRowCount = 0;
    private long fixedRowCount = 0;
    private long txn;
    private RowFunction rowFunction = openPartitionFunction;

    public TableWriter(CharSequence root, CharSequence name) {
        this.path = new CompositePath().of(root).concat(name);
        this.rootLen = path.length();
        metaMem.of(path.concat("_meta").$(), Files.PAGE_SIZE, 0);
        this.columnCount = metaMem.getInt(0);
        this.partitionBy = metaMem.getInt(4);
        this.refs.extendAndSet(columnCount, 0);
        this.nullers = new Runnable[columnCount];
        path.trimTo(rootLen);
        configureColumnMemory();
        configureAppendPosition();
    }

    @Override
    public void close() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            AppendMemory m = columns.getQuick(i);
            if (m != null) {
                m.close();
            }
        }
        txMem.close();
        metaMem.close();
        columnSizeMem.close();
        path.close();
    }

    public void commit() {
        txMem.jumpTo(8);
        txMem.putLong(transientRowCount);

        if (txPartitionCount > 1) {
            commitPendingPartitions();
            txMem.putLong(fixedRowCount);
            txMem.putLong(partitionLo);
        }

        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(0);
        txMem.putLong(txn++);
        Unsafe.getUnsafe().storeFence();
    }

    public Row newRow(long timestamp) {
        return rowFunction.newRow(timestamp);
    }

    public long size() {
        return fixedRowCount + transientRowCount;
    }

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

        partitionLo = Long.MIN_VALUE;
        transientRowCount = 0;
        fixedRowCount = 0;
        txn = 0;
        txPartitionCount = 1;

        txMem.jumpTo(0);
        TableUtils.resetTxn(txMem);
        txPartitionCount = 1;
    }

    private void commitPendingPartitions() {
        long offset = 8;
        for (int i = 0; i < txPartitionCount - 1; i++) {
            long partitionTimestamp = columnSizeMem.getLong(offset);
            offset += 8;
            setStateForTimestamp(partitionTimestamp, false);
            path.concat("_archive").$();

            long fd = Files.openAppend(path);
            try {
                int len = 8;
                while (len > 0) {
                    long l = Math.min(len, columnSizeMem.pageRemaining(offset));
                    if (Files.write(fd, columnSizeMem.addressOf(offset), l, 0) == -1) {
                        throw new RuntimeException("commit failed");
                    }
                    len -= l;
                    offset += l;
                }
            } finally {
                Files.close(fd);
                path.trimTo(rootLen);
            }
        }
        columnSizeMem.jumpTo(0);
    }

    private void configureAppendPosition() {
        path.concat("_txi").$();
        try {
            if (Files.exists(path)) {
                txMem.of(path, Files.PAGE_SIZE, 0, Files.PAGE_SIZE);
                path.trimTo(rootLen);
                this.txn = txMem.getLong(0);
                this.transientRowCount = txMem.getLong(8);
                this.fixedRowCount = txMem.getLong(16);
                long timestamp = txMem.getLong(24);
                if (timestamp > Long.MIN_VALUE || partitionBy == PartitionBy.NONE) {
                    openPartition(timestamp);
                    if (partitionBy == PartitionBy.NONE) {
                        rowFunction = noPartitionFunction;
                    } else {
                        rowFunction = switchPartitionFunction;
                    }
                } else {
                    partitionLo = timestamp;
                    rowFunction = openPartitionFunction;
                }
            } else {
                throw new RuntimeException("doesnt exist");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void configureColumnMemory() {
        for (int i = 0; i < columnCount; i++) {
            columns.add(new AppendMemory());
            switch (getColumnType(i)) {
                case ColumnType.BINARY:
                case ColumnType.SYMBOL:
                case ColumnType.STRING:
                    columns.add(new AppendMemory());
                    break;
                default:
                    columns.add(null);
                    break;
            }
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
        long pageSize = Files.PAGE_SIZE * Files.PAGE_SIZE;
        if (pageSize < 0 || pageSize > _16M) {
            if (_16M % Files.PAGE_SIZE == 0) {
                return _16M;
            }
            return Files.PAGE_SIZE;
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

    private void openPartition(long timestamp) {
        setStateForTimestamp(timestamp, true);

        int plen = path.length();
        if (Files.mkdirs(path.put(Path.SEPARATOR).$(), mode) != 0) {
            throw new RuntimeException("cannot create def partition");
        }
        path.trimTo(plen);
        assert columnCount > 0;

        long nameOffset = getColumnNameOffset();

        long pSz = Unsafe.malloc(8);
        try {
            for (int i = 0; i < columnCount; i++) {
                AppendMemory mem1 = getPrimaryColumn(i);
                AppendMemory mem2 = getSecondaryColumn(i);

                int type = getColumnType(i);

                CharSequence name = metaMem.getStr(nameOffset);
                nameOffset += VirtualMemory.getStorageLength(name);

                switch (type) {
                    case ColumnType.BINARY:
                        assert mem2 != null;
                        mem2.of(iFile(name), getMapPageSize(), transientRowCount * 8);
                        path.trimTo(plen);
                        mem1.of(dFile(name), getMapPageSize(), 0);
                        path.trimTo(plen);

                        if (transientRowCount > 0) {
                            long varOffset = mem2.getLong((transientRowCount - 1) * 8);
                            if (Files.read(mem1.getFd(), pSz, 8, varOffset) != 8) {
                                throw new RuntimeException("cannot read len");
                            }
                            mem1.jumpTo(varOffset + Unsafe.getUnsafe().getLong(pSz));
                        }
                        break;
                    case ColumnType.STRING:
                    case ColumnType.SYMBOL:
                        assert mem2 != null;
                        mem2.of(iFile(name), getMapPageSize(), transientRowCount * 8);
                        path.trimTo(plen);
                        mem1.of(dFile(name), getMapPageSize());
                        path.trimTo(plen);
                        if (transientRowCount > 0) {
                            if (Files.read(mem2.getFd(), pSz, 8, (transientRowCount - 1) * 8) != 8) {
                                throw new RuntimeException("cannot read offset");
                            }
                            long offset = Unsafe.getUnsafe().getLong(pSz);
                            if (Files.read(mem1.getFd(), pSz, 4, offset) != 4) {
                                throw new RuntimeException("cannot read len");
                            }
                            mem1.setSize(offset + Unsafe.getUnsafe().getInt(pSz));
                        }
                        break;
                    default:
                        mem1.of(path.concat(name).put(".d").$(), getMapPageSize(), transientRowCount * ColumnType.sizeOf(type));
                        path.trimTo(plen);
                        break;
                }
                // set nullers
                configureNuller(i, type, mem1, mem2);
            }
        } finally {
            Unsafe.free(pSz, 8);
            path.trimTo(rootLen);
        }
        txPartitionCount = 1;
    }

    private void removePartitionDirectories() {
        for (int i = 0; i < columnCount; i++) {
            getPrimaryColumn(i).close();
            AppendMemory mem = getSecondaryColumn(i);
            if (mem != null) {
                mem.close();
            }
        }

        long p = Files.findFirst(path.$());
        if (p > 0) {
            try {
                do {
                    long pName = Files.findName(p);
                    path.trimTo(rootLen);
                    path.concat(pName).$();
                    nativeLPSZ.of(pName);
                    if (!truncateIgnores.contains(nativeLPSZ) && !Files.rmdir(path)) {
                        throw new RuntimeException("cannot remove");
                    }
                } while (Files.findNext(p));
            } finally {
                Files.findClose(p);
            }
        }
        path.trimTo(rootLen);
    }

    private boolean setStateForTimestamp(long timestamp, boolean updatePartitionRange) {
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

                if (updatePartitionRange) {
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

                if (updatePartitionRange) {
                    partitionLo = Dates.yearMillis(y, leap);
                    partitionLo += Dates.monthOfYearMillis(m, leap);
                    partitionHi = partitionLo + Dates.getDaysPerMonth(m, leap) * 24L * Dates.HOUR_MILLIS;
                }
                break;
            case PartitionBy.YEAR:
                y = Dates.getYear(timestamp);
                leap = Dates.isLeapYear(y);
                DateFormatUtils.append000(path, y);
                if (updatePartitionRange) {
                    partitionLo = Dates.yearMillis(y, leap);
                    partitionHi = Dates.addYear(partitionLo, 1);
                }
                break;
            default:
                path.put("default");
                partitionLo = Long.MIN_VALUE;
                partitionHi = Long.MAX_VALUE;
                return false;
        }
        return true;
    }

    private void switchPartition(long timestamp) {
        // we need to store reference on partition so that archive
        // file can be created in appropriate directory
        // for simplicity use partitionLo, which can be
        // translated to directory name when needed
        long partitionLo = this.partitionLo;
        if (setStateForTimestamp(timestamp, true)) {
            if (txPartitionCount++ > 0) {
                columnSizeMem.putLong(transientRowCount);
                columnSizeMem.putLong(partitionLo);
            }
            fixedRowCount += transientRowCount;
            transientRowCount = 0;
            switchPartition();
            path.trimTo(rootLen);
        }
    }

    private void switchPartition() {
        int partitionPathLen = path.length();
        if (Files.mkdirs(path.put(Path.SEPARATOR).$(), mode) != 0) {
            throw new RuntimeException("cannot create partition");
        }

        assert columnCount > 0;
        long nameOffset = getColumnNameOffset();
        for (int i = 0; i < columnCount; i++) {
            AppendMemory mem1 = getPrimaryColumn(i);
            AppendMemory mem2 = getSecondaryColumn(i);

            CharSequence name = metaMem.getStr(nameOffset);
            nameOffset += VirtualMemory.getStorageLength(name);

            path.trimTo(partitionPathLen);
            mem1.of(dFile(name), getMapPageSize(), 0);

            if (mem2 != null) {
                path.trimTo(partitionPathLen);
                mem2.of(iFile(name), getMapPageSize(), 0);
            }
        }
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
            if (partitionLo == Long.MIN_VALUE) {
                openPartition(timestamp);
            }
            return (rowFunction = switchPartitionFunction).newRow(timestamp);
        }
    }

    private class NoPartitionFunction implements RowFunction {
        @Override
        public Row newRow(long timestamp) {
            masterRef++;
            if (timestamp < partitionLo) {
                throw new RuntimeException("outof order");
            }
            partitionLo = timestamp;
            return row;
        }
    }

    private class SwitchPartitionRowFunction implements RowFunction {
        @NotNull
        private Row newRow0(long timestamp) {
            if (timestamp < partitionLo) {
                throw new RuntimeException("out of order");
            }

            if (timestamp > partitionHi) {
                switchPartition(timestamp);
            }
            partitionLo = timestamp;
            return row;
        }

        @Override
        public Row newRow(long timestamp) {
            masterRef++;
            if (timestamp < partitionHi && timestamp >= partitionLo) {
                partitionLo = timestamp;
                return row;
            }
            return newRow0(timestamp);
        }


    }

    public class Row {
        public void append() {
            for (int i = 0; i < columnCount; i++) {
                if (refs.getQuick(i) < masterRef) {
                    Unsafe.arrayGet(nullers, i).run();
                }
            }
            transientRowCount++;
        }

        public void putDate(int index, long value) {
            putLong(index, value);
        }

        public void putDouble(int index, double value) {
            getPrimaryColumn(index).putDouble(value);
            refs.setQuick(index, masterRef);
        }

        public void putInt(int index, int value) {
            getPrimaryColumn(index).putInt(value);
            refs.setQuick(index, masterRef);
        }

        public void putLong(int index, long value) {
            getPrimaryColumn(index).putLong(value);
            refs.setQuick(index, masterRef);
        }

        public void putStr(int index, CharSequence value) {
            long r = getPrimaryColumn(index).putStr(value);
            getSecondaryColumn(index).putLong(r);
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
