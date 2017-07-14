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
import com.questdb.ex.NumericException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.LongList;
import com.questdb.std.ObjList;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.*;
import com.questdb.store.ColumnType;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.function.LongConsumer;

public class TableWriter implements Closeable {

    public static final int TODO_TRUNCATE = 1;
    public static final int TODO_RESTORE_META = 2;
    static final String TXN_FILE_NAME = "_txn";
    static final String META_FILE_NAME = "_meta";
    static final String META_SWAP_FILE_NAME = "_meta.swp";
    static final String META_PREV_FILE_NAME = "_meta.prev";
    static final String TODO_FILE_NAME = "_todo";
    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final int _16M = 16 * 1024 * 1024;
    private static final long TX_OFFSET_TXN = 0;
    private static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    private static final long TX_OFFSET_FIXED_ROW_COUNT = 16;
    private static final long TX_OFFSET_MAX_TIMESTAMP = 24;
    private static final long META_OFFSET_COUNT = 0;
    private static final long META_OFFSET_PARTITION_BY = 4;
    private static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final long META_OFFSET_COLUMN_TYPES = 12;
    private static final DateFormat fmtDay;
    private static final DateFormat fmtMonth;
    private static final DateFormat fmtYear;
    private static CharSequenceHashSet truncateIgnores = new CharSequenceHashSet();
    private final ObjList<AppendMemory> columns;
    private final CompositePath path;
    private final CompositePath other;
    private final LongList refs = new LongList();
    private final Row row = new Row();
    private final int rootLen;
    private final ReadWriteMemory txMem;
    private final ReadOnlyMemory metaMem;
    private final VirtualMemory columnSizeMem;
    private final int partitionBy;
    private final RowFunction switchPartitionFunction = new SwitchPartitionRowFunction();
    private final RowFunction openPartitionFunction = new OpenPartitionRowFunction();
    private final RowFunction noPartitionFunction = new NoPartitionFunction();
    private final NativeLPSZ nativeLPSZ = new NativeLPSZ();
    private final LongList columnTops;
    private final FilesFacade ff;
    private final LongConsumer timestampSetter;
    private final DateFormat partitionDirFmt;
    private final DateLocale partitionDirLocale = DateLocaleFactory.INSTANCE.getDefaultDateLocale();
    private final AppendMemory ddlMem;
    int txPartitionCount = 0;
    private int columnCount;
    private ObjList<Runnable> nullers = new ObjList<>();
    private int mode = 509;
    private long fixedRowCount = 0;
    private long txn;
    private RowFunction rowFunction = openPartitionFunction;
    private long prevTimestamp;
    private long prevTransientRowCount;
    private long maxTimestamp;
    private long partitionLo;
    private long partitionHi;
    private long transientRowCount = 0;
    private long masterRef = 0;
    private boolean removeDirOnCancelRow = true;

    public TableWriter(FilesFacade ff, CharSequence root, CharSequence name) {
        this.ff = ff;
        this.path = new CompositePath().of(root).concat(name);
        this.other = new CompositePath().of(root).concat(name);
        this.rootLen = path.length();
        try {
            this.txMem = openTxnFile();
            byte task = readTodoTaskCode();
            if (task == 1) {
                repairTruncate();
            }
            this.ddlMem = new AppendMemory(ff);
            this.metaMem = openMetaFile();
            this.columnCount = metaMem.getInt(META_OFFSET_COUNT);
            this.partitionBy = metaMem.getInt(META_OFFSET_PARTITION_BY);
            this.columnSizeMem = new VirtualMemory(ff.getPageSize());
            this.refs.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount);
            this.nullers = new ObjList<>(columnCount);
            this.columnTops = new LongList(columnCount);
            this.partitionDirFmt = selectPartitionDirFmt();
            configureColumnMemory();
            timestampSetter = configureTimestampSetter();
            configureAppendPosition();
            purgeUnusedPartitions();
        } catch (CairoException e) {
            close0();
            throw e;
        }
    }

    public void addColumn(CharSequence name, int type) {
        LOG.info().$("Adding column '").$(name).$('[').$(ColumnType.nameOf(type)).$("]' to ").$(path).$();

        commit();

        try {
            // delete stale _meta.swp
            path.concat(TableWriter.META_SWAP_FILE_NAME).$();
            if (Files.exists(path) && !ff.remove(path)) {
                throw CairoException.instance(Os.errno()).put("Cannot remove ").put(path);
            }

            // create new _meta.swp
            writeSwapMeta(name, type);
            metaMem.close();

            // after we moved _meta to _meta.prev
            // we have to have _todo to restore _meta should anything go wront
            path.trimTo(rootLen);
            writeTodo(TODO_RESTORE_META);

            // rename _meta to _meta.prev
            path.trimTo(rootLen);
            other.trimTo(rootLen);

            if (!ff.rename(path.concat(TableWriter.META_FILE_NAME).$(), other.concat(META_PREV_FILE_NAME).$())) {
                int errno = Os.errno();
                metaMem.of(path, ff.getPageSize(), ff.length(path));
                throw CairoException.instance(errno).put("Cannot rename ").put(path).put(" -> ").put(other);
            }

            // rename _meta.swp to _meta
            path.trimTo(rootLen);
            other.trimTo(rootLen);
            other.concat(META_FILE_NAME).$();
            if (!ff.rename(path.concat(META_SWAP_FILE_NAME).$(), other)) {
                int errno = Os.errno();
                // todo: restore _meta from _meta.prev
                throw CairoException.instance(errno).put("Cannot rename ").put(path).put(" -> ").put(other);
            }

            // add column objects
            configureColumn(type);

            // increment column count
            columnCount++;

            // extend columnTop list to make sure row cancel can work
            // need for setting correct top is hard to test without being able to read from table
            columnTops.extendAndSet(columnCount - 1, transientRowCount);

            // open _meta file
            path.trimTo(rootLen);
            path.concat(META_FILE_NAME).$();
            metaMem.of(path, getMapPageSize(), ff.length(path));

            if (transientRowCount > 0 || partitionBy == PartitionBy.NONE) {
                // open column files
                path.trimTo(rootLen);
                setStateForTimestamp(maxTimestamp, false);

                int plen = path.length();
                AppendMemory mem = getPrimaryColumn(columnCount - 1);
                mem.of(dFile(name), getMapPageSize(), 0);
                mem = getSecondaryColumn(columnCount - 1);
                if (mem != null) {
                    path.trimTo(plen);
                    mem.of(iFile(name), getMapPageSize(), 0);
                }

                // keep partition name in path
                path.trimTo(plen);

                if (transientRowCount > 0) {
                    // write .top file
                    writeColumnTop(name);
                }
            }
            path.trimTo(rootLen);
            removeTodoFile();
        } finally {
            path.trimTo(rootLen);
        }
        LOG.info().$("ADDED column '").$(name).$('[').$(ColumnType.nameOf(type)).$("]' to ").$(path).$();
    }

    @Override
    public void close() {
        close0();
    }

    public void commit() {
        if ((masterRef & 1) != 0) {
            cancelRow();
        }

        if (inTransaction()) {
            txMem.jumpTo(TX_OFFSET_TRANSIENT_ROW_COUNT);
            txMem.putLong(transientRowCount);

            if (txPartitionCount > 1) {
                commitPendingPartitions();
                txMem.putLong(fixedRowCount);
                columnSizeMem.jumpTo(0);
                txPartitionCount = 1;
            } else {
                txMem.skip(8);
            }

            txMem.putLong(maxTimestamp);

            Unsafe.getUnsafe().storeFence();
            txMem.jumpTo(TX_OFFSET_TXN);
            txMem.putLong(txn++);
            Unsafe.getUnsafe().storeFence();
            txMem.jumpTo(32);
        }
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount;
    }

    public Row newRow(long timestamp) {
        return rowFunction.newRow(timestamp);
    }

    public void rollback() {
        if (inTransaction()) {
            closeColumns(false);
            columnSizeMem.jumpTo(0);
            configureAppendPosition();
            purgeUnusedPartitions();
        }
    }

    public long size() {
        return fixedRowCount + transientRowCount;
    }

    /**
     * Truncates table. When operation is unsuccessful it throws CairoException. With that truncate can be
     * retried or alternatively table can be closed. Outcome of any other operation with the table is undefined
     * and likely to cause segmentation fault. When table re-opens any partial truncate will be retried.
     */
    public final void truncate() {

        if (size() == 0) {
            return;
        }

        writeTodo(TODO_TRUNCATE);
        for (int i = 0; i < columnCount; i++) {
            getPrimaryColumn(i).truncate();
            AppendMemory mem = getSecondaryColumn(i);
            if (mem != null) {
                mem.truncate();
            }
        }

        if (partitionBy != PartitionBy.NONE) {
            closeColumns(false);
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

        txMem.jumpTo(TX_OFFSET_TXN);
        TableUtils.resetTxn(txMem);
        removeTodoFile();
    }

    private static String getTodoText(int code) {
        switch (code) {
            case 1:
                return "truncate";
            case 2:
                return "restore meta";
            default:
                return "unknown";
        }
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
                closeColumns(false);
                if (removeDirOnCancelRow) {
                    try {
                        setStateForTimestamp(maxTimestamp, false);
                        if (!ff.rmdir(path.$())) {
                            throw CairoException.instance(Os.errno()).put("Cannot remove directory: ").put(path);
                        }
                        removeDirOnCancelRow = false;
                    } finally {
                        path.trimTo(rootLen);
                    }
                }

                // open old partition
                if (prevTimestamp > Long.MIN_VALUE) {
                    try {
                        columnSizeMem.jumpTo((txPartitionCount - 2) * 16);
                        openPartition(prevTimestamp);
                        setAppendPosition(prevTransientRowCount);
                        txPartitionCount--;
                    } catch (CairoException e) {
                        closeColumns(false);
                        throw e;
                    }
                } else {
                    rowFunction = openPartitionFunction;
                }

                // undo counts
                transientRowCount = prevTransientRowCount;
                fixedRowCount -= prevTransientRowCount;
                maxTimestamp = prevTimestamp;
                removeDirOnCancelRow = true;
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
        closeColumns(true);
        Misc.free(txMem);
        Misc.free(metaMem);
        Misc.free(columnSizeMem);
        Misc.free(ddlMem);
        Misc.free(path);
        Misc.free(other);
    }

    private void closeColumns(boolean truncate) {
        if (columns != null) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                AppendMemory m = columns.getQuick(i);
                if (m != null) {
                    m.close(truncate);
                }
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
    }

    private void configureAppendPosition() {
        this.txn = txMem.getLong(TX_OFFSET_TXN);
        this.transientRowCount = txMem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
        this.prevTransientRowCount = this.transientRowCount;
        this.fixedRowCount = txMem.getLong(TX_OFFSET_FIXED_ROW_COUNT);
        this.maxTimestamp = txMem.getLong(TX_OFFSET_MAX_TIMESTAMP);
        this.prevTimestamp = this.maxTimestamp;
        if (this.maxTimestamp > Long.MIN_VALUE || partitionBy == PartitionBy.NONE) {
            openFirstPartition(this.maxTimestamp);
            if (partitionBy == PartitionBy.NONE) {
                rowFunction = noPartitionFunction;
            } else {
                rowFunction = switchPartitionFunction;
            }
        } else {
            rowFunction = openPartitionFunction;
        }
    }

    private void configureColumn(int type) {
        final AppendMemory primary = new AppendMemory(ff);
        final AppendMemory secondary;
        switch (type) {
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
        configureNuller(type, primary, secondary);
    }

    private void configureColumnMemory() {
        for (int i = 0; i < columnCount; i++) {
            configureColumn(getColumnType(i));
        }
    }

    private void configureNuller(int type, AppendMemory mem1, AppendMemory mem2) {
        switch (type) {
            case ColumnType.BOOLEAN:
            case ColumnType.BYTE:
                nullers.add(() -> mem1.putByte((byte) 0));
                break;
            case ColumnType.DOUBLE:
                nullers.add(() -> mem1.putDouble(Double.NaN));
                break;
            case ColumnType.FLOAT:
                nullers.add(() -> mem1.putFloat(Float.NaN));
                break;
            case ColumnType.INT:
                nullers.add(() -> mem1.putInt(Numbers.INT_NaN));
                break;
            case ColumnType.LONG:
            case ColumnType.DATE:
                nullers.add(() -> mem1.putLong(Numbers.LONG_NaN));
                break;
            case ColumnType.SHORT:
                nullers.add(() -> mem1.putShort((short) 0));
                break;
            case ColumnType.STRING:
            case ColumnType.SYMBOL:
                nullers.add(() -> mem2.putLong(mem1.putNullStr()));
                break;
//            case ColumnType.SYMBOL:
//                nullers[index] = () -> mem1.putInt(SymbolTable.VALUE_IS_NULL);
//                break;
            case ColumnType.BINARY:
                nullers.add(() -> mem2.putLong(mem1.putNullBin()));
                break;
            default:
                break;
        }
    }

    private LongConsumer configureTimestampSetter() {
        int index = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
        if (index == -1) {
            return value -> {
            };
        } else {
            // validate type
            if (getColumnType(index) != ColumnType.DATE) {
                throw CairoException.instance(0).put("Column ").put(index).put(" is ").put(ColumnType.nameOf(getColumnType(index))).put(". Expected DATE.");
            }
            return getPrimaryColumn(index)::putLong;
        }
    }

    private LPSZ dFile(CharSequence columnName) {
        return path.concat(columnName).put(".d").$();
    }

    private long getColumnNameOffset() {
        return META_OFFSET_COLUMN_TYPES + columnCount * 4;
    }

    private int getColumnType(int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * 4);
    }

    private long getMapPageSize() {
        long pageSize = ff.getPageSize() * ff.getPageSize();
        if (pageSize < ff.getPageSize() || pageSize > _16M) {
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

    private ReadOnlyMemory openMetaFile() {
        try {
            path.concat(META_FILE_NAME).$();
            return new ReadOnlyMemory(ff, path, ff.getPageSize(), ff.length(path));
        } finally {
            path.trimTo(rootLen);
        }
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

                path.trimTo(plen);
                path.concat(name).put(".top").$();
                if (ff.exists(path)) {
                    columnTops.extendAndSet(i, readColumnTop());
                } else {
                    columnTops.extendAndSet(i, 0L);
                }
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private ReadWriteMemory openTxnFile() {
        try {
            path.concat(TXN_FILE_NAME).$();
            if (ff.exists(path)) {
                return new ReadWriteMemory(ff, path, ff.getPageSize(), 0, ff.getPageSize());
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);

        } finally {
            path.trimTo(rootLen);
        }
    }

    private void purgeUnusedPartitions() {
        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirsNewerThan(maxTimestamp);
        }
    }

    /**
     * path member variable has to be set to location of "top" file.
     *
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    private long readColumnTop() {
        long fd = ff.openRO(path);
        try {
            long buf = Unsafe.malloc(8);
            try {
                if (ff.read(fd, buf, 8, 0) != 8) {
                    throw CairoException.instance(Os.errno()).put("Cannot read top of column ").put(path);
                }
                return Unsafe.getUnsafe().getLong(buf);
            } finally {
                Unsafe.free(buf, 1);
            }
        } finally {
            ff.close(fd);
        }
    }

    private byte readTodoTaskCode() {
        try {
            if (ff.exists(path.concat(TODO_FILE_NAME).$())) {
                long todoFd = ff.openRO(path);
                if (todoFd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open *todo*: ").put(path);
                }
                try {
                    long buf = Unsafe.malloc(1);
                    try {
                        if (ff.read(todoFd, buf, 1, 0) != 1) {
                            throw CairoException.instance(Os.errno()).put("Cannot read *todo*: ").put(path);
                        }
                        return Unsafe.getUnsafe().getByte(buf);
                    } finally {
                        Unsafe.free(buf, 1);
                    }
                } finally {
                    ff.close(todoFd);
                }
            }
            return -1;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirectories() {
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

    private void removePartitionDirsNewerThan(long timestamp) {
        LOG.info().$("Removing partitions newer than '").$ts(timestamp).$("' from ").$(path.$()).$();
        try {
            long p = ff.findFirst(path.$());
            if (p > 0) {
                try {
                    do {
                        long pName = ff.findName(p);
                        path.trimTo(rootLen);
                        path.concat(pName).$();
                        nativeLPSZ.of(pName);
                        if (!truncateIgnores.contains(nativeLPSZ)) {
                            try {
                                long dirTimestamp = partitionDirFmt.parse(nativeLPSZ, partitionDirLocale);
                                if (dirTimestamp <= timestamp) {
                                    continue;
                                }
                            } catch (NumericException ignore) {
                                // not a date?
                                // ignore exception and remove directory
                            }

                            if (ff.findType(p) == Files.DT_DIR) {
                                if (ff.rmdir(path)) {
                                    LOG.info().$("Removing partition dir: ").$(path).$();
                                } else {
                                    throw CairoException.instance(ff.errno()).put("Cannot remove directory: ").put(path);
                                }
                            }
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

    private void removeTodoFile() {
        try {
            if (!ff.remove(path.concat(TODO_FILE_NAME).$())) {
                throw CairoException.instance(Os.errno()).put("Recovery operation completed successfully but I cannot remove todo file: ").put(path).put(". Please remove manually before opening table again,");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void repairTruncate() {
        LOG.info().$("Repairing abnormally terminated truncate on ").$(path).$();
        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirectories();
        }
        txMem.jumpTo(TX_OFFSET_TXN);
        TableUtils.resetTxn(txMem);
        removeTodoFile();
    }

    private DateFormat selectPartitionDirFmt() {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return fmtDay;
            case PartitionBy.MONTH:
                return fmtMonth;
            case PartitionBy.YEAR:
                return fmtYear;
            default:
                return null;

        }
    }

    private void setAppendPosition(final long position) {
        long pSz = Unsafe.malloc(8);
        try {
            for (int i = 0; i < columnCount; i++) {
                AppendMemory mem1 = getPrimaryColumn(i);
                AppendMemory mem2 = getSecondaryColumn(i);

                int type = getColumnType(i);
                long offset;
                long actualPosition = position - columnTops.getQuick(i);

                if (actualPosition > 0) {
                    // subtract column top
                    switch (type) {
                        case ColumnType.BINARY:
                            assert mem2 != null;
                            if (ff.read(mem2.getFd(), pSz, 8, (actualPosition - 1) * 8) != 8) {
                                throw CairoException.instance(ff.errno()).put("Cannot read offset, fd=").put(mem2.getFd()).put(", offset=").put((actualPosition - 1) * 8);
                            }
                            offset = Unsafe.getUnsafe().getLong(pSz);
                            if (ff.read(mem1.getFd(), pSz, 8, offset) != 8) {
                                throw CairoException.instance(ff.errno()).put("Cannot read length, fd=").put(mem1.getFd()).put(", offset=").put(offset);
                            }
                            mem1.setSize(offset + Unsafe.getUnsafe().getLong(pSz));
                            mem2.setSize(actualPosition * 8);
                            break;
                        case ColumnType.STRING:
                        case ColumnType.SYMBOL:
                            assert mem2 != null;
                            if (ff.read(mem2.getFd(), pSz, 8, (actualPosition - 1) * 8) != 8) {
                                throw CairoException.instance(ff.errno()).put("Cannot read offset, fd=").put(mem2.getFd()).put(", offset=").put((actualPosition - 1) * 8);
                            }
                            offset = Unsafe.getUnsafe().getLong(pSz);
                            if (ff.read(mem1.getFd(), pSz, 4, offset) != 4) {
                                throw CairoException.instance(ff.errno()).put("Cannot read length, fd=").put(mem1.getFd()).put(", offset=").put(offset);
                            }
                            mem1.setSize(offset + Unsafe.getUnsafe().getInt(pSz));
                            mem2.setSize(actualPosition * 8);
                            break;
                        default:
                            mem1.setSize(actualPosition * ColumnType.sizeOf(type));
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
        this.timestampSetter.accept(timestamp);
    }

    private void writeColumnTop(CharSequence name) {
        long fd = ff.openAppend(path.concat(name).put(".top").$());
        try {
            long buf = Unsafe.malloc(8);
            try {
                Unsafe.getUnsafe().putLong(buf, transientRowCount);
                if (ff.append(fd, buf, 8) != 8) {
                    throw CairoException.instance(Os.errno()).put("Cannot write ").put(path);
                }
            } finally {
                Unsafe.free(buf, 1);
            }
        } finally {
            ff.close(fd);
        }
    }

    private void writeSwapMeta(CharSequence name, int type) {
        try {
            ddlMem.of(path, ff.getPageSize(), 0);
            ddlMem.putInt(columnCount + 1);
            ddlMem.putInt(partitionBy);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            for (int i = 0; i < columnCount; i++) {
                ddlMem.putInt(getColumnType(i));
            }
            ddlMem.putInt(type);

            long nameOffset = getColumnNameOffset();
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += VirtualMemory.getStorageLength(columnName);
            }
            ddlMem.putStr(name);
        } finally {
            ddlMem.close();
        }
    }

    private void writeTodo(int code) {
        try {
            long fd = ff.openAppend(path.concat(TODO_FILE_NAME).$());
            try {
                long buf = Unsafe.malloc(1);
                try {
                    Unsafe.getUnsafe().putByte(buf, (byte) code);
                    if (ff.append(fd, buf, 1) != 1) {
                        throw CairoException.instance(Os.errno()).put("Cannot write ").put(getTodoText(code)).put(" *todo*: ").put(path);
                    }
                } finally {
                    Unsafe.free(buf, 1);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(rootLen);
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
                    nullers.getQuick(i).run();
                }
            }
            transientRowCount++;
            masterRef++;
        }

        public void cancel() {
            cancelRow();
        }

        public void putBin(int index, long address, long len) {
            getSecondaryColumn(index).putLong(getPrimaryColumn(index).putBin(address, len));
            notNull(index);
        }

        public void putBool(int index, boolean value) {
            putByte(index, value ? (byte) 1 : 0);
        }

        public void putByte(int index, byte value) {
            getPrimaryColumn(index).putByte(value);
            notNull(index);
        }

        public void putDate(int index, long value) {
            putLong(index, value);
        }

        public void putDouble(int index, double value) {
            getPrimaryColumn(index).putDouble(value);
            notNull(index);
        }

        public void putFloat(int index, float value) {
            getPrimaryColumn(index).putFloat(value);
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

        public void putShort(int index, short value) {
            getPrimaryColumn(index).putShort(value);
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
        truncateIgnores.add(META_FILE_NAME);
        truncateIgnores.add(TXN_FILE_NAME);
        truncateIgnores.add(TODO_FILE_NAME);
    }

    static {
        DateFormatCompiler compiler = new DateFormatCompiler();
        fmtDay = compiler.compile("yyyy-MM-dd");
        fmtMonth = compiler.compile("yyyy-MM");
        fmtYear = compiler.compile("yyyy");
    }
}
