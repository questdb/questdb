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
import com.questdb.std.*;
import com.questdb.std.str.*;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatUtils;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.Dates;
import com.questdb.store.ColumnType;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.function.LongConsumer;

public class TableWriter implements Closeable {

    private static final Log LOG = LogFactory.getLog(TableWriter.class);
    private static final CharSequenceHashSet ignoredFiles = new CharSequenceHashSet();
    final ObjList<AppendMemory> columns;
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
    private final DateFormat partitionDirFmt;
    private final AppendMemory ddlMem;
    private final int mkDirMode;
    private final int fileOperationRetryCount;
    private final CharSequence name;
    private final CharSequenceIntHashMap tmpValidationMap = new CharSequenceIntHashMap();
    int txPartitionCount = 0;
    private long lockFd = -1;
    private LongConsumer timestampSetter;
    private int columnCount;
    private ObjList<Runnable> nullers = new ObjList<>();
    private long fixedRowCount = 0;
    private long txn;
    private long structVersion;
    private RowFunction rowFunction = openPartitionFunction;
    private long prevTimestamp;
    private long prevTransientRowCount;
    private long maxTimestamp;
    private long partitionLo;
    private long partitionHi;
    private long transientRowCount = 0;
    private long masterRef = 0;
    private boolean removeDirOnCancelRow = true;
    private long tempMem8b = Unsafe.malloc(8);
    private int metaSwapIndex;
    private int metaPrevIndex;

    public TableWriter(FilesFacade ff, CharSequence root, CharSequence name) {
        this(ff, root, name, 509, 30);
    }

    public TableWriter(FilesFacade ff, CharSequence root, CharSequence name, int mkDirMode, int fileOperationRetryCount) {
        LOG.info().$("open '").$(name).$('\'').$();
        this.ff = ff;
        this.mkDirMode = mkDirMode;
        this.fileOperationRetryCount = fileOperationRetryCount;
        this.path = new CompositePath().of(root).concat(name);
        this.other = new CompositePath().of(root).concat(name);
        this.name = ImmutableCharSequence.of(name);
        this.rootLen = path.length();
        try {
            this.txMem = openTxnFile();
            path.put(".lock").$();
            try {
                this.lockFd = TableUtils.lock(ff, path);
            } finally {
                path.trimTo(rootLen);
            }

            if (this.lockFd == -1L) {
                throw CairoException.instance(ff.errno()).put("Cannot lock table: ").put(path.$());
            }

            this.txMem.jumpTo(TableUtils.TX_EOF);
            long todo = readTodoTaskCode();
            if (todo != -1L) {
                switch ((int) (todo & 0xff)) {
                    case TableUtils.TODO_TRUNCATE:
                        repairTruncate();
                        break;
                    case TableUtils.TODO_RESTORE_META:
                        repairMetaRename((int) (todo >> 8));
                        break;
                    default:
                        LOG.error().$("ignoring unknown *todo* code: ").$(todo).$();
                        break;
                }
            }
            this.ddlMem = new AppendMemory();
            this.metaMem = new ReadOnlyMemory();
            openMetaFile();
            this.columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);
            this.partitionBy = metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY);
            this.columnSizeMem = new VirtualMemory(ff.getPageSize());
            this.refs.extendAndSet(columnCount, 0);
            this.columns = new ObjList<>(columnCount);
            this.nullers = new ObjList<>(columnCount);
            this.columnTops = new LongList(columnCount);
            this.partitionDirFmt = selectPartitionDirFmt(partitionBy);
            configureColumnMemory();
            timestampSetter = configureTimestampSetter();
            configureAppendPosition();
            purgeUnusedPartitions();
        } catch (CairoException e) {
            LOG.error().$("cannot open '").$(path).$("' and this is why: {").$((Sinkable) e).$('}').$();
            close();
            throw e;
        }
    }

    /**
     * Adds new column to table, which can be either empty or can have data already. When existing columns
     * already have data this function will create ".top" file in addition to column files. ".top" file contains
     * size of partition at the moment of column creation. It must be used to accurately position inside new
     * column when either appending or reading.
     * <p>
     * <b>Failures</b>
     * Adding new column can fail in many different situations. None of the failures affect integrity of data that is already in
     * the table but can leave instance of TableWriter in inconsistent state. When this happens function will throw CairoError.
     * Calling code must close TableWriter instance and open another when problems are rectified. Those problems would be
     * either with disk or memory or both.
     * <p>
     * Whenever function throws CairoException application code can continue using TableWriter instance and may attempt to
     * add columns again.
     * <p>
     * <b>Transactions</b>
     * <p>
     * Pending transaction will be committed before function attempts to add column. Even when function is unsuccessful it may
     * still have committed transaction.
     */
    public void addColumn(CharSequence name, int type) {

        if (getColumnIndexQuiet(metaMem, name, columnCount) != -1) {
            throw CairoException.instance(0).put("Duplicate column name: ").put(name);
        }

        LOG.info().$("adding column '").$(name).$('[').$(ColumnType.nameOf(type)).$("]' to ").$(path).$();

        commit();

        // create new _meta.swp
        this.metaSwapIndex = addColumnToMeta(name, type);

        // close _meta so we can rename it
        metaMem.close();

        // rename _meta to _meta.prev
        renameMetaToMetaPrev();

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo();

        // rename _meta.swp to _meta
        renameSwapMetaToMeta();

        // add column objects
        configureColumn(type);

        // increment column count
        columnCount++;

        // extend columnTop list to make sure row cancel can work
        // need for setting correct top is hard to test without being able to read from table
        columnTops.extendAndSet(columnCount - 1, transientRowCount);

        // create column files
        if (transientRowCount > 0 || partitionBy == PartitionBy.NONE) {
            try {
                openNewColumnFiles(name);
            } catch (CairoException e) {
                runFragile(() -> {
                    removeMetaFile();
                    removeLastColumn();
                    rename(TableUtils.META_PREV_FILE_NAME, metaPrevIndex, TableUtils.META_FILE_NAME);
                    openMetaFile();
                    removeTodoFile();
                }, e);
            }
        }

        try {
            // open _meta file
            openMetaFile();

            // remove _todo
            removeTodoFile();

        } catch (CairoException err) {
            throw new CairoError(err);
        }

        bumpStructureVersion();

        LOG.info().$("ADDED column '").$(name).$('[').$(ColumnType.nameOf(type)).$("]' to ").$(path).$();
    }

    @Override
    public void close() {
        if (isOpen()) {
            closeColumns(true);
            Misc.free(txMem);
            Misc.free(metaMem);
            Misc.free(columnSizeMem);
            Misc.free(ddlMem);
            Misc.free(path);
            Misc.free(other);
            if (lockFd != -1L) {
                ff.close(lockFd);
            }
            if (tempMem8b != 0) {
                Unsafe.free(tempMem8b, 8);
                tempMem8b = 0;
            }
            LOG.info().$("closed '").$(name).$('\'').$();
        }
    }

    /**
     * Commits newly added rows of data. This method updates transaction file with pointers to end of appended data.
     * <p>
     * <b>Pending rows</b>
     * <p>This method will cancel pending rows by calling {@link #cancelRow()}. Data in partially appended row will be lost.</p>
     */
    public void commit() {
        if ((masterRef & 1) != 0) {
            cancelRow();
        }

        if (inTransaction()) {
            txMem.jumpTo(TableUtils.TX_OFFSET_TRANSIENT_ROW_COUNT);
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
            fencedTxnBump();
            prevTransientRowCount = transientRowCount;
        }
    }

    public int getColumnIndex(CharSequence name) {
        int index = getColumnIndexQuiet(metaMem, name, columnCount);
        if (index == -1) {
            throw CairoException.instance(0).put("Invalid column name: ").put(name);
        }
        return index;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public CharSequence getName() {
        return name;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public boolean inTransaction() {
        return txPartitionCount > 1 || transientRowCount != prevTransientRowCount;
    }

    public boolean isOpen() {
        return tempMem8b != 0;
    }

    public Row newRow(long timestamp) {
        return rowFunction.newRow(timestamp);
    }

    public void removeColumn(CharSequence name) {
        int index = getColumnIndex(name);

        LOG.info().$("removing column '").$(name).$("' from ").$(path).$();

        // check if we are moving timestamp from a partitioned table
        boolean timestamp = index == metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);

        if (timestamp && partitionBy != PartitionBy.NONE) {
            throw CairoException.instance(0).put("Cannot remove timestamp from partitioned table");
        }

        commit();

        this.metaSwapIndex = removeColumnFromMeta(index);

        // close _meta so we can rename it
        metaMem.close();

        // rename _meta to _meta.prev
        renameMetaToMetaPrev();

        // after we moved _meta to _meta.prev
        // we have to have _todo to restore _meta should anything go wrong
        writeRestoreMetaTodo();

        // rename _meta.swp to _meta
        renameSwapMetaToMeta();

        // remove column objects
        removeColumn(index);

        // decrement column count
        columnCount--;

        // reset timestamp limits
        if (timestamp) {
            maxTimestamp = prevTimestamp = Long.MIN_VALUE;
            timestampSetter = value -> {
            };
        }

        try {
            // open _meta file
            openMetaFile();

            // remove _todo
            removeTodoFile();

            // remove column files has to be done after _todo is removed
            removeColumnFiles(name);

        } catch (CairoException err) {
            throw new CairoError(err);
        }

        bumpStructureVersion();

        LOG.info().$("REMOVED column '").$(name).$("' from ").$(path).$();
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

        writeTodo(TableUtils.TODO_TRUNCATE);
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

        txMem.jumpTo(TableUtils.TX_OFFSET_TXN);
        TableUtils.resetTxn(txMem);
        try {
            removeTodoFile();
        } catch (CairoException err) {
            throw new CairoError(err);
        }
    }

    private static int getPrimaryColumnIndex(int index) {
        return index * 2;
    }

    private static int getSecondaryColumnIndex(int index) {
        return getPrimaryColumnIndex(index) + 1;
    }

    private static void setColumnSize(FilesFacade ff, AppendMemory mem1, AppendMemory mem2, int type, long actualPosition, long buf) {
        long offset;
        long len;
        if (actualPosition > 0) {
            // subtract column top
            switch (type) {
                case ColumnType.BINARY:
                    assert mem2 != null;
                    if (ff.read(mem2.getFd(), buf, 8, (actualPosition - 1) * 8) != 8) {
                        throw CairoException.instance(ff.errno()).put("Cannot read offset, fd=").put(mem2.getFd()).put(", offset=").put((actualPosition - 1) * 8);
                    }
                    offset = Unsafe.getUnsafe().getLong(buf);
                    if (ff.read(mem1.getFd(), buf, 8, offset) != 8) {
                        throw CairoException.instance(ff.errno()).put("Cannot read length, fd=").put(mem1.getFd()).put(", offset=").put(offset);
                    }
                    len = Unsafe.getUnsafe().getLong(buf);
                    if (len == -1) {
                        mem1.setSize(offset + 8);
                    } else {
                        mem1.setSize(offset + len + 8);
                    }
                    mem2.setSize(actualPosition * 8);
                    break;
                case ColumnType.STRING:
                case ColumnType.SYMBOL:
                    assert mem2 != null;
                    if (ff.read(mem2.getFd(), buf, 8, (actualPosition - 1) * 8) != 8) {
                        throw CairoException.instance(ff.errno()).put("Cannot read offset, fd=").put(mem2.getFd()).put(", offset=").put((actualPosition - 1) * 8);
                    }
                    offset = Unsafe.getUnsafe().getLong(buf);
                    if (ff.read(mem1.getFd(), buf, 4, offset) != 4) {
                        throw CairoException.instance(ff.errno()).put("Cannot read length, fd=").put(mem1.getFd()).put(", offset=").put(offset);
                    }
                    len = Unsafe.getUnsafe().getInt(buf);
                    if (len == -1) {
                        mem1.setSize(offset + 4);
                    } else {
                        mem1.setSize(offset + len * 2 + 4);
                    }
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

    private static DateFormat selectPartitionDirFmt(int partitionBy) {
        switch (partitionBy) {
            case PartitionBy.DAY:
                return TableUtils.fmtDay;
            case PartitionBy.MONTH:
                return TableUtils.fmtMonth;
            case PartitionBy.YEAR:
                return TableUtils.fmtYear;
            default:
                return null;
        }
    }

    /**
     * This an O(n) method to find if column by the same name already exists. The benefit of poor performance
     * is that we don't keep column name strings on heap. We only use this method when adding new column, where
     * high performance of name check does not matter much.
     *
     * @param name to check
     * @return 0 based column index.
     */
    private static int getColumnIndexQuiet(ReadOnlyMemory metaMem, CharSequence name, int columnCount) {
        long nameOffset = TableUtils.getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence col = metaMem.getStr(nameOffset);
            if (Chars.equals(col, name)) {
                return i;
            }
            nameOffset += VirtualMemory.getStorageLength(col);
        }
        return -1;
    }

    private static void runFragile(Runnable runnable, CairoException e) {
        try {
            runnable.run();
        } catch (CairoException err) {
            LOG.error().$("DOUBLE ERROR: 1st: '").$((Sinkable) e).$('\'').$();
            throw new CairoError(err);
        }
        throw e;
    }

    private int addColumnToMeta(CharSequence name, int type) {
        int index;
        try {
            index = TableUtils.openMetaSwapFile(ff, ddlMem, path, rootLen, 30);

            int columnCount = metaMem.getInt(TableUtils.META_OFFSET_COUNT);

            ddlMem.putInt(columnCount + 1);
            ddlMem.putInt(metaMem.getInt(TableUtils.META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX));
            for (int i = 0; i < columnCount; i++) {
                ddlMem.putInt(TableUtils.getColumnType(metaMem, i));
            }
            ddlMem.putInt(type);

            long nameOffset = TableUtils.getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                ddlMem.putStr(columnName);
                nameOffset += VirtualMemory.getStorageLength(columnName);
            }
            ddlMem.putStr(name);
        } finally {
            ddlMem.close();
        }
        return index;
    }

    private void bumpMasterRef() {
        if ((masterRef & 1) != 0) {
            cancelRow();
        }
        masterRef++;
    }

    private void bumpStructureVersion() {
        txMem.jumpTo(TableUtils.TX_OFFSET_STRUCT_VERSION);
        txMem.putLong(++structVersion);
        fencedTxnBump();
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
                            throw CairoException.instance(ff.errno()).put("Cannot remove directory: ").put(path);
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
        long offset = 0;
        for (int i = 0; i < txPartitionCount - 1; i++) {
            try {
                long partitionTimestamp = columnSizeMem.getLong(offset + 8);
                setStateForTimestamp(partitionTimestamp, false);

                long fd = openAppend(path.concat(TableUtils.ARCHIVE_FILE_NAME).$());
                try {
                    int len = 8;
                    long o = offset;
                    while (len > 0) {
                        long l = Math.min(len, columnSizeMem.pageRemaining(o));
                        if (ff.write(fd, columnSizeMem.addressOf(o), l, 0) != l) {
                            throw CairoException.instance(ff.errno()).put("Commit failed, file=").put(path);
                        }
                        len -= l;
                        o += l;
                    }
                } finally {
                    ff.close(fd);
                }
                offset += 16;
            } finally {
                path.trimTo(rootLen);
            }
        }
    }

    private void configureAppendPosition() {
        this.txn = txMem.getLong(TableUtils.TX_OFFSET_TXN);
        this.transientRowCount = txMem.getLong(TableUtils.TX_OFFSET_TRANSIENT_ROW_COUNT);
        this.prevTransientRowCount = this.transientRowCount;
        this.fixedRowCount = txMem.getLong(TableUtils.TX_OFFSET_FIXED_ROW_COUNT);
        this.maxTimestamp = txMem.getLong(TableUtils.TX_OFFSET_MAX_TIMESTAMP);
        this.structVersion = txMem.getLong(TableUtils.TX_OFFSET_STRUCT_VERSION);
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
        final AppendMemory primary = new AppendMemory();
        final AppendMemory secondary;
        switch (type) {
            case ColumnType.BINARY:
            case ColumnType.SYMBOL:
            case ColumnType.STRING:
                secondary = new AppendMemory();
                break;
            default:
                secondary = null;
                break;
        }
        columns.add(primary);
        columns.add(secondary);
        configureNuller(type, primary, secondary);
        refs.add(0);
    }

    private void configureColumnMemory() {
        for (int i = 0; i < columnCount; i++) {
            configureColumn(TableUtils.getColumnType(metaMem, i));
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
        int index = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
        if (index == -1) {
            return value -> {
            };
        } else {
            // validate type
            if (TableUtils.getColumnType(metaMem, index) != ColumnType.DATE) {
                throw CairoException.instance(0).put("Column ").put(index).put(" is ").put(ColumnType.nameOf(TableUtils.getColumnType(metaMem, index))).put(". Expected DATE.");
            }
            return getPrimaryColumn(index)::putLong;
        }
    }

    private void fencedTxnBump() {
        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(TableUtils.TX_OFFSET_TXN);
        txMem.putLong(++txn);
        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(TableUtils.TX_EOF);
    }

    private AppendMemory getPrimaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getPrimaryColumnIndex(column));
    }

    private AppendMemory getSecondaryColumn(int column) {
        assert column < columnCount : "Column index is out of bounds: " + column + " >= " + columnCount;
        return columns.getQuick(getSecondaryColumnIndex(column));
    }

    private long openAppend(LPSZ name) {
        long fd = ff.openAppend(name);
        if (fd == -1) {
            throw CairoException.instance(Os.errno()).put("Cannot open for append: ").put(name);
        }
        return fd;
    }

    private void openColumnFiles(CharSequence name, int i, int plen) {
        AppendMemory mem1 = getPrimaryColumn(i);
        AppendMemory mem2 = getSecondaryColumn(i);

        mem1.of(ff, TableUtils.dFile(path.trimTo(plen), name), TableUtils.getMapPageSize(ff));

        if (mem2 != null) {
            mem2.of(ff, TableUtils.iFile(path.trimTo(plen), name), TableUtils.getMapPageSize(ff));
        }

        path.trimTo(plen);
    }

    private void openFirstPartition(long timestamp) {
        openPartition(timestamp);
        setAppendPosition(transientRowCount);
        txPartitionCount = 1;
    }

    private void openMetaFile() {
        path.concat(TableUtils.META_FILE_NAME).$();
        try {
            metaMem.of(ff, path, ff.getPageSize());
        } finally {
            path.trimTo(rootLen);
        }
        tmpValidationMap.clear();
        TableUtils.validate(ff, metaMem, tmpValidationMap);
    }

    private void openNewColumnFiles(CharSequence name) {
        try {
            // open column files
            setStateForTimestamp(maxTimestamp, false);
            int plen = path.length();
            openColumnFiles(name, columnCount - 1, plen);
            if (transientRowCount > 0) {
                // write .top file
                writeColumnTop(name);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void openPartition(long timestamp) {
        try {
            setStateForTimestamp(timestamp, true);
            int plen = path.length();
            if (ff.mkdirs(path.put(Path.SEPARATOR).$(), mkDirMode) != 0) {
                throw CairoException.instance(ff.errno()).put("Cannot create directory: ").put(path);
            }
            assert columnCount > 0;

            long nameOffset = TableUtils.getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(nameOffset);
                openColumnFiles(name, i, plen);
                columnTops.extendAndSet(i, TableUtils.readColumnTop(ff, path, name, plen, tempMem8b));
                nameOffset += VirtualMemory.getStorageLength(name);
            }
            LOG.info().$("switched partition to '").$(path).$('\'').$();
        } finally {
            path.trimTo(rootLen);
        }
    }

    private ReadWriteMemory openTxnFile() {
        try {
            if (ff.exists(path.concat(TableUtils.TXN_FILE_NAME).$())) {
                return new ReadWriteMemory(ff, path, ff.getPageSize(), 0, ff.getPageSize());
            }
            throw CairoException.instance(ff.errno()).put("Cannot append. File does not exist: ").put(path);

        } finally {
            path.trimTo(rootLen);
        }
    }

    private void purgeUnusedPartitions() {
        if (partitionBy != PartitionBy.NONE && maxTimestamp != Numbers.LONG_NaN) {
            removePartitionDirsNewerThan(maxTimestamp);
        }
    }

    private long readTodoTaskCode() {
        try {
            if (ff.exists(path.concat(TableUtils.TODO_FILE_NAME).$())) {
                long todoFd = ff.openRO(path);
                if (todoFd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open *todo*: ").put(path);
                }
                try {
                    if (ff.read(todoFd, tempMem8b, 8, 0) != 8) {
                        LOG.info().$("Cannot read *todo* code. File seems to be truncated. Ignoring. [file=").$(path).$(']').$();
                        return -1;
                    }
                    return Unsafe.getUnsafe().getLong(tempMem8b);
                } finally {
                    ff.close(todoFd);
                }
            }
            return -1;
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeColumn(int index) {
        Misc.free(getPrimaryColumn(index));
        Misc.free(getSecondaryColumn(index));
        columns.remove(getSecondaryColumnIndex(index));
        columns.remove(getPrimaryColumnIndex(index));
        columnTops.removeIndex(index);
    }

    private void removeColumnFiles(CharSequence name) {
        try {
            ff.iterateDir(path.$(), (file, type) -> {
                nativeLPSZ.of(file);
                if (type == Files.DT_DIR && !ignoredFiles.contains(nativeLPSZ)) {
                    path.trimTo(rootLen);
                    path.concat(nativeLPSZ);
                    int plen = path.length();
                    removeFileAndOrLog(TableUtils.dFile(path, name));
                    removeFileAndOrLog(TableUtils.iFile(path.trimTo(plen), name));
                    removeFileAndOrLog(TableUtils.topFile(path.trimTo(plen), name));
                }
            });
        } finally {
            path.trimTo(rootLen);
        }
    }

    private int removeColumnFromMeta(int index) {
        try {
            int metaSwapIndex = TableUtils.openMetaSwapFile(ff, ddlMem, path, rootLen, fileOperationRetryCount);
            int timestampIndex = metaMem.getInt(TableUtils.META_OFFSET_TIMESTAMP_INDEX);
            ddlMem.putInt(columnCount - 1);
            ddlMem.putInt(partitionBy);

            if (timestampIndex == index) {
                ddlMem.putInt(-1);
            } else if (index < timestampIndex) {
                ddlMem.putInt(timestampIndex - 1);
            } else {
                ddlMem.putInt(timestampIndex);
            }

            for (int i = 0; i < columnCount; i++) {
                if (i != index) {
                    ddlMem.putInt(TableUtils.getColumnType(metaMem, i));
                }
            }

            long nameOffset = TableUtils.getColumnNameOffset(columnCount);
            for (int i = 0; i < columnCount; i++) {
                CharSequence columnName = metaMem.getStr(nameOffset);
                if (i != index) {
                    ddlMem.putStr(columnName);
                }
                nameOffset += VirtualMemory.getStorageLength(columnName);
            }

            return metaSwapIndex;
        } finally {
            ddlMem.close();
        }
    }

    private void removeFileAndOrLog(LPSZ name) {
        if (ff.exists(name)) {
            if (ff.remove(name)) {
                LOG.info().$("removed: ").$(path).$();
            } else {
                LOG.error().$("cannot remove: ").$(name).$(" [errno=").$(ff.errno()).$(']').$();
            }
        }
    }

    private void removeLastColumn() {
        removeColumn(columnCount - 1);
        columnCount--;
    }

    private void removeMetaFile() {
        try {
            path.concat(TableUtils.META_FILE_NAME).$();
            if (ff.exists(path) && !ff.remove(path)) {
                throw CairoException.instance(ff.errno()).put("Recovery failed. Cannot remove: ").put(path);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirectories() {
        try {
            ff.iterateDir(path.$(), (name, type) -> {
                path.trimTo(rootLen);
                path.concat(name).$();
                nativeLPSZ.of(name);
                if (!ignoredFiles.contains(nativeLPSZ)) {
                    if (type == Files.DT_DIR && !ff.rmdir(path)) {
                        throw CairoException.instance(ff.errno()).put("Cannot remove directory: ").put(path);
                    } else if (type != Files.DT_DIR && !ff.remove(path)) {
                        throw CairoException.instance(ff.errno()).put("Cannot remove file: ").put(path);
                    }
                }
            });
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removePartitionDirsNewerThan(long timestamp) {
        LOG.info().$("looking to remove partitions newer than '").$ts(timestamp).$("' from ").$(path.$()).$();
        try {
            ff.iterateDir(path.$(), (pName, type) -> {
                path.trimTo(rootLen);
                path.concat(pName).$();
                nativeLPSZ.of(pName);
                if (!ignoredFiles.contains(nativeLPSZ)) {
                    try {
                        long dirTimestamp = partitionDirFmt.parse(nativeLPSZ, DateLocaleFactory.INSTANCE.getDefaultDateLocale());
                        if (dirTimestamp <= timestamp) {
                            return;
                        }
                    } catch (NumericException ignore) {
                        // not a date?
                        // ignore exception and remove directory
                    }

                    if (type == Files.DT_DIR) {
                        if (ff.rmdir(path)) {
                            LOG.info().$("removing partition dir: ").$(path).$();
                        } else {
                            LOG.error().$("cannot remove: ").$(path).$(" [errno=").$(ff.errno()).$(']').$();
                        }
                    }
                }
            });
        } finally {
            path.trimTo(rootLen);
        }
    }

    private void removeTodoFile() {
        try {
            if (!ff.remove(path.concat(TableUtils.TODO_FILE_NAME).$())) {
                throw CairoException.instance(Os.errno()).put("Recovery operation completed successfully but I cannot remove todo file: ").put(path).put(". Please remove manually before opening table again,");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    private int rename(CharSequence from, CharSequence toBase, int retries) {
        try {

            int index = 0;
            other.concat(toBase).$();
            path.concat(from).$();
            int l = other.length();

            do {
                if (index > 0) {
                    other.trimTo(l);
                    other.put('.').put(index);
                    other.$();
                }

                if (ff.exists(other) && !ff.remove(other)) {
                    LOG.info().$("cannot remove target of rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).$(']').$();
                    index++;
                    continue;
                }

                if (!ff.rename(path, other)) {
                    LOG.info().$("cannot rename '").$(path).$("' to '").$(other).$(" [errno=").$(ff.errno()).$(']').$();
                    index++;
                    continue;
                }

                return index;
            } while (index < retries);

            throw CairoException.instance(0).put("Cannot rename ").put(path).put(". Max number of attempts reached [").put(index).put("]. Last target was: ").put(other);
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void rename(CharSequence fromBase, int fromIndex, CharSequence to) {
        try {
            path.concat(fromBase);
            if (fromIndex > 0) {
                path.put('.').put(fromIndex);
            }
            path.$();

            if (!ff.rename(path, other.concat(to).$())) {
                throw CairoException.instance(ff.errno()).put("Cannot rename ").put(path).put(" -> ").put(other);
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }
    }

    private void renameMetaToMetaPrev() {
        try {
            this.metaPrevIndex = rename(TableUtils.META_FILE_NAME, TableUtils.META_PREV_FILE_NAME, fileOperationRetryCount);
        } catch (CairoException e) {
            runFragile(this::openMetaFile, e);
        }
    }

    private void renameSwapMetaToMeta() {
        // rename _meta.swp to _meta
        try {
            rename(TableUtils.META_SWAP_FILE_NAME, metaSwapIndex, TableUtils.META_FILE_NAME);
        } catch (CairoException e) {
            runFragile(() -> {
                rename(TableUtils.META_PREV_FILE_NAME, metaPrevIndex, TableUtils.META_FILE_NAME);
                openMetaFile();
                removeTodoFile();
            }, e);
        }
    }

    private void repairMetaRename(int index) {
        try {
            path.concat(TableUtils.META_PREV_FILE_NAME);
            if (index > 0) {
                path.put('.').put(index);
            }
            path.$();

            if (ff.exists(path)) {
                LOG.info().$("Repairing metadata from: ").$(path).$();
                if (ff.exists(other.concat(TableUtils.META_FILE_NAME).$()) && !ff.remove(other)) {
                    throw CairoException.instance(Os.errno()).put("Repair failed. Cannot replace ").put(other);
                }

                if (!ff.rename(path, other)) {
                    throw CairoException.instance(Os.errno()).put("Repair failed. Cannot rename ").put(path).put(" -> ").put(other);
                }
            }
        } finally {
            path.trimTo(rootLen);
            other.trimTo(rootLen);
        }

        removeTodoFile();
    }

    private void repairTruncate() {
        LOG.info().$("repairing abnormally terminated truncate on ").$(path).$();
        if (partitionBy != PartitionBy.NONE) {
            removePartitionDirectories();
        }
        txMem.jumpTo(TableUtils.TX_OFFSET_TXN);
        TableUtils.resetTxn(txMem);
        removeTodoFile();
    }

    private void setAppendPosition(final long position) {
        for (int i = 0; i < columnCount; i++) {
            setColumnSize(ff, getPrimaryColumn(i), getSecondaryColumn(i), TableUtils.getColumnType(metaMem, i), position - columnTops.getQuick(i), tempMem8b);
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
                path.put(TableUtils.DEFAULT_PARTITION_NAME);
                partitionLo = Long.MIN_VALUE;
                partitionHi = Long.MAX_VALUE;
                break;
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
        long fd = openAppend(path.concat(name).put(".top").$());
        try {
            Unsafe.getUnsafe().putLong(tempMem8b, transientRowCount);
            if (ff.append(fd, tempMem8b, 8) != 8) {
                throw CairoException.instance(Os.errno()).put("Cannot append ").put(path);
            }
        } finally {
            ff.close(fd);
        }
    }

    private void writeRestoreMetaTodo() {
        try {
            writeTodo(((long) metaPrevIndex << 8) | TableUtils.TODO_RESTORE_META);
        } catch (CairoException e) {
            runFragile(() -> {
                rename(TableUtils.META_PREV_FILE_NAME, metaPrevIndex, TableUtils.META_FILE_NAME);
                openMetaFile();
            }, e);
        }
    }

    private void writeTodo(long code) {
        try {
            long fd = openAppend(path.concat(TableUtils.TODO_FILE_NAME).$());
            try {
                Unsafe.getUnsafe().putLong(tempMem8b, code);
                if (ff.append(fd, tempMem8b, 8) != 8) {
                    throw CairoException.instance(Os.errno()).put("Cannot write ").put(TableUtils.getTodoText(code)).put(" *todo*: ").put(path);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(rootLen);
        }
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
            getPrimaryColumn(index).putBool(value);
            notNull(index);
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
        ignoredFiles.add("..");
        ignoredFiles.add(".");
        ignoredFiles.add(TableUtils.META_FILE_NAME);
        ignoredFiles.add(TableUtils.TXN_FILE_NAME);
        ignoredFiles.add(TableUtils.TODO_FILE_NAME);
    }
}
