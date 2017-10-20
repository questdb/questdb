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
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Chars;
import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.Sinkable;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatCompiler;
import com.questdb.store.ColumnType;

public class TableUtils {
    static final byte TODO_RESTORE_META = 2;
    static final byte TODO_TRUNCATE = 1;
    static final long META_OFFSET_COLUMN_TYPES = 12;
    static final DateFormat fmtDay;
    static final DateFormat fmtMonth;
    static final DateFormat fmtYear;
    static final String ARCHIVE_FILE_NAME = "_archive";
    static final String DEFAULT_PARTITION_NAME = "default";
    static final String META_FILE_NAME = "_meta";
    static final String TXN_FILE_NAME = "_txn";
    static final long TX_OFFSET_TXN = 0;
    static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    static final long TX_OFFSET_FIXED_ROW_COUNT = 16;
    static final long TX_OFFSET_MAX_TIMESTAMP = 24;
    static final long TX_OFFSET_STRUCT_VERSION = 32;
    static final long TX_EOF = 40;
    static final String META_SWAP_FILE_NAME = "_meta.swp";
    static final String META_PREV_FILE_NAME = "_meta.prev";
    static final String TODO_FILE_NAME = "_todo";
    static final long META_OFFSET_COUNT = 0;
    static final long META_OFFSET_PARTITION_BY = 4;
    static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final int _16M = 16 * 1024 * 1024;
    private final static Log LOG = LogFactory.getLog(TableUtils.class);

    public static void create(FilesFacade ff, CompositePath path, AppendMemory memory, CharSequence root, JournalMetadata metadata, int mode) {
        path.of(root).concat(metadata.getName()).put(Path.SEPARATOR).$();
        if (ff.mkdirs(path, mode) == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot create dir: ").put(path);
        }

        final int rootLen = path.length();
        try (AppendMemory mem = memory) {

            mem.of(ff, path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

            int count = metadata.getColumnCount();
            mem.putInt(count);
            mem.putInt(metadata.getPartitionBy());
            mem.putInt(metadata.getTimestampIndex());
            for (int i = 0; i < count; i++) {
                mem.putInt(metadata.getColumnQuick(i).type);
            }
            for (int i = 0; i < count; i++) {
                mem.putStr(metadata.getColumnQuick(i).name);
            }

            mem.of(ff, path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), ff.getPageSize());
            resetTxn(mem);
        }
    }

    public static int exists(FilesFacade ff, CompositePath path, CharSequence root, CharSequence name) {
        path.of(root).concat(name).$();
        if (ff.exists(path)) {
            // prepare to replace trailing \0
            if (ff.exists(path.chopZ().concat(TXN_FILE_NAME).$())) {
                return 0;
            } else {
                return 2;
            }
        } else {
            return 1;
        }
    }

    public static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES + columnCount * 4;
    }

    public static long lock(FilesFacade ff, CompositePath path) {
        long fd = ff.openRW(path);
        if (fd == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot open ").put(path).put(" to lock");
        }

        if (ff.lock(fd) != 0) {
            ff.close(fd);

            return -1L;
        }
        return fd;
    }

    public static void validate(FilesFacade ff, ReadOnlyMemory metaMem, CharSequenceIntHashMap nameIndex) {
        try {
            final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            final int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            long offset = getColumnNameOffset(columnCount);

            if (offset < columnCount || (
                    columnCount > 0 && (offset < 0 || offset >= ff.length(metaMem.getFd())))) {
                throw validationException(metaMem).put("Incorrect columnCount: ").put(columnCount);
            }

            if (timestampIndex < -1 || timestampIndex >= columnCount) {
                throw validationException(metaMem).put("Timestamp index is outside of columnCount");
            }

            if (timestampIndex != -1) {
                int timestampType = getColumnType(metaMem, timestampIndex);
                if (timestampType != ColumnType.DATE) {
                    throw validationException(metaMem).put("Timestamp column must by DATE but found ").put(ColumnType.nameOf(timestampType));
                }
            }

            // validate column types
            for (int i = 0; i < columnCount; i++) {
                int type = getColumnType(metaMem, i);
                if (ColumnType.sizeOf(type) == -1) {
                    throw validationException(metaMem).put("Invalid column type ").put(type).put(" at [").put(i).put(']');
                }
            }

            // validate column names
            for (int i = 0; i < columnCount; i++) {
                CharSequence name = metaMem.getStr(offset);
                if (name == null || name.length() < 1) {
                    throw validationException(metaMem).put("NULL column name at [").put(i).put(']');
                }

                String s = name.toString();
                if (!nameIndex.put(s, i)) {
                    throw validationException(metaMem).put("Duplicate column: ").put(s).put(" at [").put(i).put(']');
                }
                offset += ReadOnlyMemory.getStorageLength(name);
            }
        } catch (CairoException e) {
            nameIndex.clear();
            throw e;
        }
    }

    static void repairMetaRename(FilesFacade ff, CompositePath path, CompositePath newPath, int rootLen, int index) {
        try {
            path.concat(TableUtils.META_PREV_FILE_NAME);
            if (index > 0) {
                path.put('.').put(index);
            }
            path.$();

            if (ff.exists(path)) {
                LOG.info().$("Repairing metadata from: ").$(path).$();
                if (ff.exists(newPath.concat(TableUtils.META_FILE_NAME).$()) && !ff.remove(newPath)) {
                    throw CairoException.instance(Os.errno()).put("Repair failed. Cannot replace ").put(newPath);
                }

                if (!ff.rename(path, newPath)) {
                    throw CairoException.instance(Os.errno()).put("Repair failed. Cannot rename ").put(path).put(" -> ").put(newPath);
                }
            }
        } finally {
            path.trimTo(rootLen);
            newPath.trimTo(rootLen);
        }
        removeTodoFile(ff, path, rootLen);
    }

    static void removeTodoFile(FilesFacade ff, CompositePath path, int rootLen) {
        try {
            if (!ff.remove(path.concat(TableUtils.TODO_FILE_NAME).$())) {
                throw CairoException.instance(Os.errno()).put("Recovery operation completed successfully but I cannot remove todo file: ").put(path).put(". Please remove manually before opening table again,");
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    static long readTodo(FilesFacade ff, CompositePath path, int rootLen, long buf) {
        try {
            if (ff.exists(path.concat(TableUtils.TODO_FILE_NAME).$())) {
                long todoFd = ff.openRO(path);
                if (todoFd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open *todo*: ").put(path);
                }
                try {
                    if (ff.read(todoFd, buf, 8, 0) != 8) {
                        LOG.info().$("Cannot read *todo* code. File seems to be truncated. Ignoring").$();
                        return -1;
                    }
                    return Unsafe.getUnsafe().getLong(buf);
                } finally {
                    ff.close(todoFd);
                }
            }
            return -1;
        } finally {
            path.trimTo(rootLen);
        }
    }

    static void openViaSharedPath(FilesFacade ff, ReadOnlyMemory mem, CompositePath path, int rootLen) {
        try {
            mem.of(ff, path, ff.getPageSize());
        } finally {
            path.trimTo(rootLen);
        }
    }

    static void rename(FilesFacade ff, CompositePath path, CharSequence name, CompositePath newPath, CharSequence newName, int rootLen) {
        try {
            if (!ff.rename(path.concat(name).$(), newPath.concat(newName).$())) {
                throw CairoException.instance(Os.errno()).put("Cannot rename ").put(path).put(" -> ").put(newPath);
            }
        } finally {
            path.trimTo(rootLen);
            newPath.trimTo(rootLen);
        }
    }

    static void runFragile(Runnable runnable, CairoException e) {
        try {
            runnable.run();
        } catch (CairoException err) {
            LOG.error().$("DOUBLE ERROR: 1st: '").$((Sinkable) e).$('\'').$();
            throw new CairoError(err);
        }
        throw e;
    }


    static void resetTxn(VirtualMemory txMem) {
        // txn to let readers know table is being reset
        txMem.putLong(-1);
        // transient row count
        txMem.putLong(0);
        // fixed row count
        txMem.putLong(0);
        // partition low
        txMem.putLong(Long.MIN_VALUE);
        // structure version
        txMem.putLong(0);
        //
        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(0);
        // txn
        txMem.putLong(0);
        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(TX_EOF);
    }

    /**
     * path member variable has to be set to location of "top" file.
     *
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    static long readColumnTop(FilesFacade ff, CompositePath path, CharSequence name, int plen, long buf) {
        try {
            if (ff.exists(topFile(path.chopZ(), name))) {
                long fd = ff.openRO(path);
                try {
                    if (ff.read(fd, buf, 8, 0) != 8) {
                        throw CairoException.instance(Os.errno()).put("Cannot read top of column ").put(path);
                    }
                    return Unsafe.getUnsafe().getLong(buf);
                } finally {
                    ff.close(fd);
                }
            }
            return 0L;
        } finally {
            path.trimTo(plen);
        }
    }

    static void writeColumnTop(FilesFacade ff, CharSequence name, long count, CompositePath path, int plen, long buf) {
        try {
            long fd = openAppend(ff, path.concat(name).put(".top").$());
            try {
                Unsafe.getUnsafe().putLong(buf, count);
                if (ff.append(fd, buf, 8) != 8) {
                    throw CairoException.instance(Os.errno()).put("Cannot append ").put(path);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(plen);
        }
    }

    static long openAppend(FilesFacade ff, LPSZ name) {
        long fd = ff.openAppend(name);
        if (fd == -1) {
            throw CairoException.instance(Os.errno()).put("Cannot open for append: ").put(name);
        }
        return fd;
    }


    static LPSZ dFile(CompositePath path, CharSequence columnName) {
        return path.concat(columnName).put(".d").$();
    }

    static LPSZ topFile(CompositePath path, CharSequence columnName) {
        return path.concat(columnName).put(".top").$();
    }

    static LPSZ iFile(CompositePath path, CharSequence columnName) {
        return path.concat(columnName).put(".i").$();
    }

    static long getMapPageSize(FilesFacade ff) {
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

    static DateFormat selectPartitionDirFmt(int partitionBy) {
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

    static int getColumnType(ReadOnlyMemory metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * 4);
    }

    /**
     * This an O(n) method to find if column by the same name already exists. The benefit of poor performance
     * is that we don't keep column name strings on heap. We only use this method when adding new column, where
     * high performance of name check does not matter much.
     *
     * @param name to check
     * @return 0 based column index.
     */
    static int getColumnIndexQuiet(ReadOnlyMemory metaMem, CharSequence name, int columnCount) {
        long nameOffset = getColumnNameOffset(columnCount);
        for (int i = 0; i < columnCount; i++) {
            CharSequence col = metaMem.getStr(nameOffset);
            if (Chars.equals(col, name)) {
                return i;
            }
            nameOffset += VirtualMemory.getStorageLength(col);
        }
        return -1;
    }

    static int openMetaSwapFile(FilesFacade ff, AppendMemory mem, CompositePath path, int rootLen, int retryCount) {
        return openIndexedFile(ff, META_SWAP_FILE_NAME, mem, path, rootLen, retryCount);
    }

    /**
     * Ensures there is file available for writing new metadata. On Windows or in a hostile OS environment
     * it is possible that swap file cannot be created from single attempt. It could exist and be open
     * by antivirus or exist and have permissions forbidding us access. Whatever is the reason we should
     * not be hellbent on trying to open same file name if it can't be opened.
     * This method will cycle thru up to 30 file names initializing AppendMemory with first one that
     * succeeded. The names are suffixed with cycle index, for example meta.swp.1 etc.
     *
     * @param mem     AppendMemory to be wrapped around swap file this method manages to open.
     * @param path    object that can be reused for opening new file.
     * @param rootLen length to trim path to when we are done.
     */
    static int openIndexedFile(FilesFacade ff, CharSequence base, AppendMemory mem, CompositePath path, int rootLen, int retries) {
        try {
            path.concat(base).$();
            int l = path.length();
            int index = 0;
            do {
                if (index > 0) {
                    path.trimTo(l).put('.').put(index);
                    path.$();
                }

                if (!ff.exists(path) || ff.remove(path)) {
                    try {
                        mem.of(ff, path, ff.getPageSize());
                        return index;
                    } catch (CairoException e) {
                        // right, cannot open file for some reason?
                        LOG.error().$("Cannot open file: ").$(path).$('[').$(Os.errno()).$(']').$();
                    }
                } else {
                    LOG.error().$("Cannot remove file: ").$(path).$('[').$(Os.errno()).$(']').$();
                }
            } while (++index < retries);
            throw CairoException.instance(0).put("Cannot open indexed file. Max number of attempts reached [").put(index).put("]. Last file tried: ").put(path);
        } finally {
            path.trimTo(rootLen);
        }
    }

    static int addColumnToMeta(
            FilesFacade ff,
            ReadOnlyMemory metaMem,
            CharSequence name,
            int type,
            CompositePath path,
            int rootLen,
            AppendMemory ddlMem) {

        int index;
        try {
            index = openMetaSwapFile(ff, ddlMem, path, rootLen, 30);

            int columnCount = metaMem.getInt(META_OFFSET_COUNT);

            ddlMem.putInt(columnCount + 1);
            ddlMem.putInt(metaMem.getInt(META_OFFSET_PARTITION_BY));
            ddlMem.putInt(metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX));
            for (int i = 0; i < columnCount; i++) {
                ddlMem.putInt(getColumnType(metaMem, i));
            }
            ddlMem.putInt(type);

            long nameOffset = getColumnNameOffset(columnCount);
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

    static void writeTodo(FilesFacade ff, CompositePath path, long todo, int rootLen, long buf) {
        try {
            long fd = TableUtils.openAppend(ff, path.concat(TableUtils.TODO_FILE_NAME).$());
            try {
                Unsafe.getUnsafe().putLong(buf, todo);
                if (ff.append(fd, buf, 8) != 8) {
                    throw CairoException.instance(Os.errno()).put("Cannot write ").put(getTodoText(todo)).put(" *todo*: ").put(path);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    static String getTodoText(long code) {
        switch ((int) (code & 0xff)) {
            case TODO_TRUNCATE:
                return "truncate";
            case TODO_RESTORE_META:
                return "restore meta";
            default:
                // really impossible to happen, but we keep this line to comply with Murphy's law.
                return "unknown";
        }
    }

    private static CairoException validationException(ReadOnlyMemory mem) {
        return CairoException.instance(0).put("Invalid metadata at fd=").put(mem.getFd()).put(". ");
    }

    static {
        DateFormatCompiler compiler = new DateFormatCompiler();
        fmtDay = compiler.compile("yyyy-MM-dd");
        fmtMonth = compiler.compile("yyyy-MM");
        fmtYear = compiler.compile("yyyy");
    }
}
