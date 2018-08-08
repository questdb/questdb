/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.FilesFacade;
import com.questdb.std.Os;
import com.questdb.std.Unsafe;
import com.questdb.std.microtime.DateFormat;
import com.questdb.std.microtime.DateFormatCompiler;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;

public final class TableUtils {
    public static final int TABLE_EXISTS = 0;
    public static final int TABLE_DOES_NOT_EXIST = 1;
    public static final int TABLE_RESERVED = 2;
    public static final String META_FILE_NAME = "_meta";
    public static final String TXN_FILE_NAME = "_txn";
    public static final long META_OFFSET_COLUMN_TYPES = 128;
    public static final int INITIAL_TXN = 0;
    public static final int NULL_LEN = -1;
    static final byte TODO_RESTORE_META = 2;
    static final byte TODO_TRUNCATE = 1;
    static final DateFormat fmtDay;
    static final DateFormat fmtMonth;
    static final DateFormat fmtYear;
    static final String ARCHIVE_FILE_NAME = "_archive";
    static final String DEFAULT_PARTITION_NAME = "default";
    static final long TX_OFFSET_TXN = 0;
    static final long TX_OFFSET_TRANSIENT_ROW_COUNT = 8;
    static final long TX_OFFSET_FIXED_ROW_COUNT = 16;
    static final long TX_OFFSET_MAX_TIMESTAMP = 24;
    static final long TX_OFFSET_STRUCT_VERSION = 32;
    static final long TX_OFFSET_TXN_CHECK = 40;
    static final long TX_OFFSET_MAP_WRITER_COUNT = 48;
    static final String META_SWAP_FILE_NAME = "_meta.swp";
    static final String META_PREV_FILE_NAME = "_meta.prev";
    static final String TODO_FILE_NAME = "_todo";
    static final long META_OFFSET_COUNT = 0;
    static final long META_OFFSET_PARTITION_BY = 4;
    static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final int META_COLUMN_DATA_SIZE = 16;
    private final static Log LOG = LogFactory.getLog(TableUtils.class);

    public static int exists(FilesFacade ff, Path path, CharSequence root, CharSequence name) {
        return exists(ff, path, root, name, 0, name.length());
    }

    public static int exists(FilesFacade ff, Path path, CharSequence root, CharSequence name, int lo, int hi) {
        path.of(root).concat(name, lo, hi).$();
        if (ff.exists(path)) {
            // prepare to replace trailing \0
            if (ff.exists(path.chopZ().concat(TXN_FILE_NAME).$())) {
                return TABLE_EXISTS;
            } else {
                return TABLE_RESERVED;
            }
        } else {
            return TABLE_DOES_NOT_EXIST;
        }
    }

    public static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES + columnCount * META_COLUMN_DATA_SIZE;
    }

    public static long lock(FilesFacade ff, Path path) {
        long fd = ff.openRW(path);
        if (fd == -1) {
            LOG.error().$("cannot open '").$(path).$("' to lock [errno=").$(ff.errno()).$(']').$();
            return -1L;
        }

        if (ff.lock(fd) != 0) {
            LOG.error().$("cannot lock '").$(path).$("' [errno=").$(ff.errno()).$(", fd=").$(fd).$(']').$();
            ff.close(fd);
            return -1L;
        }

        return fd;
    }

    public static void lockName(Path path) {
        path.put(".lock").$();
    }

    public static void resetTxn(VirtualMemory txMem, int symbolMapCount) {
        txMem.jumpTo(TX_OFFSET_TXN);
        // txn to let readers know table is being reset
        txMem.putLong(INITIAL_TXN);
        Unsafe.getUnsafe().storeFence();

        // transient row count
        txMem.putLong(0);
        // fixed row count
        txMem.putLong(0);
        // partition low
        txMem.putLong(Long.MIN_VALUE);
        // structure version
        txMem.putLong(0);

        // skip over txn check
        txMem.skip(8);
        txMem.putInt(symbolMapCount);
        for (int i = 0; i < symbolMapCount; i++) {
            txMem.putLong(0);
        }

        long offset = txMem.getAppendOffset();
        txMem.jumpTo(TX_OFFSET_TXN_CHECK);

        Unsafe.getUnsafe().storeFence();
        // txn check
        txMem.putLong(INITIAL_TXN);
        txMem.jumpTo(offset);
    }

    public static void validate(FilesFacade ff, ReadOnlyMemory metaMem, CharSequenceIntHashMap nameIndex) {
        try {
            final int columnCount = metaMem.getInt(META_OFFSET_COUNT);
            long offset = getColumnNameOffset(columnCount);

            if (offset < columnCount || (
                    columnCount > 0 && (offset < 0 || offset >= ff.length(metaMem.getFd())))) {
                throw validationException(metaMem).put("Incorrect columnCount: ").put(columnCount);
            }

            final int timestampIndex = metaMem.getInt(META_OFFSET_TIMESTAMP_INDEX);
            if (timestampIndex < -1 || timestampIndex >= columnCount) {
                throw validationException(metaMem).put("Timestamp index is outside of columnCount");
            }

            if (timestampIndex != -1) {
                int timestampType = getColumnType(metaMem, timestampIndex);
                if (timestampType != ColumnType.TIMESTAMP) {
                    throw validationException(metaMem).put("Timestamp column must be TIMESTAMP, but found ").put(ColumnType.nameOf(timestampType));
                }
            }

            // validate column types and index attributes
            for (int i = 0; i < columnCount; i++) {
                int type = getColumnType(metaMem, i);
                if (ColumnType.sizeOf(type) == -1) {
                    throw validationException(metaMem).put("Invalid column type ").put(type).put(" at [").put(i).put(']');
                }

                if (isColumnIndexed(metaMem, i)) {
                    if (type != ColumnType.SYMBOL) {
                        throw validationException(metaMem).put("Index flag is only supported for SYMBOL").put(" at [").put(i).put(']');
                    }

                    if (getIndexBlockCapacity(metaMem, i) < 2) {
                        throw validationException(metaMem).put("Invalid index value block capacity ").put(getIndexBlockCapacity(metaMem, i)).put(" at [").put(i).put(']');
                    }
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

    /**
     * path member variable has to be set to location of "top" file.
     *
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    static long readColumnTop(FilesFacade ff, Path path, CharSequence name, int plen, long buf) {
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

    static LPSZ dFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".d").$();
    }

    static LPSZ topFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".top").$();
    }

    static LPSZ iFile(Path path, CharSequence columnName) {
        return path.concat(columnName).put(".i").$();
    }

    static int getColumnType(ReadOnlyMemory metaMem, int columnIndex) {
        return metaMem.getByte(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE);
    }

    static boolean isColumnIndexed(ReadOnlyMemory metaMem, int columnIndex) {
        return metaMem.getBool(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 1);
    }

    static int getIndexBlockCapacity(ReadOnlyMemory metaMem, int columnIndex) {
        return metaMem.getInt(META_OFFSET_COLUMN_TYPES + columnIndex * META_COLUMN_DATA_SIZE + 2);
    }

    static int openMetaSwapFile(FilesFacade ff, AppendMemory mem, Path path, int rootLen, int retryCount) {
        try {
            path.concat(META_SWAP_FILE_NAME).$();
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
            } while (++index < retryCount);
            throw CairoException.instance(0).put("Cannot open indexed file. Max number of attempts reached [").put(index).put("]. Last file tried: ").put(path);
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
