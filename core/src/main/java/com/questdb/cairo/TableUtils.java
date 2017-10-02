package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Chars;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.Sinkable;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatCompiler;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.std.time.Dates;
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
    private final static ThreadLocal<CompositePath> tlPath = new ThreadLocal<>(CompositePath::new);
    private final static ThreadLocal<CompositePath> tlRenamePath = new ThreadLocal<>(CompositePath::new);
    private final static ThreadLocal<AppendMemory> tlMetaAppendMem = new ThreadLocal<>(AppendMemory::new);
    private final static ThreadLocal<ReadOnlyMemory> tlMetaReadOnlyMem = new ThreadLocal<>(ReadOnlyMemory::new);
    private final static ThreadLocal<CharSequenceIntHashMap> tlColumnNameIndexMap = new ThreadLocal<>(CharSequenceIntHashMap::new);
    private static Log LOG = LogFactory.getLog(TableUtils.class);

    public static void addColumn(FilesFacade ff, CharSequence root, CharSequence tableName, CharSequence columnName, int columnType) {
        final CompositePath path = tlPath.get().of(root).concat(tableName);
        final CompositePath other = tlRenamePath.get().of(root).concat(tableName);

        int rootLen = path.length();

        long buf = Unsafe.malloc(8);
        try {

            byte code = readTodo(ff, path, rootLen, buf);
            if (code == TODO_RESTORE_META) {
                repairMetaRename(ff, path, other, rootLen);
            }

            try (ReadOnlyMemory mem = tlMetaReadOnlyMem.get()) {

                openViaSharedPath(ff, mem, path.concat(META_FILE_NAME).$(), rootLen);

                int columnCount = mem.getInt(META_OFFSET_COUNT);
                int partitioBy = mem.getInt(META_OFFSET_PARTITION_BY);

                if (TableUtils.getColumnIndexQuiet(mem, columnName, columnCount) != -1) {
                    throw CairoException.instance(0).put("Column ").put(columnName).put(" already exists in ").put(path);
                }

                LOG.info().$("Adding column '").$(columnName).$('[').$(ColumnType.nameOf(columnType)).$("]' to ").$(path).$();

                // create new _meta.swp
                try (AppendMemory ddlMem = tlMetaAppendMem.get()) {
                    TableUtils.addColumnToMeta(ff, mem, columnName, columnType, path, rootLen, ddlMem);
                }

                // re-purpose memory object
                path.concat(TXN_FILE_NAME).$();
                if (ff.exists(path)) {

                    openViaSharedPath(ff, mem, path, rootLen);

                    long transientRowCount = mem.getLong(TX_OFFSET_TRANSIENT_ROW_COUNT);
                    if (transientRowCount > 0) {
                        long timestamp = mem.getLong(TX_OFFSET_MAX_TIMESTAMP);

                        path.put(Path.SEPARATOR);
                        switch (partitioBy) {
                            case PartitionBy.DAY:
                                fmtDay.format(Dates.floorDD(timestamp), DateLocaleFactory.INSTANCE.getDefaultDateLocale(), null, path);
                                break;
                            case PartitionBy.MONTH:
                                fmtMonth.format(Dates.floorMM(timestamp), DateLocaleFactory.INSTANCE.getDefaultDateLocale(), null, path);
                                break;
                            case PartitionBy.YEAR:
                                fmtYear.format(Dates.floorYYYY(timestamp), DateLocaleFactory.INSTANCE.getDefaultDateLocale(), null, path);
                                break;
                            case PartitionBy.NONE:
                                path.put(DEFAULT_PARTITION_NAME);
                                break;
                            default:
                                break;
                        }

                        writeColumnTop(ff, columnName, transientRowCount, path, rootLen, buf);
                    }
                } else {
                    LOG.error().$("No transaction file in ").$(path).$();
                    path.trimTo(rootLen);
                }

                writeTodo(ff, path, TODO_RESTORE_META, rootLen, buf);

                // rename _meta to _meta.prev
                rename(ff, path, META_FILE_NAME, other, META_PREV_FILE_NAME, rootLen);

                // rename _meta.swp to _meta
                rename(ff, path, META_SWAP_FILE_NAME, other, META_FILE_NAME, rootLen);

                try {
                    if (!ff.remove(path.concat(TODO_FILE_NAME).$())) {
                        throw CairoException.instance(Os.errno()).put("Cannot remove ").put(path);
                    }
                } finally {
                    path.trimTo(rootLen);
                }
            } catch (CairoException e) {
                LOG.error().$("Cannot add column '").$(columnName).$("' to '").$(tableName).$("' and this is why {").$((Sinkable) e).$('}').$();
                throw e;
            }
        } finally {
            Unsafe.free(buf, 8);
        }
    }

    public static void create(FilesFacade ff, CharSequence root, JournalMetadata metadata, int mode) {
        CompositePath path = tlPath.get();
        path.of(root).concat(metadata.getName()).put(Path.SEPARATOR).$();
        if (ff.mkdirs(path, mode) == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot create dir: ").put(path);
        }

        final int rootLen = path.length();
        try (AppendMemory mem = tlMetaAppendMem.get()) {

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

    public static int exists(FilesFacade ff, CharSequence root, CharSequence name) {
        CompositePath path = tlPath.get();
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

    public static void freeThreadLocals() {
        tlMetaAppendMem.get().close();
        tlMetaAppendMem.remove();

        tlMetaReadOnlyMem.get().close();
        tlMetaReadOnlyMem.remove();

        tlPath.get().close();
        tlPath.remove();

        tlRenamePath.get().close();
        tlRenamePath.remove();
    }

    public static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES + columnCount * 4;
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

    public static void validate(FilesFacade ff, ReadOnlyMemory metaMem) {
        CharSequenceIntHashMap map = tlColumnNameIndexMap.get();
        map.clear();
        validate(ff, metaMem, map);
    }

    static void repairMetaRename(FilesFacade ff, CompositePath path, CompositePath newPath, int rootLen) {
        try {
            if (ff.exists(path.concat(TableUtils.META_PREV_FILE_NAME).$())) {
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

    static byte readTodo(FilesFacade ff, CompositePath path, int rootLen, long buf) {
        try {
            if (ff.exists(path.concat(TableUtils.TODO_FILE_NAME).$())) {
                long todoFd = ff.openRO(path);
                if (todoFd == -1) {
                    throw CairoException.instance(Os.errno()).put("Cannot open *todo*: ").put(path);
                }
                try {
                    if (ff.read(todoFd, buf, 1, 0) != 1) {
                        LOG.info().$("Cannot read *todo* code. File seems to be truncated. Ignoring").$();
                        return -1;
                    }
                    return Unsafe.getUnsafe().getByte(buf);
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

    static void renameOrElse(FilesFacade ff, CompositePath path, CharSequence name, CompositePath newPath, CharSequence newName, int rootLen, Runnable fail) {
        try {
            rename(ff, path, name, newPath, newName, rootLen);
        } catch (CairoException e) {
            try {
                fail.run();
            } catch (CairoException err) {
                LOG.error().$("DOUBLE ERROR: 1st: '").$((Sinkable) e).$('\'').$();
                throw new CairoError(err);
            }
            throw e;
        }
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
        txMem.jumpTo(32);
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

    static void addColumnToMeta(
            FilesFacade ff,
            ReadOnlyMemory metaMem,
            CharSequence name,
            int type,
            CompositePath path,
            int rootLen,
            AppendMemory ddlMem) {
        try {
            // delete stale _meta.swp
            path.concat(META_SWAP_FILE_NAME).$();

            if (ff.exists(path) && !ff.remove(path)) {
                throw CairoException.instance(Os.errno()).put("Cannot remove ").put(path);
            }

            try {
                int columnCount = metaMem.getInt(META_OFFSET_COUNT);
                ddlMem.of(ff, path, ff.getPageSize());
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
        } finally {
            path.trimTo(rootLen);
        }
    }

    static void writeTodo(FilesFacade ff, CompositePath path, byte code, int rootLen, long buf) {
        try {
            long fd = TableUtils.openAppend(ff, path.concat(TableUtils.TODO_FILE_NAME).$());
            try {
                Unsafe.getUnsafe().putByte(buf, code);
                if (ff.append(fd, buf, 1) != 1) {
                    throw CairoException.instance(Os.errno()).put("Cannot write ").put(getTodoText(code)).put(" *todo*: ").put(path);
                }
            } finally {
                ff.close(fd);
            }
        } finally {
            path.trimTo(rootLen);
        }
    }

    static String getTodoText(int code) {
        switch (code) {
            case TODO_TRUNCATE:
                return "truncate";
            case TODO_RESTORE_META:
                return "restore meta";
            default:
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
