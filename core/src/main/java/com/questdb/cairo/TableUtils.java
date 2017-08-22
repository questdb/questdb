package com.questdb.cairo;

import com.questdb.PartitionBy;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.Os;
import com.questdb.misc.Unsafe;
import com.questdb.std.ObjectFactory;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.LPSZ;
import com.questdb.std.str.Path;
import com.questdb.std.time.DateFormat;
import com.questdb.std.time.DateFormatCompiler;
import com.questdb.store.ColumnType;

import java.io.Closeable;

public class TableUtils implements Closeable {
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
    static final String META_SWAP_FILE_NAME = "_meta.swp";
    static final String META_PREV_FILE_NAME = "_meta.prev";
    static final String TODO_FILE_NAME = "_todo";
    static final long META_OFFSET_COUNT = 0;
    static final long META_OFFSET_PARTITION_BY = 4;
    static final long META_OFFSET_TIMESTAMP_INDEX = 8;
    private static final int _16M = 16 * 1024 * 1024;
    private final ThreadLocal<CompositePath> tlPath = new ThreadLocal<>(CompositePath.FACTORY);
    private final FilesFacade ff;
    private final ThreadLocal<AppendMemory> tlMem = new ThreadLocal<>(new ObjectFactory<AppendMemory>() {
        @Override
        public AppendMemory newInstance() {
            return new AppendMemory(ff);
        }
    });

    public TableUtils(FilesFacade ff) {
        this.ff = ff;
    }

    public static long getColumnNameOffset(int columnCount) {
        return META_OFFSET_COLUMN_TYPES + columnCount * 4;
    }

    @Override
    public void close() {
        tlMem.get().close();
        tlMem.remove();

        tlPath.get().close();
        tlPath.remove();
    }

    public void create(CharSequence root, JournalMetadata metadata, int mode) {
        CompositePath path = tlPath.get();
        path.of(root).concat(metadata.getName()).put(Path.SEPARATOR).$();
        if (ff.mkdirs(path, mode) == -1) {
            throw CairoException.instance(ff.errno()).put("Cannot create dir: ").put(path);
        }

        final int rootLen = path.length();
        try (AppendMemory mem = tlMem.get()) {

            mem.of(path.trimTo(rootLen).concat(META_FILE_NAME).$(), ff.getPageSize());

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

            mem.of(path.trimTo(rootLen).concat(TXN_FILE_NAME).$(), ff.getPageSize());
            resetTxn(mem);
        }
    }

    public int exists(CharSequence root, CharSequence name) {
        CompositePath path = tlPath.get();
        path.of(root).concat(name).$();
        if (ff.exists(path)) {
            // prepare to replace trailing \0
            if (ff.exists(path.trimTo(path.length()).concat(TXN_FILE_NAME).$())) {
                return 0;
            } else {
                return 2;
            }
        } else {
            return 1;
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
        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(0);
        // txn
        txMem.putLong(0);
        Unsafe.getUnsafe().storeFence();
        txMem.jumpTo(32);
    }

    static void setColumnSize(FilesFacade ff, AppendMemory mem1, AppendMemory mem2, int type, long actualPosition, long buf) {
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
                        mem1.setSize(offset + len + 4);
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

    /**
     * path member variable has to be set to location of "top" file.
     *
     * @return number of rows column doesn't have when column was added to table that already had data.
     */
    static long readColumnTop(FilesFacade ff, CompositePath path, CharSequence name, int plen) {
        try {
            path.trimTo(path.length());
            if (ff.exists(topFile(path, name))) {
                long fd = ff.openRO(path);
                try {
                    long buf = Unsafe.malloc(8);
                    try {
                        if (ff.read(fd, buf, 8, 0) != 8) {
                            throw CairoException.instance(Os.errno()).put("Cannot read top of column ").put(path);
                        }
                        return Unsafe.getUnsafe().getLong(buf);
                    } finally {
                        Unsafe.free(buf, 8);
                    }
                } finally {
                    ff.close(fd);
                }
            }
            return 0L;
        } finally {
            path.trimTo(plen);
        }
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

    static {
        DateFormatCompiler compiler = new DateFormatCompiler();
        fmtDay = compiler.compile("yyyy-MM-dd");
        fmtMonth = compiler.compile("yyyy-MM");
        fmtYear = compiler.compile("yyyy");
    }
}
