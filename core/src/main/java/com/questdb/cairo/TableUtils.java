package com.questdb.cairo;

import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.FilesFacade;
import com.questdb.misc.Unsafe;
import com.questdb.std.ObjectFactory;
import com.questdb.std.ThreadLocal;
import com.questdb.std.str.CompositePath;
import com.questdb.std.str.Path;

import java.io.Closeable;

public class TableUtils implements Closeable {
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

        int rootLen = path.length();
        path.trimTo(rootLen);

        try (AppendMemory mem = tlMem.get()) {

            mem.of(path.concat(TableWriter.META_FILE_NAME).$(), ff.getPageSize(), 0);

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


            path.trimTo(rootLen);
            mem.of(path.concat(TableWriter.TXN_FILE_NAME).$(), ff.getPageSize(), 0);
            resetTxn(mem);
        }
    }

    public int exists(CharSequence root, CharSequence name) {
        CompositePath path = tlPath.get();
        path.of(root).concat(name).$();
        if (ff.exists(path)) {
            // prepare to replace trailing \0
            path.trimTo(path.length());
            if (ff.exists(path.concat(TableWriter.TXN_FILE_NAME).$())) {
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
}
