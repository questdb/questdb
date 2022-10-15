/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cairo.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.pool.AbstractPool;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import static io.questdb.cairo.wal.Sequencer.SEQ_DIR;

public class TableRegistry extends AbstractPool {
    private static final Log LOG = LogFactory.getLog(TableRegistry.class);
    private final ConcurrentHashMap<SequencerEntry> seqRegistry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<WalWriterPool> walRegistry = new ConcurrentHashMap<>();
    private final CairoEngine engine;
    private final MemoryMARW tableNameMemory = Vm.getCMARWInstance();
    private final ConcurrentHashMap<String> tableNameRegistry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String> reverseTableNameRegistry = new ConcurrentHashMap<>();

    private final boolean mangleTableSystemNames;

    public TableRegistry(CairoEngine engine, CairoConfiguration configuration) {
        super(configuration, configuration.getInactiveWriterTTL()); //todo: separate config option
        this.engine = engine;
        this.mangleTableSystemNames = configuration.mangleTableSystemNames();
        populateCache();
        notifyListener(Thread.currentThread().getId(), "TableRegistry", PoolListener.EV_POOL_OPEN);
    }

    @Override
    public void close() {
        super.close();
        tableNameRegistry.clear();
        reverseTableNameRegistry.clear();
        tableNameMemory.close(true);
    }

    public void deregisterTableName(final CharSequence tableName) {
        removeEntry(tableName);
        CharSequence systemTableName = tableNameRegistry.remove(tableName);
        reverseTableNameRegistry.remove(systemTableName);

        //todo: fix open close !!!
        //todo: fix thread safety !!!
        try (Sequencer seq = seqRegistry.remove(tableName)) {
            if (seq != null) {
                seq.nextTxn(0, TxnCatalog.DROP_TABLE_WALID, 0, 0);
            }
        }

        final WalWriterPool pool = walRegistry.get(tableName);
        if (pool != null) {
            pool.releaseAll(Long.MAX_VALUE);
            walRegistry.remove(tableName);
        }
    }


    public void forAllWalTables(final RegisteredTable callback) {
        final CharSequence root = getConfiguration().getRoot();
        final FilesFacade ff = getConfiguration().getFilesFacade();

        try (Path path = new Path().of(root).slash$()) {
            final StringSink nameSink = new StringSink();
            int rootLen = path.length();
            ff.iterateDir(path, (name, type) -> {
                if (Files.isDir(name, type, nameSink)) {
                    path.trimTo(rootLen);
                    if (isWalTable(nameSink, path, ff)) {
                        try (SequencerImpl sequencer = openSequencer(nameSink)) {
                            callback.onTable(sequencer.getTableId(), nameSink, sequencer.lastTxn());
                        }
                    }
                }
            });
        }
    }

    public CharSequence getSystemTableName(final CharSequence tableName) {
        return tableNameRegistry.get(tableName);
    }

    @NotNull
    public CharSequence getDefaultTableName(CharSequence tableName) {
        if (!mangleTableSystemNames) {
            return tableName;
        }
        return tableName.toString() + TableUtils.SYSTEM_TABLE_NAME_SUFFIX;
    }

    public void removeEntry(final CharSequence tableName) {
        long entryCount = tableNameMemory.getLong(0);
        long offset = Long.BYTES;
        for (int i = 0; i < entryCount; i++) {
            long tableId = tableNameMemory.getLong(offset);
            int tableNameLength = tableNameMemory.getStrLen(offset + Long.BYTES);
            int entrySize = Long.BYTES + Integer.BYTES + 2 * tableNameLength;
            CharSequence currentTableName = tableNameMemory.getStr(offset + Long.BYTES);
            if (Chars.equals(tableName, currentTableName)) {
                tableNameMemory.putLong(offset, -tableId);
            }
            offset += entrySize;
        }
    }

    public void appendEntry(final CharSequence tableName, int tableId) {
        long entryCount = tableNameMemory.getLong(0);
        tableNameMemory.putLong(tableId);
        tableNameMemory.putStr(tableName);
        tableNameMemory.putLong(0, entryCount + 1);
    }

    public CharSequence getSystemTableNameOrDefault(final CharSequence tableName) {
        final CharSequence systemName = tableNameRegistry.get(tableName);
        if (systemName != null) {
            return systemName;
        }

        return getDefaultTableName(tableName);
    }

    public boolean hasSequencer(final CharSequence systemTableName) {
        Sequencer sequencer = seqRegistry.get(systemTableName);
        if (sequencer != null) {
            return true;
        }
        return isWalTable(systemTableName, getConfiguration().getRoot(), getConfiguration().getFilesFacade());
    }

    public long lastTxn(final CharSequence systemTableName) {
        try (SequencerImpl sequencer = openSequencer(systemTableName)) {
            return sequencer.lastTxn();
        }
    }

    public CharSequence registerTable(int tableId, final TableStructure tableStructure) {
        CharSequence tableSystemName = registerTableName(tableStructure.getTableName(), tableId);
        if (tableSystemName != null) {
            //noinspection EmptyTryBlock
            try (SequencerImpl ignore = createSequencer(tableId, tableStructure, tableSystemName)) {
            }
        }
        return tableSystemName;
    }

    @Nullable
    public CharSequence registerTableName(final CharSequence tableName, int tableId) {
        String tableNameStr = Chars.toString(tableName);
        String tableSystemNameStr = tableNameStr + TableUtils.SYSTEM_TABLE_NAME_SUFFIX + tableId;
        CharSequence str = tableNameRegistry.get(tableName);
        if (str != null) {
            if (str.equals(tableSystemNameStr)) {
                return str;
            } else {
                return null;
            }
        }

        str = tableNameRegistry.computeIfAbsent(tableNameStr, (key) -> {
            appendEntry(tableNameStr, tableId);
            return tableSystemNameStr;
        });

        if (str.equals(tableSystemNameStr)) {
            reverseTableNameRegistry.put(tableSystemNameStr, tableNameStr);
            return str;
        } else {
            return null;
        }

    }

    @FunctionalInterface
    public interface RegisteredTable {
        void onTable(int tableId, final CharSequence tableName, long lastTxn);
    }

    public void copyMetadataTo(final CharSequence tableName, final SequencerMetadata metadata) {
        try (Sequencer sequencer = openSequencer(tableName)) {
            sequencer.copyMetadataTo(metadata);
        }
    }

    public @NotNull SequencerCursor getCursor(final CharSequence tableName, long lastCommittedTxn) {
        try (Sequencer sequencer = openSequencer(tableName)) {
            return sequencer.getCursor(lastCommittedTxn);
        }
    }

    public SequencerStructureChangeCursor getStructureChangeCursor(
            final CharSequence tableName,
            @Nullable SequencerStructureChangeCursor reusableCursor,
            long fromSchemaVersion
    ) {
        try (Sequencer sequencer = openSequencer(tableName)) {
            return sequencer.getStructureChangeCursor(reusableCursor, fromSchemaVersion);
        }
    }

    @TestOnly
    public void reopen() {
        if (Unsafe.getUnsafe().compareAndSwapInt(this, CLOSED, 1, 0)) {
            populateCache();
        }
    }

    public long nextStructureTxn(final CharSequence tableName, long structureVersion, AlterOperation operation) {
        try (Sequencer sequencer = openSequencer(tableName)) {
            return sequencer.nextStructureTxn(structureVersion, operation);
        }
    }

    public long nextTxn(final CharSequence tableName, int walId, long expectedSchemaVersion, long segmentId, long segmentTxn) {
        try (Sequencer sequencer = openSequencer(tableName)) {
            return sequencer.nextTxn(expectedSchemaVersion, walId, segmentId, segmentTxn);
        }
    }

    private @NotNull SequencerImpl createSequencer(int tableId, final TableStructure tableStructure, CharSequence tableSystemName) {
        throwIfClosed();
        String systemNameStr = Chars.toString(tableSystemName);
        return seqRegistry.compute(tableSystemName, (key, value) -> {
            SequencerEntry sequencer = new SequencerEntry(this, this.engine, systemNameStr);
            sequencer.create(tableId, tableStructure);
            sequencer.open();
            if (value != null) {
                value.close();
            }
            return sequencer;
        });
    }

    public int getNextWalId(final CharSequence tableName) {
        try (Sequencer sequencer = openSequencer(tableName)) {
            return sequencer.getNextWalId();
        }
    }

    public @NotNull WalWriter getWalWriter(final CharSequence tableName) {
        return getWalPool(tableName).get();
    }

    public String getTableNameBySystemName(CharSequence systemTableName) {
        return reverseTableNameRegistry.get(systemTableName);
    }

    private boolean isWalTable(final CharSequence systemTableName, final Path root, final FilesFacade ff) {
        root.concat(systemTableName).concat(SEQ_DIR);
        return ff.exists(root.$());
    }

    // Check if sequencer files exist, e.g. is it WAL table sequencer must exist
    private boolean isWalTable(final CharSequence systemTableName, final CharSequence root, final FilesFacade ff) {
        Path path = Path.getThreadLocal2(root);
        return isWalTable(systemTableName, path, ff);
    }

    private @NotNull SequencerImpl openSequencer(final CharSequence systemTableName) {
        throwIfClosed();
        final String tableNameStr = Chars.toString(systemTableName);

        SequencerEntry entry = seqRegistry.get(tableNameStr);
        if (entry != null && !entry.isDistressed()) {
            return entry;
        }

        if (entry != null) {
            // Remove distressed entry
            seqRegistry.remove(tableNameStr, entry);
        }

        entry = seqRegistry.computeIfAbsent(tableNameStr, (key) -> {
            if (isWalTable(tableNameStr, getConfiguration().getRoot(), getConfiguration().getFilesFacade())) {
                SequencerEntry sequencer = new SequencerEntry(this, this.engine, tableNameStr);
                sequencer.reset();
                sequencer.open();
                return sequencer;
            }
            return null;
        });

        if (entry == null) {
            throw new CairoError("Unknown table [name=`" + tableNameStr + "`]");
        }
        return entry;
    }

    private @NotNull WalWriterPool getWalPool(final CharSequence tableName) {
        throwIfClosed();
        final String tableNameStr = Chars.toString(tableName);
        return walRegistry.computeIfAbsent(tableNameStr, key
                -> new WalWriterPool(tableNameStr, this, engine.getConfiguration()));
    }

    @Override
    protected boolean releaseAll(long deadline) {
        boolean r0 = releaseWalWriters(deadline);
        boolean r1 = releaseEntries(deadline);
        if (isClosed() && deadline == Long.MAX_VALUE && tableNameMemory.isOpen()) {
            tableNameMemory.close(true);
        }
        return  r0 || r1;
    }

    private boolean releaseEntries(long deadline) {
        if (seqRegistry.size() == 0) {
            // nothing to release
            return true;
        }
        boolean removed = false;
        final Iterator<SequencerEntry> iterator = seqRegistry.values().iterator();
        while (iterator.hasNext()) {
            final SequencerEntry sequencer = iterator.next();
            if (deadline >= sequencer.releaseTime) {
                sequencer.pool = null;
                sequencer. close();
                removed = true;
                iterator.remove();
            }
        }
        return removed;
    }

    private boolean releaseWalWriters(long deadline) {
        if (walRegistry.size() == 0) {
            // nothing to release
            return true;
        }
        boolean removed = false;
        final Iterator<WalWriterPool> iterator = walRegistry.values().iterator();
        while (iterator.hasNext()) {
            final WalWriterPool pool = iterator.next();
            removed = pool.releaseAll(deadline);
            if (deadline == Long.MAX_VALUE && pool.size() == 0) {
                iterator.remove();
            }
        }
        return removed;
    }

    private boolean returnToPool(final SequencerEntry entry) {
        if (isClosed()) {
            return false;
        }
        entry.releaseTime = getConfiguration().getMicrosecondClock().getTicks();
        return true;
    }

    private void throwIfClosed() {
        if (isClosed()) {
            LOG.info().$("is closed").$();
            throw PoolClosedException.INSTANCE;
        }
    }

    private static class SequencerEntry extends SequencerImpl {
        private TableRegistry pool;
        private volatile long releaseTime = Long.MAX_VALUE;

        SequencerEntry(TableRegistry pool, CairoEngine engine, String tableName) {
            super(engine, tableName);
            this.pool = pool;
        }

        public void reset() {
            this.releaseTime = Long.MAX_VALUE;
        }

        @Override
        public void close() {
            if (isOpen()) {
                if (!isDistressed() && pool != null) {
                    if (pool.returnToPool(this)) {
                        return;
                    }
                }
                if (pool != null) {
                    pool.seqRegistry.remove(getTableName(), this);
                }
                super.close();
            }
        }
    }

    private void populateCache() {
        tableNameRegistry.clear();
        reverseTableNameRegistry.clear();
        CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();

        try (final Path path = Path.getThreadLocal(configuration.getRoot()).concat("tables.d").$()) {
            tableNameMemory.smallFile(ff, path, MemoryTag.MMAP_TABLE_WAL_WRITER);

            long entryCount = tableNameMemory.getLong(0);
            long validCount = entryCount;
            long compactOffset = Long.BYTES;
            long currentOffset = compactOffset;

            for (int i = 0; i < entryCount; i++) {
                long tableId = tableNameMemory.getLong(currentOffset);
                int tableNameLength = tableNameMemory.getStrLen(currentOffset + Long.BYTES);
                int entrySize = Long.BYTES + Integer.BYTES + 2 * tableNameLength;

                if (tableId < 0) {
                    // skip removed record
                    currentOffset += entrySize;
                    validCount -= 1;
                    continue;
                }
                CharSequence tableName = tableNameMemory.getStr(currentOffset + Long.BYTES);
                String tableNameStr = Chars.toString(tableName);

                CharSequence str = tableNameRegistry.get(tableNameStr);
                if (str != null) {
                    continue;
                }

                String systemTableNameStr = tableNameRegistry.computeIfAbsent(tableNameStr, (key) -> tableNameStr + TableUtils.SYSTEM_TABLE_NAME_SUFFIX + tableId);
                reverseTableNameRegistry.put(tableNameStr, systemTableNameStr);

                if (currentOffset != compactOffset) {
                    tableNameMemory.putLong(compactOffset, tableId);
                    tableNameMemory.putStr(compactOffset + Long.BYTES, tableName);
                }

                compactOffset += entrySize;
                currentOffset += entrySize;
            }

            tableNameMemory.putLong(0, validCount);
            tableNameMemory.jumpTo(currentOffset);
        }
    }

    public static class WalWriterPool {
        private final ReentrantLock lock = new ReentrantLock();
        private final ArrayDeque<Entry> cache = new ArrayDeque<>();
        private final String systemTableName;
        private final TableRegistry tableRegistry;
        private final CairoConfiguration configuration;
        private volatile boolean closed;

        public WalWriterPool(String systemTableName, TableRegistry tableRegistry, CairoConfiguration configuration) {
            this.systemTableName = systemTableName;
            this.tableRegistry = tableRegistry;
            this.configuration = configuration;
        }

        public Entry get() {
            lock.lock();
            try {
                Entry obj;
                do {
                    obj = cache.poll();

                    if (obj == null) {
                        obj = new Entry(systemTableName, tableRegistry, configuration, this);
                    } else {
                        if (!obj.goActive()) {
                            obj = Misc.free(obj);
                        } else {
                            obj.reset();
                        }
                    }
                } while (obj == null);

                return obj;
            } finally {
                lock.unlock();
            }
        }

        public boolean returnToPool(Entry obj) {
            assert obj != null;
            lock.lock();
            try {
                if (closed) {
                    return false;
                } else {
                    try {
                        obj.rollback();
                    } catch (Throwable e) {
                        LOG.error().$("could not rollback WAL writer [table=").$(obj.getTableName())
                                .$(", walId=").$(obj.getWalId())
                                .$(", error=").$(e).$();
                        obj.close();
                        throw e;
                    }
                    cache.push(obj);
                    obj.releaseTime = configuration.getMicrosecondClock().getTicks();
                    return true;
                }
            } finally {
                lock.unlock();
            }
        }

        public int size() {
            lock.lock();
            try {
                return cache.size();
            } finally {
                lock.unlock();
            }
        }

       protected boolean releaseAll(long deadline) {
           boolean removed = false;
           lock.lock();
           try {
               Iterator<Entry> iterator = cache.iterator();
               while (iterator.hasNext()) {
                   final Entry e = iterator.next();
                   if (deadline >= e.releaseTime) {
                       removed = true;
                       e.doClose(true);
                       e.pool = null;
                       iterator.remove();
                   }
               }
               if (deadline == Long.MAX_VALUE) {
                   this.closed = true;
               }
           } finally {
              lock.unlock();
           }
           return removed;
        }

        public static class Entry extends WalWriter {
            private WalWriterPool pool;
            private volatile long releaseTime = Long.MAX_VALUE;

            public Entry(String systemTableName, TableRegistry tableRegistry, CairoConfiguration configuration, WalWriterPool pool) {
                super(systemTableName, tableRegistry.getTableNameBySystemName(systemTableName), tableRegistry, configuration);
                this.pool = pool;
            }

            public void reset() {
                this.releaseTime = Long.MAX_VALUE;
            }

            @Override
            public void close() {
                if (isOpen()) {
                    if (!isDistressed() && pool != null) {
                        if (pool.returnToPool(this)) {
                            return;
                        }
                    }
                    super.close();
                }
            }
        }
    }
}
