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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.pool.AbstractPool;
import io.questdb.cairo.pool.PoolListener;
import io.questdb.cairo.pool.ex.PoolClosedException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.ConcurrentHashMap;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import static io.questdb.cairo.wal.Sequencer.SEQ_DIR;

public class TableRegistry extends AbstractPool {
    private static final Log LOG = LogFactory.getLog(TableRegistry.class);
    private final ConcurrentHashMap<Entry> seqRegistry = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<WalWriterPool> walRegistry = new ConcurrentHashMap<>();
    private final CairoEngine engine;

    public TableRegistry(CairoEngine engine, CairoConfiguration configuration) {
        super(configuration, configuration.getInactiveWriterTTL()); //todo: separate config option
        this.engine = engine;
        notifyListener(Thread.currentThread().getId(), "TableRegistry", PoolListener.EV_POOL_OPEN);
    }

    public long lastTxn(final CharSequence tableName) {
        try (SequencerImpl sequencer = openSequencer(tableName)) {
            return sequencer.lastTxn();
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

    public boolean hasSequencer(final CharSequence tableName) {
        Sequencer sequencer = seqRegistry.get(tableName);
        if (sequencer != null) {
            return true;
        }
        return isWalTable(tableName, getConfiguration().getRoot(), getConfiguration().getFilesFacade());
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

    public void registerTable(int tableId, final TableStructure tableStructure) {
        try (SequencerImpl ignore = createSequencer(tableId, tableStructure)) {
        }
    }

    public int getNextWalId(final CharSequence tableName) {
        try (Sequencer sequencer = openSequencer(tableName)) {
            return sequencer.getNextWalId();
        }
    }

    public @NotNull WalWriter getWalWriter(final CharSequence tableName) {
        return getWalPool(tableName).get();
    }

    // Check if sequencer files exist, e.g. is it WAL table sequencer must exist
    private static boolean isWalTable(final CharSequence tableName, final CharSequence root, final FilesFacade ff) {
        Path path = Path.getThreadLocal2(root);
        return isWalTable(tableName, path, ff);
    }

    private static boolean isWalTable(final CharSequence tableName, final Path root, final FilesFacade ff) {
        root.concat(tableName).concat(SEQ_DIR);
        return ff.exists(root.$());
    }

    private @NotNull SequencerImpl createSequencer(int tableId, final TableStructure tableStructure) {
        throwIfClosed();
        final String tableName = Chars.toString(tableStructure.getTableName());
        return seqRegistry.compute(tableName, (key, value) -> {
            Entry sequencer = new Entry(this, this.engine, tableName);
            sequencer.create(tableId, tableStructure);
            sequencer.open();
            if (value != null) {
                value.close();
            }
            return sequencer;
        });
    }

    private @NotNull SequencerImpl openSequencer(final CharSequence tableName) {
        throwIfClosed();
        final String tableNameStr = Chars.toString(tableName);

        Entry entry = seqRegistry.get(tableNameStr);
        if (entry != null) {
            return entry;
        }

        entry = seqRegistry.computeIfAbsent(tableNameStr, (key) -> {
            if (isWalTable(tableNameStr, getConfiguration().getRoot(), getConfiguration().getFilesFacade())) {
                Entry sequencer = new Entry(this, this.engine, tableNameStr);
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
        return  r0 || r1;
    }

    private boolean releaseEntries(long deadline) {
        Iterator<Entry> iterator = seqRegistry.values().iterator();
        boolean removed = false;
        while (iterator.hasNext()) {
            final Entry sequencer = iterator.next();
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
        Iterator<WalWriterPool> iterator = walRegistry.values().iterator();
        boolean removed = false;
        while (iterator.hasNext()) {
            final WalWriterPool pool = iterator.next();
            removed = pool.releaseAll(deadline);
            if (deadline == Long.MAX_VALUE && pool.size() == 0) {
                iterator.remove();
            }
        }
        return removed;
    }

    private boolean returnToPool(final Entry entry) {
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

    private static class Entry extends SequencerImpl {
        private TableRegistry pool;
        private volatile long releaseTime = Long.MAX_VALUE;

        Entry(TableRegistry pool, CairoEngine engine, String tableName) {
            super(engine, tableName);
            this.pool = pool;
        }

        public void reset() {
            this.releaseTime = Long.MAX_VALUE;
        }

        @Override
        public void close() {
            if (isOpen()) {
                if (pool != null) {
                    if (pool.returnToPool(this)) {
                        return;
                    }
                }
                super.close();
            }
        }
    }

    public static class WalWriterPool {
        private final ReentrantLock lock = new ReentrantLock();
        private final ArrayDeque<Entry> cache = new ArrayDeque<>();
        private final String tableName;
        private final TableRegistry tableRegistry;
        private final CairoConfiguration configuration;
        private volatile boolean closed;

        public WalWriterPool(String tableName, TableRegistry tableRegistry, CairoConfiguration configuration) {
            this.tableName = tableName;
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
                        obj = new Entry(tableName, tableRegistry, configuration, this);
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

            public Entry(String tableName, TableRegistry tableRegistry, CairoConfiguration configuration, WalWriterPool pool) {
                super(tableName, tableRegistry, configuration);
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
