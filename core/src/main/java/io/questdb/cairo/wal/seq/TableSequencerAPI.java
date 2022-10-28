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

package io.questdb.cairo.wal.seq;

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
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

import static io.questdb.cairo.wal.WalUtils.SEQ_DIR;

public class TableSequencerAPI extends AbstractPool {
    private static final Log LOG = LogFactory.getLog(TableSequencerAPI.class);
    private final ConcurrentHashMap<TableSequencerEntry> seqRegistry = new ConcurrentHashMap<>();
    private final CairoEngine engine;

    public TableSequencerAPI(CairoEngine engine, CairoConfiguration configuration) {
        super(configuration, configuration.getInactiveWriterTTL()); //todo: separate config option
        this.engine = engine;
        notifyListener(Thread.currentThread().getId(), "TableSequencerAPI", PoolListener.EV_POOL_OPEN);
    }

    public void getTableMetadata(final CharSequence tableName, final TableRecordMetadataSink sink, boolean compress) {
        try (TableSequencer tableSequencer = openSequencer(tableName)) {
            tableSequencer.getTableMetadata(sink, compress);
        }
    }

    public void forAllWalTables(final RegisteredTable callback) {
        final CharSequence root = getConfiguration().getRoot();
        final FilesFacade ff = getConfiguration().getFilesFacade();

        // todo: too much GC here
        // this will be replaced with table name registry when drop WAL table implemented
        try (Path path = new Path().of(root).slash$()) {
            final StringSink nameSink = new StringSink();
            int rootLen = path.length();
            ff.iterateDir(path, (name, type) -> {
                if (Files.isDir(name, type, nameSink)) {
                    path.trimTo(rootLen);
                    if (isWalTable(nameSink, path, ff)) {
                        try (TableSequencer sequencer = openSequencer(nameSink)) {
                            callback.onTable(sequencer.getTableId(), nameSink, sequencer.lastTxn());
                        }
                    }
                }
            });
        }
    }

    public @NotNull TransactionLogCursor getCursor(final CharSequence tableName, long seqTxn) {
        try (TableSequencer tableSequencer = openSequencer(tableName)) {
            return tableSequencer.getTransactionLogCursor(seqTxn);
        }
    }

    public int getNextWalId(final CharSequence tableName) {
        try (TableSequencer tableSequencer = openSequencer(tableName)) {
            return tableSequencer.getNextWalId();
        }
    }

    public TableMetadataChangeLog getMetadataChangeLogCursor(final CharSequence tableName, long structureVersionLo) {
        try (TableSequencer tableSequencer = openSequencer(tableName)) {
            return tableSequencer.getMetadataChangeLogCursor(structureVersionLo);
        }
    }

    public boolean hasSequencer(final CharSequence tableName) {
        TableSequencer tableSequencer = seqRegistry.get(tableName);
        if (tableSequencer != null) {
            return true;
        }
        return isWalTable(tableName, getConfiguration().getRoot(), getConfiguration().getFilesFacade());
    }

    public long lastTxn(final CharSequence tableName) {
        try (TableSequencer sequencer = openSequencer(tableName)) {
            return sequencer.lastTxn();
        }
    }

    public boolean isSuspended(final CharSequence tableName) {
        try (TableSequencer sequencer = openSequencer(tableName)) {
            return sequencer.isSuspended();
        }
    }

    public void suspendTable(final CharSequence tableName) {
        try (TableSequencer sequencer = openSequencer(tableName)) {
            sequencer.suspendTable();
        }
    }

    public long nextStructureTxn(final CharSequence tableName, long structureVersion, AlterOperation operation) {
        try (TableSequencer tableSequencer = openSequencer(tableName)) {
            return tableSequencer.nextStructureTxn(structureVersion, operation);
        }
    }

    public long nextTxn(final CharSequence tableName, int walId, long expectedSchemaVersion, int segmentId, long segmentTxn) {
        try (TableSequencer tableSequencer = openSequencer(tableName)) {
            return tableSequencer.nextTxn(expectedSchemaVersion, walId, segmentId, segmentTxn);
        }
    }

    public void reloadMetadataConditionally(
            final CharSequence tableName,
            long expectedStructureVersion,
            TableRecordMetadataSink sink,
            boolean compress
    ) {
        try (TableSequencer tableSequencer = openSequencer(tableName)) {
            if (tableSequencer.getStructureVersion() != expectedStructureVersion) {
                tableSequencer.getTableMetadata(sink, compress);
            }
        }
    }

    public void registerTable(int tableId, final TableStructure tableStructure) {
        //noinspection EmptyTryBlock
        try (TableSequencerImpl ignore = createSequencer(tableId, tableStructure)) {
        }
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

    private @NotNull TableSequencerImpl createSequencer(int tableId, final TableStructure tableStructure) {
        throwIfClosed();
        final String tableName = Chars.toString(tableStructure.getTableName());
        return seqRegistry.compute(tableName, (key, value) -> {
            TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, tableName);
            sequencer.create(tableId, tableStructure);
            sequencer.open();
            if (value != null) {
                value.close();
            }
            return sequencer;
        });
    }

    private @NotNull TableSequencerImpl openSequencer(final CharSequence tableName) {
        throwIfClosed();

        // todo: too much GC
        final String tableNameStr = Chars.toString(tableName);

        TableSequencerEntry entry = seqRegistry.get(tableNameStr);
        if (entry != null && !entry.isDistressed()) {
            return entry;
        }

        if (entry != null) {
            // Remove distressed entry
            seqRegistry.remove(tableNameStr, entry);
        }

        entry = seqRegistry.computeIfAbsent(tableNameStr, (key) -> {
            if (isWalTable(tableNameStr, getConfiguration().getRoot(), getConfiguration().getFilesFacade())) {
                TableSequencerEntry sequencer = new TableSequencerEntry(this, this.engine, tableNameStr);
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

    @Override
    protected boolean releaseAll(long deadline) {
        return releaseEntries(deadline);
    }

    private boolean releaseEntries(long deadline) {
        if (seqRegistry.size() == 0) {
            // nothing to release
            return true;
        }
        boolean removed = false;
        final Iterator<TableSequencerEntry> iterator = seqRegistry.values().iterator();
        while (iterator.hasNext()) {
            final TableSequencerEntry sequencer = iterator.next();
            if (deadline >= sequencer.releaseTime) {
                sequencer.pool = null;
                sequencer.close();
                removed = true;
                iterator.remove();
            }
        }
        return removed;
    }

    private boolean returnToPool(final TableSequencerEntry entry) {
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

    @FunctionalInterface
    public interface RegisteredTable {
        void onTable(int tableId, final CharSequence tableName, long lastTxn);
    }

    private static class TableSequencerEntry extends TableSequencerImpl {
        private TableSequencerAPI pool;
        private volatile long releaseTime = Long.MAX_VALUE;

        TableSequencerEntry(TableSequencerAPI pool, CairoEngine engine, String tableName) {
            super(engine, tableName);
            this.pool = pool;
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

        public void reset() {
            this.releaseTime = Long.MAX_VALUE;
        }
    }
}
