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

import io.questdb.cairo.*;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cairo.wal.WalUtils.WAL_INDEX_FILE_NAME;

public class TableSequencerImpl implements TableSequencer {
    private static final Log LOG = LogFactory.getLog(TableSequencerImpl.class);
    private final static BinaryAlterSerializer alterCommandWalFormatter = new BinaryAlterSerializer();
    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();
    private final EmptyOperationCursor emptyOperationCursor = new EmptyOperationCursor();
    private final CairoEngine engine;
    private final String systemTableName;
    private final int rootLen;
    private final SequencerMetadata metadata;
    private final TableTransactionLog tableTransactionLog;
    private final IDGenerator walIdGenerator;
    private final Path path;
    private final SequencerMetadataUpdater sequencerMetadataUpdater;
    private final FilesFacade ff;
    private final int mkDirMode;
    private volatile String tableName;
    private volatile boolean closed = false;
    private boolean distressed;

    TableSequencerImpl(CairoEngine engine, String systemTableName, String tableName) {
        this.engine = engine;
        this.systemTableName = systemTableName;
        this.tableName = tableName;

        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try {
            path = new Path();
            path.of(configuration.getRoot()).concat(systemTableName).concat(WalUtils.SEQ_DIR);
            rootLen = path.length();
            this.ff = ff;
            this.mkDirMode = configuration.getMkDirMode();

            metadata = new SequencerMetadata(ff);
            sequencerMetadataUpdater = new SequencerMetadataUpdater(metadata, systemTableName);
            walIdGenerator = new IDGenerator(configuration, WAL_INDEX_FILE_NAME);
            tableTransactionLog = new TableTransactionLog(ff);
        } catch (Throwable th) {
            LOG.critical().$("could not create sequencer [name=").utf8(systemTableName)
                    .$(", error=").$(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
    }

    @Override
    public void close() {
        schemaLock.writeLock().lock();
        try {
            closeLocked();
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    @Override
    public void dropTable() {
        checkDropped();
        tableTransactionLog.addEntry(WalUtils.DROP_TABLE_WALID, 0, 0);
        metadata.dropTable();
        engine.notifyWalTxnCommitted(metadata.getTableId(), systemTableName, Integer.MAX_VALUE);
    }

    @Override
    public TableMetadataChangeLog getMetadataChangeLogCursor(long structureVersionLo) {
        checkDropped();
        if (metadata.getStructureVersion() == structureVersionLo) {
            // Nothing to do.
            return emptyOperationCursor.of(tableName);
        }
        return tableTransactionLog.getTableMetadataChangeLog(structureVersionLo, alterCommandWalFormatter, tableName);
    }

    @Override
    public TransactionLogCursor getTransactionLogCursor(long seqTxn) {
        checkDropped();
        return tableTransactionLog.getCursor(seqTxn);
    }

    @Override
    public int getNextWalId() {
        return (int) walIdGenerator.getNextId();
    }

    @Override
    public long getStructureVersion() {
        return metadata.getStructureVersion();
    }

    @Override
    public int getTableId() {
        return metadata.getTableId();
    }

    @Override
    public void getTableMetadata(@NotNull TableRecordMetadataSink sink, boolean compress) {
        int columnCount = metadata.getColumnCount();
        int timestampIndex = metadata.getTimestampIndex();
        int compressedTimestampIndex = -1;
        sink.clear();

        int compressedColumnCount = 0;
        for (int i = 0; i < columnCount; i++) {
            int columnType = metadata.getColumnType(i);
            if (columnType > -1 || !compress) {
                sink.addColumn(
                        metadata.getColumnName(i),
                        columnType,
                        metadata.isColumnIndexed(i),
                        metadata.getIndexValueBlockCapacity(i),
                        metadata.isSymbolTableStatic(i),
                        i
                );
                if (i == timestampIndex) {
                    compressedTimestampIndex = compressedColumnCount;
                }
                compressedColumnCount++;
            }
        }

        sink.of(
                systemTableName,
                metadata.getTableId(),
                compressedTimestampIndex,
                metadata.isSuspended(),
                metadata.getStructureVersion(),
                compressedColumnCount
        );
    }

    @Override
    public boolean isSuspended() {
        return metadata.isSuspended();
    }

    @Override
    public long lastTxn() {
        return tableTransactionLog.lastTxn();
    }

    @Override
    public long nextStructureTxn(long expectedStructureVersion, TableMetadataChange change) {
        checkDropped();
        long txn;
        try {
            if (metadata.getStructureVersion() == expectedStructureVersion) {
                long offset = tableTransactionLog.beginMetadataChangeEntry(expectedStructureVersion + 1, alterCommandWalFormatter, change);

                applyToMetadata(change);
                if (metadata.getStructureVersion() != expectedStructureVersion + 1) {
                    throw CairoException.critical(0)
                            .put("applying structure change to WAL table failed [table=").put(systemTableName)
                            .put(", oldVersion: ").put(expectedStructureVersion)
                            .put(", newVersion: ").put(metadata.getStructureVersion())
                            .put(']');
                }
                txn = tableTransactionLog.endMetadataChangeEntry(expectedStructureVersion + 1, offset);
            } else {
                return NO_TXN;
            }
        } catch (Throwable th) {
            distressed = true;
            LOG.critical().$("could not apply structure change to WAL table sequencer [table=").utf8(systemTableName)
                    .$(", error=").$(th.getMessage())
                    .I$();
            throw th;
        }

        if (!metadata.isSuspended()) {
            engine.notifyWalTxnCommitted(metadata.getTableId(), systemTableName, txn);
        }
        return txn;
    }

    @Override
    public long nextTxn(long expectedSchemaVersion, int walId, int segmentId, long segmentTxn) {
        checkDropped();
        long txn;
        try {
            if (metadata.getStructureVersion() == expectedSchemaVersion) {
                txn = nextTxn(walId, segmentId, segmentTxn);
            } else {
                return NO_TXN;
            }
        } catch (Throwable th) {
            distressed = true;
            LOG.critical().$("could not apply transaction to WAL table sequencer [table=").utf8(systemTableName)
                    .$(", error=").$(th.getMessage())
                    .I$();
            throw th;
        }

        if (!metadata.isSuspended()) {
            engine.notifyWalTxnCommitted(metadata.getTableId(), systemTableName, txn);
        }
        return txn;
    }

    @Override
    public void rename(String newTableName) {
        tableName = newTableName;
    }

    @Override
    public void suspendTable() {
        metadata.suspendTable();
    }

    public String getTableName() {
        return systemTableName;
    }

    public boolean isClosed() {
        return closed;
    }

    public boolean isDistressed() {
        return distressed;
    }

    public void open() {
        try {
            walIdGenerator.open(path);
            metadata.open(path, rootLen);
            tableTransactionLog.open(path);
        } catch (Throwable th) {
            LOG.critical().$("could not open sequencer [name=").utf8(systemTableName)
                    .$(", path=").$(path)
                    .$(", error=").$(th.getMessage())
                    .I$();
            closeLocked();
            throw th;
        }
    }

    @TestOnly
    public void setDistressed() {
        this.distressed = true;
    }

    private void checkDropped() {
        if (metadata.isDropped()) {
            throw TableDroppedException.instance().put("table is dropped [systemTableName=").put(metadata.getSystemTableName()).put(']');
        }
    }

    private void applyToMetadata(TableMetadataChange change) {
        try {
            change.apply(sequencerMetadataUpdater, true);
            metadata.syncToMetaFile();
        } catch (SqlException e) {
            throw CairoException.critical(0).put("error applying alter command to sequencer metadata [error=").put(e.getFlyweightMessage()).put(']');
        }
    }

    void closeLocked() {
        if (!closed) {
            closed = true;
            Misc.free(metadata);
            Misc.free(tableTransactionLog);
            Misc.free(walIdGenerator);
            Misc.free(path);
        }
    }

    void create(int tableId, TableStructure model) {
        schemaLock.writeLock().lock();
        try {
            createSequencerDir(ff, mkDirMode);
            metadata.create(model, systemTableName, path, rootLen, tableId);
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    private void createSequencerDir(FilesFacade ff, int mkDirMode) {
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            final CairoException e = CairoException.critical(ff.errno()).put("Cannot create sequencer directory: ").put(path);
            closeLocked();
            throw e;
        }
        path.trimTo(rootLen);
    }

    private long nextTxn(int walId, int segmentId, long segmentTxn) {
        return tableTransactionLog.addEntry(walId, segmentId, segmentTxn);
    }

    void readLock() {
        schemaLock.readLock().lock();
    }

    void unlockRead() {
        schemaLock.readLock().unlock();
    }

    void unlockWrite() {
        schemaLock.writeLock().unlock();
    }

    void writeLock() {
        schemaLock.writeLock().lock();
    }
}
