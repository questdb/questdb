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
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.SimpleReadWriteLock;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.locks.ReadWriteLock;

import static io.questdb.cairo.wal.WalUtils.WAL_INDEX_FILE_NAME;

public class SequencerImpl implements Sequencer {

    private final ReadWriteLock schemaLock = new SimpleReadWriteLock();
    private final CairoEngine engine;
    private final String tableName;
    private final int rootLen;
    private final SequencerMetadata metadata;
    private final TxnCatalog catalog;
    private final IDGenerator walIdGenerator;
    private final Path path;
    private final BinaryAlterFormatter alterCommandWalFormatter = new BinaryAlterFormatter();
    private final SequencerMetadataUpdater sequencerMetadataUpdater;
    private final FilesFacade ff;
    private final int mkDirMode;
    private volatile boolean open = false;

    SequencerImpl(CairoEngine engine, String tableName) {
        this.engine = engine;
        this.tableName = tableName;

        final CairoConfiguration configuration = engine.getConfiguration();
        final FilesFacade ff = configuration.getFilesFacade();
        try {
            CharSequence systemTableName = engine.getSystemTableName(tableName);
            path = new Path().of(configuration.getRoot()).concat(systemTableName).concat(SEQ_DIR);
            rootLen = path.length();
            this.ff = ff;
            this.mkDirMode = configuration.getMkDirMode();

            metadata = new SequencerMetadata(ff, SequencerMetadata.READ_WRITE);
            sequencerMetadataUpdater = new SequencerMetadataUpdater(metadata, tableName);
            walIdGenerator = new IDGenerator(configuration, WAL_INDEX_FILE_NAME);
            catalog = new TxnCatalog(ff);
        } catch (Throwable th) {
            doClose();
            throw th;
        }
    }

    public void open() {
        schemaLock.writeLock().lock();
        try {
            walIdGenerator.open(path);
            metadata.open(tableName, path, rootLen);
            catalog.open(path);
            open = true;
        } catch (Throwable th) {
            doClose();
            throw th;
        } finally {
            schemaLock.writeLock().unlock();
        }
    }

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void copyMetadataTo(@NotNull SequencerMetadata copyTo) {
        schemaLock.readLock().lock();
        try {
            copyTo.copyFrom(metadata);
        } finally {
            schemaLock.readLock().unlock();
        }
    }

    @Override
    public int getTableId() {
        return metadata.getTableId();
    }

    @Override
    public int getNextWalId() {
        return (int) walIdGenerator.getNextId();
    }

    @Override
    public long nextStructureTxn(long expectedSchemaVersion, AlterOperation operation) {
        // Writing to Sequencer can happen from multiple threads, so we need to protect against concurrent writes.
        schemaLock.writeLock().lock();
        long txn;
        try {
            if (metadata.getStructureVersion() == expectedSchemaVersion) {
                txn = catalog.addMetadataChangeEntry(expectedSchemaVersion + 1, alterCommandWalFormatter, operation);
                try {
                    applyToMetadata(operation);
                    assert metadata.getStructureVersion() == expectedSchemaVersion + 1;
                } catch (Throwable th) {
                    // TODO: handle errors in updating the metadata by destroying the sequencer and safe reload from txn catalog
                    throw th;
                }
            } else {
                return NO_TXN;
            }
        } finally {
            schemaLock.writeLock().unlock();
        }
        engine.notifyWalTxnCommitted(metadata.getTableId(), tableName, txn);
        return txn;
    }

    @Override
    public SequencerStructureChangeCursor getStructureChangeCursor(
            @Nullable SequencerStructureChangeCursor reusableCursor,
            long fromSchemaVersion
    ) {
        schemaLock.readLock().lock();
        try {
            if (metadata.getStructureVersion() == fromSchemaVersion) {
                // Nothing to do.
                if (reusableCursor != null) {
                    return reusableCursor.empty();
                }
                return null;
            }
        } finally {
            schemaLock.readLock().unlock();
        }
        return catalog.getStructureChangeCursor(reusableCursor, fromSchemaVersion, alterCommandWalFormatter);
    }

    @Override
    public long nextTxn(long expectedSchemaVersion, int walId, long segmentId, long segmentTxn) {
        // Writing to Sequencer can happen from multiple threads, so we need to protect against concurrent writes.
        schemaLock.writeLock().lock();
        long txn;
        try {
            if (metadata.getStructureVersion() == expectedSchemaVersion) {
                txn = nextTxn(walId, segmentId, segmentTxn);
            } else {
                return NO_TXN;
            }
        } finally {
            schemaLock.writeLock().unlock();
        }
        engine.notifyWalTxnCommitted(metadata.getTableId(), tableName, txn);
        return txn;
    }

    public long lastTxn() {
        schemaLock.readLock().lock();
        try {
            return catalog.lastTxn();
        } finally {
            schemaLock.readLock().unlock();
        }
    }

    private void applyToMetadata(AlterOperation operation) {
        try {
            operation.apply(sequencerMetadataUpdater, true);
        } catch (SqlException e) {
            throw CairoException.critical(0).put("error applying alter command to sequencer metadata [error=").put(e.getFlyweightMessage()).put(']');
        }
    }

    @Override
    public SequencerCursor getCursor(long lastCommittedTxn) {
        return catalog.getCursor(lastCommittedTxn);
    }

    @Override
    public void close() {
        doClose();
    }

    private void doClose() {
        if (open) {
            schemaLock.writeLock().lock();
            try {
                open = false;
                Misc.free(metadata);
                Misc.free(catalog);
                Misc.free(walIdGenerator);
                Misc.free(path);
            } finally {
                schemaLock.writeLock().unlock();
            }
        }
    }
    private void createSequencerDir(FilesFacade ff, int mkDirMode) {
        if (ff.mkdirs(path.slash$(), mkDirMode) != 0) {
            throw CairoException.critical(ff.errno()).put("Cannot create sequencer directory: ").put(path);
        }
        path.trimTo(rootLen);
    }

    private long nextTxn(int walId, long segmentId, long segmentTxn) {
        return catalog.addEntry(walId, segmentId, segmentTxn);
    }

    void create(int tableId, TableStructure model) {
        schemaLock.writeLock().lock();
        try {
            createSequencerDir(ff, mkDirMode);
            metadata.create(model, tableName, path, rootLen, tableId);
        } finally {
            schemaLock.writeLock().unlock();
        }
    }
}
