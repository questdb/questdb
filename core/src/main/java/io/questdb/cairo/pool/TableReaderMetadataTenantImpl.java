/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.pool;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TxReader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.str.Path;

import java.io.Closeable;

import static io.questdb.cairo.TableUtils.TXN_FILE_NAME;

class TableReaderMetadataTenantImpl extends TableReaderMetadata implements PoolTenant<TableReaderMetadataTenantImpl>, Closeable {
    private static final Log LOG = LogFactory.getLog(TableReaderMetadataTenantImpl.class);
    private final MillisecondClock clock;
    private final int index;
    private AbstractMultiTenantPool.Entry<TableReaderMetadataTenantImpl> entry;
    private boolean initialized;
    private AbstractMultiTenantPool<TableReaderMetadataTenantImpl> pool;
    private long rowCount;
    private TxReader txFile;
    private long txn = TableUtils.INITIAL_TXN;

    TableReaderMetadataTenantImpl(
            AbstractMultiTenantPool<TableReaderMetadataTenantImpl> pool,
            AbstractMultiTenantPool.Entry<TableReaderMetadataTenantImpl> entry,
            int index,
            TableToken tableToken,
            boolean lazy
    ) {
        super(pool.getConfiguration(), tableToken);
        this.clock = configuration.getMillisecondClock();
        if (!lazy) {
            TableReaderMetadataTenantImpl.this.initialize(configuration, tableToken);
        }
        this.pool = pool;
        this.entry = entry;
        this.index = index;
    }

    @Override
    public void close() {
        if (pool != null && getEntry() != null) {
            if (pool.returnToPool(this)) {
                return;
            }
        }
        this.txFile = Misc.free(txFile);
        super.close();
    }

    @Override
    public AbstractMultiTenantPool.Entry<TableReaderMetadataTenantImpl> getEntry() {
        return entry;
    }

    @Override
    public int getIndex() {
        return index;
    }

    public long getMaxTimestamp() {
        return txFile.getMaxTimestamp();
    }

    public long getMinTimestamp() {
        return txFile.getMinTimestamp();
    }

    public long getTransientRowCount() {
        return txFile.getTransientRowCount();
    }

    public long getTxn() {
        return txn;
    }

    public long getVersion() {
        return this.txFile.getMetadataVersion();
    }

    public void goodbye() {
        entry = null;
        pool = null;
    }

    @Override
    public void refresh(ResourcePoolSupervisor<TableReaderMetadataTenantImpl> supervisor) {
        reload();
    }

    public void reload() {
        if (!initialized) {
            initialize(configuration, getTableToken());
        } else {
            if (acquireTxn()) {
                return;
            }
            reloadSlow();
            // partition reload will apply truncate if necessary
            // applyTruncate for non-partitioned tables only
        }
    }

    public long size() {
        return rowCount;
    }

    private boolean acquireTxn() {
        // txFile can also be reloaded in goPassive->checkSchedulePurgeO3Partitions
        // if txFile txn doesn't much reader txn, reader has to be slow reloaded
        if (txn == txFile.getTxn()) {
            // We have to be sure last txn is acquired in Scoreboard
            // otherwise writer can delete partition version files
            // between reading txn file and acquiring txn in the Scoreboard.
            Unsafe.getUnsafe().loadFence();
            return txFile.getVersion() == txFile.unsafeReadVersion();
        }
        return false;
    }

    private void initialize(CairoConfiguration configuration, TableToken tableToken) {
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(tableToken);
            loadMetadata();
            this.txFile = new TxReader(
                    configuration.getFilesFacade()).ofRO(path.concat(TXN_FILE_NAME).$(),
                    this.getTimestampType(),
                    getPartitionBy()
            );
            initialized = true;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    private void readTxnSlow(long deadline) {
        int count = 0;

        while (true) {
            if (txFile.unsafeLoadAll()) {
                // good, very stable, congrats
                long txn = txFile.getTxn();
                this.txn = txn;

                if (acquireTxn()) {
                    this.rowCount = txFile.getFixedRowCount() + txFile.getTransientRowCount();
                    LOG.debug()
                            .$("new transaction [txn=").$(txn)
                            .$(", transientRowCount=").$(txFile.getTransientRowCount())
                            .$(", fixedRowCount=").$(txFile.getFixedRowCount())
                            .$(", maxTimestamp=").$ts(ColumnType.getTimestampDriver(getTimestampType()), txFile.getMaxTimestamp())
                            .$(", attempts=").$(count)
                            .$(", thread=").$(Thread.currentThread().getName())
                            .$(']').$();
                    break;
                }
            }
            // This is unlucky, sequences have changed while we were reading transaction data
            // We must discard and try again
            count++;
            if (clock.getTicks() > deadline) {
                throw CairoException.critical(0).put("Transaction read timeout [src=metadata, timeout=").put(configuration.getSpinLockTimeout()).put("ms]");
            }
            Os.pause();
        }
    }

    private boolean reloadMetadata(long txnMetadataVersion, long deadline) {
        // create transition index, which will help us reuse already open resources
        if (txnMetadataVersion == getMetadataVersion()) {
            return true;
        }

        while (true) {
            try {
                if (!prepareTransition(txnMetadataVersion)) {
                    if (clock.getTicks() < deadline) {
                        return false;
                    }
                    throw CairoException.critical(0).put("Metadata read timeout [src=metadata, timeout=").put(configuration.getSpinLockTimeout()).put("ms]");
                }
            } catch (CairoException ex) {
                // This is temporary solution until we can get multiple version of metadata not overwriting each other
                TableUtils.handleMetadataLoadException(getTableToken(), deadline, ex, configuration.getMillisecondClock(), configuration.getSpinLockTimeout());
                continue;
            }

            applyTransition();
            return true;
        }
    }

    private void reloadSlow() {
        final long deadline = clock.getTicks() + configuration.getSpinLockTimeout();
        do {
            // Reload txn
            readTxnSlow(deadline);
            // Reload _meta if structure version updated, reload _cv if column version updated
            // Start again if _meta with matching structure version cannot be loaded
        } while (!reloadMetadata(txFile.getMetadataVersion(), deadline));
    }
}
