/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.Telemetry;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DdlListener;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.std.str.CharSink;
import io.questdb.tasks.TelemetryWalTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class WalWriterPool extends AbstractMultiTenantPool<WalWriterPool.WalWriterTenant> {

    private final CairoEngine engine;

    public WalWriterPool(CairoConfiguration configuration, CairoEngine engine) {
        super(configuration, configuration.getWalWriterPoolMaxSegments(), configuration.getInactiveWalWriterTTL());
        this.engine = engine;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_WAL_WRITER;
    }

    @Override
    protected WalWriterTenant newTenant(
            TableToken tableToken,
            Entry<WalWriterTenant> rootEntry,
            Entry<WalWriterTenant> entry,
            int index,
            @Nullable ResourcePoolSupervisor<WalWriterTenant> supervisor
    ) {
        return new WalWriterTenant(
                this,
                rootEntry,
                entry,
                index,
                tableToken,
                engine.getTableSequencerAPI(),
                engine.getDdlListener(tableToken),
                engine.getWalDirectoryPolicy(),
                engine.getWalLocker(),
                engine.getRecentWriteTracker(),
                engine.getTelemetryWal()
        );
    }

    public static class WalWriterTenant extends WalWriter implements PoolTenant<WalWriterTenant> {
        private final int index;
        private final Entry<WalWriterTenant> rootEntry;
        private Entry<WalWriterTenant> entry;
        private AbstractMultiTenantPool<WalWriterTenant> pool;

        private WalWriterTenant(
                AbstractMultiTenantPool<WalWriterTenant> pool,
                Entry<WalWriterTenant> rootEntry,
                Entry<WalWriterTenant> entry,
                int index,
                TableToken tableToken,
                TableSequencerAPI tableSequencerAPI,
                DdlListener ddlListener,
                WalDirectoryPolicy walDirectoryPolicy,
                WalLocker walLocker,
                RecentWriteTracker recentWriteTracker,
                Telemetry<TelemetryWalTask> telemetryWal
        ) {
            super(
                    pool.getConfiguration(),
                    tableToken,
                    tableSequencerAPI,
                    ddlListener,
                    walDirectoryPolicy,
                    walLocker,
                    recentWriteTracker,
                    telemetryWal
            );
            this.pool = pool;
            this.rootEntry = rootEntry;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            if (isOpen()) {
                rollback();
                final AbstractMultiTenantPool<WalWriterTenant> pool = this.pool;
                if (pool != null && entry != null) {
                    if (!isDistressed()) {
                        if (pool.returnToPool(this)) {
                            return;
                        }
                    } else {
                        try {
                            super.close();
                        } finally {
                            pool.expelFromPool(this);
                        }
                        return;
                    }
                }
                super.close();
            }
        }

        @Override
        public Entry<WalWriterTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public Entry<WalWriterTenant> getRootEntry() {
            return rootEntry;
        }

        @Override
        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void refresh(@Nullable ResourcePoolSupervisor<WalWriterTenant> supervisor) {
            try {
                goActive();
            } catch (Throwable ex) {
                close();
                throw ex;
            }
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("WalWriterTenant{index=").put(index).put(", tableToken=").put(getTableToken()).put('}');
        }

        @Override
        public void updateTableToken(TableToken tableToken) {
            super.updateTableToken(tableToken);
        }
    }
}