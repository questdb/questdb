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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.ViewWalWriter;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalLocker;
import io.questdb.cairo.wal.seq.TableSequencerAPI;
import io.questdb.std.str.CharSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ViewWalWriterPool extends AbstractMultiTenantPool<ViewWalWriterPool.ViewWalWriterTenant> {

    private final CairoEngine engine;

    public ViewWalWriterPool(CairoConfiguration configuration, CairoEngine engine) {
        super(configuration, configuration.getViewWalWriterPoolMaxSegments(), configuration.getInactiveViewWalWriterTTL());
        this.engine = engine;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_VIEW_WAL_WRITER;
    }

    @Override
    protected ViewWalWriterTenant newTenant(
            TableToken tableToken,
            Entry<ViewWalWriterTenant> rootEntry,
            Entry<ViewWalWriterTenant> entry,
            int index,
            @Nullable ResourcePoolSupervisor<ViewWalWriterTenant> supervisor
    ) {
        return new ViewWalWriterTenant(
                this,
                rootEntry,
                entry,
                index,
                tableToken,
                engine.getTableSequencerAPI(),
                engine.getWalDirectoryPolicy(),
                engine.getWalLocker()
        );
    }

    public static class ViewWalWriterTenant extends ViewWalWriter implements PoolTenant<ViewWalWriterTenant> {
        private final int index;
        private final Entry<ViewWalWriterTenant> rootEntry;
        private Entry<ViewWalWriterTenant> entry;
        private AbstractMultiTenantPool<ViewWalWriterTenant> pool;

        private ViewWalWriterTenant(
                AbstractMultiTenantPool<ViewWalWriterTenant> pool,
                Entry<ViewWalWriterTenant> rootEntry,
                Entry<ViewWalWriterTenant> entry,
                int index,
                TableToken tableToken,
                TableSequencerAPI tableSequencerAPI,
                WalDirectoryPolicy walDirectoryPolicy,
                WalLocker walLocker
        ) {
            super(pool.getConfiguration(), tableToken, tableSequencerAPI, walDirectoryPolicy, walLocker);
            this.pool = pool;
            this.rootEntry = rootEntry;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            if (isOpen()) {
                final AbstractMultiTenantPool<ViewWalWriterTenant> pool = this.pool;
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
        public Entry<ViewWalWriterTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public Entry<ViewWalWriterTenant> getRootEntry() {
            return rootEntry;
        }

        @Override
        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void refresh(@Nullable ResourcePoolSupervisor<ViewWalWriterTenant> supervisor) {
        }

        @Override
        public void toSink(@NotNull CharSink<?> sink) {
            sink.put("ViewWalWriterTenant{index=").put(index).put(", tableToken=").put(getTableToken()).put('}');
        }

        @Override
        public void updateTableToken(TableToken ignoredTableToken) {
        }
    }
}