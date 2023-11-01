/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.Metrics;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DdlListener;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalDirectoryPolicy;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.cairo.wal.seq.TableSequencerAPI;

public class WalWriterPool extends AbstractMultiTenantPool<WalWriterPool.WalWriterTenant> {

    private final CairoEngine engine;

    public WalWriterPool(CairoConfiguration configuration, CairoEngine engine) {
        super(configuration, configuration.getReaderPoolMaxSegments(), configuration.getInactiveReaderTTL());
        this.engine = engine;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_WAL_WRITER;
    }

    @Override
    protected WalWriterTenant newTenant(TableToken tableToken, Entry<WalWriterTenant> entry, int index) {
        return new WalWriterTenant(
                this,
                entry,
                index,
                tableToken,
                engine.getTableSequencerAPI(),
                engine.getDdlListener(tableToken),
                engine.getWalDirectoryPolicy(),
                engine.getMetrics()
        );
    }

    public static class WalWriterTenant extends WalWriter implements PoolTenant {
        private final int index;
        private Entry<WalWriterTenant> entry;
        private AbstractMultiTenantPool<WalWriterTenant> pool;

        public WalWriterTenant(
                AbstractMultiTenantPool<WalWriterTenant> pool,
                Entry<WalWriterTenant> entry,
                int index,
                TableToken tableToken,
                TableSequencerAPI tableSequencerAPI,
                DdlListener ddlListener,
                WalDirectoryPolicy walDirectoryPolicy,
                Metrics metrics
        ) {
            super(pool.getConfiguration(), tableToken, tableSequencerAPI, ddlListener, walDirectoryPolicy, metrics);
            this.pool = pool;
            this.entry = entry;
            this.index = index;
        }

        @Override
        public void close() {
            if (isOpen()) {
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

        @SuppressWarnings("unchecked")
        @Override
        public Entry<WalWriterTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public void refresh() {
            try {
                goActive();
            } catch (Throwable ex) {
                close();
                throw ex;
            }
        }
    }
}