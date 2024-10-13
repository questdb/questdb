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
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.GenericTableRecordMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.TableSequencerAPI;

public class SequencerMetadataPool extends AbstractMultiTenantPool<MetadataPoolTenant> {
    private final CairoEngine engine;

    public SequencerMetadataPool(CairoConfiguration configuration, CairoEngine engine) {
        super(configuration, configuration.getMetadataPoolCapacity(), configuration.getInactiveReaderTTL());
        this.engine = engine;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_SEQUENCER_METADATA;
    }

    @Override
    protected MetadataPoolTenant newTenant(TableToken tableToken, Entry<MetadataPoolTenant> entry, int index) {
        return new SequencerMetadataTenantImpl(this, entry, index, tableToken, engine.getTableSequencerAPI());
    }

    private static class SequencerMetadataTenantImpl extends GenericTableRecordMetadata implements MetadataPoolTenant {
        private final int index;
        private final TableSequencerAPI tableSequencerAPI;
        private final TableToken tableToken;
        private AbstractMultiTenantPool.Entry<MetadataPoolTenant> entry;
        private AbstractMultiTenantPool<MetadataPoolTenant> pool;

        public SequencerMetadataTenantImpl(
                AbstractMultiTenantPool<MetadataPoolTenant> pool,
                Entry<MetadataPoolTenant> entry,
                int index,
                TableToken tableToken,
                TableSequencerAPI tableSequencerAPI
        ) {
            this.pool = pool;
            this.entry = entry;
            this.index = index;
            this.tableSequencerAPI = tableSequencerAPI;
            this.tableToken = tableToken;
            tableSequencerAPI.getTableMetadata(tableToken, this);
        }

        @Override
        public void close() {
            if (pool != null && getEntry() != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            super.close();
        }

        @Override
        public AbstractMultiTenantPool.Entry<MetadataPoolTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public int getMaxUncommittedRows() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getO3MaxLag() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getPartitionBy() {
            throw new UnsupportedOperationException();
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isSoftLink() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void refresh() {
            tableSequencerAPI.reloadMetadataConditionally(tableToken, getMetadataVersion(), this);
        }
    }
}
