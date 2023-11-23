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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.seq.TableSequencerAPI;

public class MetadataPool extends AbstractMultiTenantPool<MetadataPool.MetadataTenant> {
    private final CairoEngine engine;

    public MetadataPool(CairoConfiguration configuration, CairoEngine engine) {
        super(configuration, configuration.getMetadataPoolCapacity(), configuration.getInactiveReaderTTL());
        this.engine = engine;
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_METADATA;
    }

    @Override
    protected MetadataTenant newTenant(TableToken tableToken, Entry<MetadataTenant> entry, int index) {
        if (engine.isWalTable(tableToken)) {
            return new SequencerMetadataTenant(this, entry, index, tableToken, engine.getTableSequencerAPI());
        }
        return new TableReaderMetadataTenant(this, entry, index, tableToken, false);
    }

    public interface MetadataTenant extends TableMetadata, PoolTenant {
    }

    private static class SequencerMetadataTenant extends GenericTableRecordMetadata implements MetadataTenant {
        private final int index;
        private final TableReaderMetadataTenant readerMetadataTenant;
        private final TableSequencerAPI tableSequencerAPI;
        private final TableToken tableToken;
        private AbstractMultiTenantPool.Entry<MetadataTenant> entry;
        private AbstractMultiTenantPool<MetadataTenant> pool;
        private boolean tableReaderMetadataRefreshPending;

        public SequencerMetadataTenant(
                AbstractMultiTenantPool<MetadataTenant> pool,
                Entry<MetadataTenant> entry,
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
            this.readerMetadataTenant = new TableReaderMetadataTenant(pool, entry, index, tableToken, true);
            tableReaderMetadataRefreshPending = true;
        }

        @Override
        public void close() {
            if (pool != null && getEntry() != null) {
                if (pool.returnToPool(this)) {
                    return;
                }
            }
            super.close();
            readerMetadataTenant.goodbye();
            readerMetadataTenant.close();
        }

        @SuppressWarnings("unchecked")
        @Override
        public AbstractMultiTenantPool.Entry<MetadataTenant> getEntry() {
            return entry;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public int getMaxUncommittedRows() {
            lazilyRefreshReaderMetadata();
            return readerMetadataTenant.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            lazilyRefreshReaderMetadata();
            return readerMetadataTenant.getO3MaxLag();
        }

        @Override
        public int getPartitionBy() {
            lazilyRefreshReaderMetadata();
            return readerMetadataTenant.getPartitionBy();
        }

        @Override
        public boolean isSoftLink() {
            lazilyRefreshReaderMetadata();
            return readerMetadataTenant.isSoftLink();
        }

        public void goodbye() {
            entry = null;
            pool = null;
        }

        @Override
        public boolean isDedupKey(int columnIndex) {
            lazilyRefreshReaderMetadata();
            return readerMetadataTenant.isDedupKey(columnIndex);
        }

        @Override
        public void refresh() {
            tableSequencerAPI.reloadMetadataConditionally(tableToken, getMetadataVersion(), this);
            tableReaderMetadataRefreshPending = true;
        }

        private void lazilyRefreshReaderMetadata() {
            if (tableReaderMetadataRefreshPending) {
                readerMetadataTenant.reload();
                tableReaderMetadataRefreshPending = false;
            }
        }

    }

    private static class TableReaderMetadataTenant extends DynamicTableReaderMetadata implements MetadataTenant {
        private final int index;
        private AbstractMultiTenantPool.Entry<MetadataTenant> entry;
        private AbstractMultiTenantPool<MetadataTenant> pool;

        public TableReaderMetadataTenant(
                AbstractMultiTenantPool<MetadataTenant> pool,
                AbstractMultiTenantPool.Entry<MetadataTenant> entry,
                int index,
                TableToken tableToken,
                boolean lazy
        ) {
            super(pool.getConfiguration(), tableToken, lazy);
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
            super.close();
        }

        @SuppressWarnings("unchecked")
        @Override
        public AbstractMultiTenantPool.Entry<MetadataTenant> getEntry() {
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
            reload();
        }
    }
}
