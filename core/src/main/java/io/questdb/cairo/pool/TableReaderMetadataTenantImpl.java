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

import io.questdb.cairo.DynamicTableReaderMetadata;
import io.questdb.cairo.TableToken;

class TableReaderMetadataTenantImpl extends DynamicTableReaderMetadata implements MetadataPoolTenant {
    private final int index;
    private AbstractMultiTenantPool.Entry<MetadataPoolTenant> entry;
    private AbstractMultiTenantPool<MetadataPoolTenant> pool;

    TableReaderMetadataTenantImpl(
            AbstractMultiTenantPool<MetadataPoolTenant> pool,
            AbstractMultiTenantPool.Entry<MetadataPoolTenant> entry,
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

    @Override
    public AbstractMultiTenantPool.Entry<MetadataPoolTenant> getEntry() {
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
