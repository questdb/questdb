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
import io.questdb.cairo.TableToken;
import org.jetbrains.annotations.Nullable;

public class TableMetadataPool extends AbstractMultiTenantPool<TableReaderMetadataTenantImpl> {

    public TableMetadataPool(CairoConfiguration configuration) {
        super(configuration, configuration.getMetadataPoolCapacity(), configuration.getInactiveReaderTTL());
    }

    @Override
    protected byte getListenerSrc() {
        return PoolListener.SRC_TABLE_METADATA;
    }

    @Override
    protected TableReaderMetadataTenantImpl newTenant(
            TableToken tableToken,
            Entry<TableReaderMetadataTenantImpl> rootEntry,
            Entry<TableReaderMetadataTenantImpl> entry,
            int index,
            @Nullable ResourcePoolSupervisor<TableReaderMetadataTenantImpl> supervisor
    ) {
        return new TableReaderMetadataTenantImpl(this, rootEntry, entry, index, tableToken, false);
    }
}
