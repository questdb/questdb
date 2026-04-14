/*+*****************************************************************************
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

package io.questdb.cairo.lv;

import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableStructure;

/**
 * Adapts a live view's metadata to the {@link TableStructure} interface
 * so that the existing disk file creation infrastructure can write
 * {@code _meta} and {@code _txn} files.
 * <p>
 * Live views use {@code isWalEnabled()=true} and {@code partitionBy=NOT_APPLICABLE}
 * (same as views and mat views) so that the table token gets persisted
 * in the WAL table name registry store and survives restarts.
 */
public class LiveViewTableStructure implements TableStructure {
    private final GenericRecordMetadata metadata;
    private final String viewName;

    public LiveViewTableStructure(String viewName, GenericRecordMetadata metadata) {
        this.viewName = viewName;
        this.metadata = metadata;
    }

    @Override
    public int getColumnCount() {
        return metadata.getColumnCount();
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        return metadata.getColumnName(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) {
        return metadata.getColumnType(columnIndex);
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 0;
    }

    @Override
    public int getMaxUncommittedRows() {
        return 0;
    }

    @Override
    public long getO3MaxLag() {
        return 0;
    }

    @Override
    public int getPartitionBy() {
        return PartitionBy.NOT_APPLICABLE;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return false;
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return 0;
    }

    @Override
    public CharSequence getTableName() {
        return viewName;
    }

    @Override
    public int getTimestampIndex() {
        return metadata.getTimestampIndex();
    }

    @Override
    public boolean isDedupKey(int columnIndex) {
        return false;
    }

    @Override
    public boolean isIndexed(int columnIndex) {
        return false;
    }

    @Override
    public boolean isLiveView() {
        return true;
    }

    @Override
    public boolean isWalEnabled() {
        return true;
    }
}
