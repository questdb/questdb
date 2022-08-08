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

package io.questdb.cairo;

import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.UpdateOperator;

public class SequencerMetadataWriterBackend implements TableWriterBackend {
    private final SequencerMetadata metadata;
    private final CharSequence tableName;

    public SequencerMetadataWriterBackend(SequencerMetadata metadata, CharSequence tableName) {
        this.metadata = metadata;
        this.tableName = tableName;
    }

    @Override
    public void addColumn(CharSequence name, int type, int symbolCapacity, boolean symbolCacheFlag, boolean isIndexed, int indexValueBlockCapacity, boolean isSequential) {
        metadata.addColumn(name, type);
    }

    @Override
    public void addIndex(CharSequence columnName, int indexValueBlockSize) {
        throw CairoException.instance(0).put("add index does not update sequencer metadata");
    }

    @Override
    public int attachPartition(long partitionTimestamp) {
        throw CairoException.instance(0).put("attach partition does not update sequencer metadata");
    }

    @Override
    public void changeCacheFlag(int columnIndex, boolean isCacheOn) {
        // Do nothing, does not impact sequencer / wal metadata
        throw CairoException.instance(0).put("change cache flag does not update sequencer metadata");
    }

    @Override
    public void dropIndex(CharSequence columnName) {
        // Do nothing, does not impact sequencer / wal metadata
        throw CairoException.instance(0).put("drop index does not update sequencer metadata");
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public int getPartitionBy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public UpdateOperator getUpdateOperator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeColumn(CharSequence columnName) {
        metadata.removeColumn(columnName);
    }

    @Override
    public boolean removePartition(long partitionTimestamp) {
        // Do nothing, does not impact sequencer / wal metadata
        throw CairoException.instance(0).put("remove partition does not update sequencer metadata");
    }

    @Override
    public void renameColumn(CharSequence columnName, CharSequence newName) {
        metadata.renameColumn(columnName, newName);
    }

    @Override
    public void setMetaCommitLag(long commitLag) {
        // Do nothing, does not impact sequencer / wal metadata
        throw CairoException.instance(0).put("change commit lag does not update sequencer metadata");
    }

    @Override
    public void setMetaMaxUncommittedRows(int maxUncommittedRows) {
        // Do nothing, does not impact sequencer / wal metadata
        throw CairoException.instance(0).put("change max uncommitted does not update sequencer metadata");
    }
}
