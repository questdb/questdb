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
import io.questdb.cairo.wal.SequencerMetadataWriterBackend;

public class SequencerMetadataUpdater implements SequencerMetadataWriterBackend {
    private final SequencerMetadata metadata;
    private final CharSequence tableName;

    public SequencerMetadataUpdater(SequencerMetadata metadata, CharSequence tableName) {
        this.metadata = metadata;
        this.tableName = tableName;
    }

    @Override
    public void addColumn(CharSequence name, int type, int symbolCapacity, boolean symbolCacheFlag, boolean isIndexed, int indexValueBlockCapacity, boolean isSequential) {
        metadata.addColumn(name, type);
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public void removeColumn(CharSequence columnName) {
        metadata.removeColumn(columnName);
    }

    public void renameColumn(CharSequence columnName, CharSequence newName) {
        metadata.renameColumn(columnName, newName);
    }
}
