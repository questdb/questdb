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

import io.questdb.std.*;

public class WalWriterMetadataCache extends BaseRecordMetadata {
    private final CairoConfiguration configuration;

    public WalWriterMetadataCache(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.columnMetadata = new ObjList<>();
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    }

    public WalWriterMetadataCache of(TableReader tableReader) {
        reset();

        final TableReaderMetadata metadata = tableReader.getMetadata();
        timestampIndex = metadata.getTimestampIndex();

        for (int i = 0; i < metadata.getColumnCount(); i++) {
            final CharSequence name = metadata.getColumnName(i);
            final int type = metadata.getColumnType(i);
            addColumn(name, type, i);
        }
        return this;
    }

    private void reset() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
    }

    void addColumn(CharSequence columnName, int type, int columnIndex) {
        final String name = columnName.toString();
        columnNameIndexMap.put(name, columnNameIndexMap.size());
        if (ColumnType.isSymbol(type)) {
            columnMetadata.add(new TableColumnMetadata(name, -1L, type,
                    configuration.getDefaultSymbolCacheFlag(), configuration.getDefaultSymbolCapacity(),
                    true, null, columnIndex));
        } else {
            columnMetadata.add(new TableColumnMetadata(name, -1L, type, false, 0, false, null, columnIndex));
        }
        columnCount++;
    }

    void removeColumn(int columnIndex) {
        final TableColumnMetadata deletedMeta = columnMetadata.getQuick(columnIndex);
        deletedMeta.markDeleted();
        columnNameIndexMap.remove(deletedMeta.getName());

        // enumerate columns that would have moved up after column deletion
        for (int i = columnIndex + 1; i < columnCount; i++) {
            TableColumnMetadata columnMeta = columnMetadata.getQuick(i);
            if (columnMeta.getType() > 0) {
                columnNameIndexMap.put(columnMeta.getName(), i);
            }
        }
    }
}
