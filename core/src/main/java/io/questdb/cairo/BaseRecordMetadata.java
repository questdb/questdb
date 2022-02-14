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
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;

public abstract class BaseRecordMetadata implements RecordMetadata {
    protected ObjList<TableColumnMetadata> columnMetadata;
    protected LowerCaseCharSequenceIntHashMap columnNameIndexMap;
    protected int timestampIndex;
    protected int columnCount;

    public static TableColumnMetadata copyOf(RecordMetadata metadata, int columnIndex) {
        if (metadata instanceof BaseRecordMetadata) {
            return ((BaseRecordMetadata) metadata).getColumnQuick(columnIndex);
        }
        return new TableColumnMetadata(
                metadata.getColumnName(columnIndex),
                metadata.getColumnHash(columnIndex),
                metadata.getColumnType(columnIndex),
                metadata.isColumnIndexed(columnIndex),
                metadata.getIndexValueBlockCapacity(columnIndex),
                metadata.isSymbolTableStatic(columnIndex),
                metadata.getMetadata(columnIndex)
        );
    }

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnType(int columnIndex) {
        return getColumnQuick(columnIndex).getType();
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        final int index = columnNameIndexMap.keyIndex(columnName, lo, hi);
        if (index < 0) {
            return columnNameIndexMap.valueAt(index);
        }
        return -1;
    }

    @Override
    public long getColumnHash(int columnIndex) {
        return getColumnQuick(columnIndex).getHash();
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getColumnQuick(columnIndex).getName();
    }

    @Override
    public int getIndexValueBlockCapacity(int columnIndex) {
        return getColumnQuick(columnIndex).getIndexValueBlockCapacity();
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex;
    }

    @Override
    public RecordMetadata getMetadata(int columnIndex) {
        return getColumnQuick(columnIndex).getMetadata();
    }

    public int getWriterIndex(int columnIndex) {
        return getColumnQuick(columnIndex).getWriterIndex();
    }

    @Override
    public boolean isColumnIndexed(int columnIndex) {
        return getColumnQuick(columnIndex).isIndexed();
    }

    @Override
    public boolean isSymbolTableStatic(int columnIndex) {
        return columnMetadata.getQuick(columnIndex).isSymbolTableStatic();
    }

    public TableColumnMetadata getColumnQuick(int index) {
        return columnMetadata.getQuick(index);
    }

}
