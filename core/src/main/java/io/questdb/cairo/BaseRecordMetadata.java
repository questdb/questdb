/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.CharSequenceIntHashMap;
import io.questdb.std.ObjList;

public abstract class BaseRecordMetadata implements RecordMetadata {
    protected ObjList<TableColumnMetadata> columnMetadata;
    protected CharSequenceIntHashMap columnNameIndexMap;
    protected int timestampIndex;
    protected int columnCount;

    @Override
    public int getColumnCount() {
        return columnCount;
    }

    @Override
    public int getColumnType(int columnIndex) {
        return getColumnQuick(columnIndex).getType();
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName) {
        return columnNameIndexMap.get(columnName);
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
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
    public boolean isColumnIndexed(int columnIndex) {
        return getColumnQuick(columnIndex).isIndexed();
    }

    public TableColumnMetadata getColumnQuick(int index) {
        return columnMetadata.getQuick(index);
    }

    @Override
    public boolean isSymbolTableStatic(int columnIndex) {
        return columnMetadata.getQuick(columnIndex).isSymbolTableStatic();
    }
}
