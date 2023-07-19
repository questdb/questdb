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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.*;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StringSink;

import static io.questdb.std.Chars.utf8ToUtf16;

class TableStructureAdapter implements TableStructure {
    private static final String DEFAULT_TIMESTAMP_FIELD = "timestamp";
    private static final ThreadLocal<StringSink> tempSink = new ThreadLocal<>(StringSink::new);
    private final CairoConfiguration cairoConfiguration;
    private final DefaultColumnTypes defaultColumnTypes;
    private final int defaultPartitionBy;
    private final ObjList<LineTcpParser.ProtoEntity> entities = new ObjList<>();
    private final LowerCaseCharSequenceHashSet entityNamesUtf16 = new LowerCaseCharSequenceHashSet();
    private CharSequence tableName;
    private int timestampIndex = -1;

    public TableStructureAdapter(CairoConfiguration configuration, DefaultColumnTypes defaultColumnTypes, int defaultPartitionBy) {
        this.cairoConfiguration = configuration;
        this.defaultColumnTypes = defaultColumnTypes;
        this.defaultPartitionBy = defaultPartitionBy;
    }

    @Override
    public int getColumnCount() {
        final int size = entities.size();
        return timestampIndex == -1 ? size + 1 : size;
    }

    @Override
    public CharSequence getColumnName(int columnIndex) {
        assert columnIndex < getColumnCount();
        if (columnIndex == getTimestampIndex()) {
            return DEFAULT_TIMESTAMP_FIELD;
        }
        CharSequence colName = entities.get(columnIndex).getName().toString();
        if (TableUtils.isValidColumnName(colName, cairoConfiguration.getMaxFileNameLength())) {
            return colName;
        }
        throw CairoException.nonCritical().put("column name contains invalid characters [colName=").put(colName).put(']');
    }

    @Override
    public int getColumnType(int columnIndex) {
        if (columnIndex == getTimestampIndex()) {
            return ColumnType.TIMESTAMP;
        }
        return defaultColumnTypes.DEFAULT_COLUMN_TYPES[entities.get(columnIndex).getType()];
    }

    @Override
    public int getIndexBlockCapacity(int columnIndex) {
        return 0;
    }

    @Override
    public int getMaxUncommittedRows() {
        return cairoConfiguration.getMaxUncommittedRows();
    }

    @Override
    public long getO3MaxLag() {
        return cairoConfiguration.getO3MaxLag();
    }

    @Override
    public int getPartitionBy() {
        return defaultPartitionBy;
    }

    @Override
    public boolean getSymbolCacheFlag(int columnIndex) {
        return cairoConfiguration.getDefaultSymbolCacheFlag();
    }

    @Override
    public int getSymbolCapacity(int columnIndex) {
        return cairoConfiguration.getDefaultSymbolCapacity();
    }

    @Override
    public CharSequence getTableName() {
        return tableName;
    }

    @Override
    public int getTimestampIndex() {
        return timestampIndex == -1 ? entities.size() : timestampIndex;
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
    public boolean isSequential(int columnIndex) {
        return false;
    }

    @Override
    public boolean isWalEnabled() {
        return cairoConfiguration.getWalEnabledDefault() && PartitionBy.isPartitioned(getPartitionBy());
    }

    TableStructureAdapter of(CharSequence tableName, LineTcpParser parser) {
        this.tableName = tableName;
        entityNamesUtf16.clear();
        entities.clear();
        timestampIndex = -1;
        final boolean hasNonAsciiChars = parser.hasNonAsciiChars();
        for (int i = 0; i < parser.getEntityCount(); i++) {
            final LineTcpParser.ProtoEntity entity = parser.getEntity(i);
            final DirectByteCharSequence colNameUtf8 = entity.getName();
            final CharSequence colNameUtf16 = utf8ToUtf16(colNameUtf8, tempSink.get(), hasNonAsciiChars);
            int index = entityNamesUtf16.keyIndex(colNameUtf16);
            if (index > -1) {
                entityNamesUtf16.addAt(index, colNameUtf16.toString());
                if (Chars.equals(colNameUtf16, DEFAULT_TIMESTAMP_FIELD)) {
                    timestampIndex = entities.size();
                }
                entities.add(entity);
            }
        }
        return this;
    }
}
