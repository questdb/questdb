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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractRecordMetadata;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.str.Utf16Sink;

import java.io.Closeable;

public class JoinRecordMetadata extends AbstractRecordMetadata implements Closeable {

    private static final ColumnTypes keyTypes;
    private static final ColumnTypes valueTypes;
    private final Map map;
    private int refCount;

    public JoinRecordMetadata(CairoConfiguration configuration, int columnCount) {
        this.map = new OrderedMap(
                configuration.getSqlJoinMetadataPageSize(),
                keyTypes,
                valueTypes,
                columnCount * 2,
                0.6,
                configuration.getSqlJoinMetadataMaxResizes(),
                MemoryTag.NATIVE_JOIN_MAP
        );
        this.timestampIndex = -1;
        this.columnCount = 0;
        this.refCount = 1;
    }

    public void add(
            CharSequence tableAlias,
            CharSequence columnName,
            int columnType,
            byte indexType,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            RecordMetadata metadata
    ) {
        int dot = addAlias(tableAlias, columnName);
        final Utf16Sink b = Misc.getThreadLocalSink();
        TableColumnMetadata cm;
        if (dot == -1) {
            cm = new TableColumnMetadata(
                    b.put(tableAlias).put('.').put(columnName).toString(),
                    columnType,
                    indexType,
                    indexValueBlockCapacity,
                    symbolTableStatic,
                    metadata
            );
        } else {
            cm = new TableColumnMetadata(
                    Chars.toString(columnName),
                    columnType,
                    indexType,
                    indexValueBlockCapacity,
                    symbolTableStatic,
                    metadata
            );
        }
        addToMap(columnName, dot, cm);
    }

    public void add(CharSequence tableAlias, TableColumnMetadata m) {
        final CharSequence columnName = m.getColumnName();
        final int dot = addAlias(tableAlias, columnName);
        final Utf16Sink b = Misc.getThreadLocalSink();
        TableColumnMetadata cm;
        if (dot == -1 && tableAlias != null) {
            cm = new TableColumnMetadata(
                    b.put(tableAlias).put('.').put(columnName).toString(),
                    m.getColumnType(),
                    m.getIndexType(),
                    m.getIndexValueBlockCapacity(),
                    m.isSymbolTableStatic(),
                    m.getMetadata()
            );
        } else {
            cm = m;
        }
        addToMap(columnName, dot, cm);
    }

    @Override
    public void close() {
        if (--refCount < 1) {
            map.close();
        }
    }

    public void copyColumnMetadataFrom(CharSequence alias, RecordMetadata fromMetadata) {
        for (int i = 0, n = fromMetadata.getColumnCount(); i < n; i++) {
            add(alias, fromMetadata.getColumnMetadata(i));
        }
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        final MapKey key = map.withKey();
        final int dot = Chars.indexOfLastUnquoted(columnName, '.', lo, hi);
        if (dot == -1) {
            key.putStr(null);
            key.putStrLowerCase(columnName, lo, hi);
        } else {
            key.putStrLowerCase(columnName, 0, dot);
            key.putStrLowerCase(columnName, dot + 1, columnName.length());
        }
        MapValue value = key.findValue();
        if (value != null) {
            return value.getInt(0);
        }
        return -1;
    }

    public void incrementRefCount() {
        refCount++;
    }

    public void setTimestampIndex(int index) {
        this.timestampIndex = index;
    }

    private int addAlias(CharSequence tableAlias, CharSequence columnName) {
        int dot = Chars.indexOfLastUnquoted(columnName, '.');
        // add column with its own alias
        MapKey key = map.withKey();

        if (dot == -1) {
            if (tableAlias != null) {
                key.putStrLowerCase(tableAlias);
            }
        } else {
            assert tableAlias == null;
            key.putStrLowerCase(columnName, 0, dot);
        }
        key.putStrLowerCase(columnName, dot + 1, columnName.length());

        MapValue value = key.createValue();
        if (!value.isNew()) {
            throw CairoException.duplicateColumn(columnName, tableAlias);
        }

        value.putInt(0, columnCount++);
        return dot;
    }

    private void addToMap(CharSequence columnName, int dot, TableColumnMetadata cm) {
        columnMetadata.add(cm);

        final MapKey key = map.withKey();
        key.putStr(null);
        key.putStrLowerCase(columnName, dot + 1, columnName.length());

        final MapValue value = key.createValue();
        if (value.isNew()) {
            value.putInt(0, columnCount - 1);
        } else {
            // this is a duplicate columns, if somebody looks it up without alias
            // we would treat this lookup as if column hadn't been found.
            value.putInt(0, -1);
        }
    }

    static {
        final ArrayColumnTypes kt = new ArrayColumnTypes();
        kt.add(ColumnType.STRING);
        kt.add(ColumnType.STRING);
        keyTypes = kt;
        valueTypes = new SingleColumnType(ColumnType.INT);
    }
}
