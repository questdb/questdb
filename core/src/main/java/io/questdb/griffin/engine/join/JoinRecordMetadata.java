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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.*;
import io.questdb.cairo.map.FastMap;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

import java.io.Closeable;

public class JoinRecordMetadata extends BaseRecordMetadata implements Closeable {

    private final static ColumnTypes keyTypes;
    private final static ColumnTypes valueTypes;
    private final StringSink sink = new StringSink();
    private final Map map;
    private int refCount;

    public JoinRecordMetadata(CairoConfiguration configuration, int columnCount) {
        this.map = new FastMap(configuration.getSqlJoinMetadataPageSize(), keyTypes, valueTypes, columnCount * 2, 0.6, configuration.getSqlJoinMetadataMaxResizes());
        this.timestampIndex = -1;
        this.columnCount = 0;
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap(columnCount);
        this.columnMetadata = new ObjList<>(columnCount);
        this.refCount = 1;
    }

    public void add(
            CharSequence tableAlias,
            CharSequence columnName,
            long columnHash,
            int columnType,
            boolean indexFlag,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            RecordMetadata metadata
    ) {
        int dot = addAlias(tableAlias, columnName);
        final CharSink b = Misc.getThreadLocalBuilder();
        TableColumnMetadata cm;
        if (dot == -1) {
            cm = new TableColumnMetadata(
                    b.put(tableAlias).put('.').put(columnName).toString(),
                    columnHash,
                    columnType,
                    indexFlag,
                    indexValueBlockCapacity,
                    symbolTableStatic,
                    metadata
            );
        } else {
            cm = new TableColumnMetadata(
                    Chars.toString(columnName),
                    columnHash,
                    columnType,
                    indexFlag,
                    indexValueBlockCapacity,
                    symbolTableStatic,
                    metadata
            );
        }
        addToMap(columnName, dot, cm);
    }

    public void add(CharSequence tableAlias, TableColumnMetadata m) {
        final CharSequence columnName = m.getName();
        final int dot = addAlias(tableAlias, columnName);
        final CharSink b = Misc.getThreadLocalBuilder();
        TableColumnMetadata cm;
        if (dot == -1) {
            cm = new TableColumnMetadata(
                    b.put(tableAlias).put('.').put(columnName).toString(),
                    m.getHash(),
                    m.getType(),
                    m.isIndexed(),
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
        if (fromMetadata instanceof BaseRecordMetadata) {
            for (int i = 0, n = fromMetadata.getColumnCount(); i < n; i++) {
                add(alias, ((BaseRecordMetadata) fromMetadata).getColumnQuick(i));
            }
        } else {
            for (int i = 0, n = fromMetadata.getColumnCount(); i < n; i++) {
                add(
                        alias,
                        fromMetadata.getColumnName(i),
                        fromMetadata.getColumnHash(i),
                        fromMetadata.getColumnType(i),
                        fromMetadata.isColumnIndexed(i),
                        fromMetadata.getIndexValueBlockCapacity(i),
                        fromMetadata.isSymbolTableStatic(i),
                        GenericRecordMetadata.copyOf(fromMetadata.getMetadata(i))
                );
            }
        }
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        final MapKey key = map.withKey();
        final int dot = Chars.indexOf(columnName, lo, '.');
        if (dot == -1) {
            key.putStrLowerCase(null);
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
        int dot = Chars.indexOf(columnName, '.');
        assert dot != -1 || tableAlias != null;

        // add column with its own alias
        MapKey key = map.withKey();

        if (dot == -1) {
            key.putStrLowerCase(tableAlias);
        } else {
            assert tableAlias == null;
            key.putStrLowerCase(columnName, 0, dot);
        }
        key.putStrLowerCase(columnName, dot + 1, columnName.length());

        MapValue value = key.createValue();
        if (!value.isNew()) {
            throw CairoException.duplicateColumn(columnName, "(tableAlias=" + tableAlias + ")");
        }

        value.putLong(0, columnCount++);
        return dot;
    }

    private void addToMap(CharSequence columnName, int dot, TableColumnMetadata cm) {
        MapKey key;
        MapValue value;
        this.columnMetadata.add(cm);

        key = map.withKey();
        key.putStr(null);
        key.putStrLowerCase(columnName, dot + 1, columnName.length());

        value = key.createValue();
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
