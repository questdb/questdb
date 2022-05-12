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

public class GenericRecordMetadata extends BaseRecordMetadata {
    public static final GenericRecordMetadata EMPTY = new GenericRecordMetadata();
    private final LowerCaseCharSequenceIntHashMap columnNameIndexMap;

    public GenericRecordMetadata() {
        this.columnMetadata = new ObjList<>();
        this.columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
        this.timestampIndex = -1;
    }

    public static void copyColumns(RecordMetadata from, GenericRecordMetadata to) {
        if (from instanceof BaseRecordMetadata) {
            final BaseRecordMetadata gm = (BaseRecordMetadata) from;
            for (int i = 0, n = gm.getColumnCount(); i < n; i++) {
                to.add(gm.getColumnQuick(i));
            }
        } else {
            for (int i = 0, n = from.getColumnCount(); i < n; i++) {
                to.add(new TableColumnMetadata(
                        from.getColumnName(i),
                        from.getColumnHash(i),
                        from.getColumnType(i),
                        from.isColumnIndexed(i),
                        from.getIndexValueBlockCapacity(i),
                        from.isSymbolTableStatic(i),
                        GenericRecordMetadata.copyOf(from.getMetadata(i))
                ));
            }
        }
    }

    public static GenericRecordMetadata copyOf(RecordMetadata that) {
        if (that != null) {
            if (that instanceof GenericRecordMetadata) {
                return (GenericRecordMetadata) that;
            }
            GenericRecordMetadata metadata = copyOfSansTimestamp(that);
            metadata.setTimestampIndex(that.getTimestampIndex());
            return metadata;
        }
        return null;
    }

    public static GenericRecordMetadata copyOfSansTimestamp(RecordMetadata that) {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        copyColumns(that, metadata);
        return metadata;
    }

    public static RecordMetadata removeTimestamp(RecordMetadata that) {
        if (that.getTimestampIndex() != -1) {
            if (that instanceof GenericRecordMetadata) {
                ((GenericRecordMetadata) that).setTimestampIndex(-1);
                return that;
            }
            return GenericRecordMetadata.copyOfSansTimestamp(that);
        }
        return that;
    }

    public GenericRecordMetadata add(TableColumnMetadata meta) {
        return add(columnCount, meta);
    }

    public GenericRecordMetadata add(int i, TableColumnMetadata meta) {
        int index = columnNameIndexMap.keyIndex(meta.getName());
        if (index > -1) {
            columnNameIndexMap.putAt(index, meta.getName(), i);
            columnMetadata.extendAndSet(i, meta);
            columnCount++;
            return this;
        } else {
            throw CairoException.duplicateColumn(meta.getName());
        }
    }

    public void clear() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        final int index = columnNameIndexMap.keyIndex(columnName, lo, hi);
        if (index < 0) {
            return columnNameIndexMap.valueAt(index);
        }
        return -1;
    }

    public void setTimestampIndex(int index) {
        this.timestampIndex = index;
    }
}
