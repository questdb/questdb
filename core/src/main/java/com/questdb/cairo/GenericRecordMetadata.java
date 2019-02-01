/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.ObjList;

public class GenericRecordMetadata extends BaseRecordMetadata {
    public GenericRecordMetadata() {
        this.columnMetadata = new ObjList<>();
        this.columnNameIndexMap = new CharSequenceIntHashMap();
        this.timestampIndex = -1;
    }

    public static void copyColumns(RecordMetadata from, GenericRecordMetadata to) {
        for (int i = 0, n = from.getColumnCount(); i < n; i++) {
            to.add(new TableColumnMetadata(
                    from.getColumnName(i).toString(),
                    from.getColumnType(i),
                    from.isColumnIndexed(i),
                    from.getIndexValueBlockCapacity(i)
            ));
        }
    }

    public static GenericRecordMetadata copyOf(RecordMetadata that) {
        GenericRecordMetadata metadata = copyOfSansTimestamp(that);
        metadata.setTimestampIndex(that.getTimestampIndex());
        return metadata;
    }

    public static GenericRecordMetadata copyOfSansTimestamp(RecordMetadata that) {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        copyColumns(that, metadata);
        return metadata;
    }

    public GenericRecordMetadata add(TableColumnMetadata meta) {
        int index = columnNameIndexMap.keyIndex(meta.getName());
        if (index > -1) {
            columnNameIndexMap.putAt(index, meta.getName(), columnCount);
            columnMetadata.add(meta);
            columnCount++;
            return this;
        } else {
            throw CairoException.instance(0).put("Duplicate column [name=").put(meta.getName()).put(']');
        }
    }

    public void clear() {
        columnMetadata.clear();
        columnNameIndexMap.clear();
        columnCount = 0;
        timestampIndex = -1;
    }

    public void setTimestampIndex(int index) {
        this.timestampIndex = index;
    }
}
