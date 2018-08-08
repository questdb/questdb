/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql;

import com.questdb.std.CharSequenceIntHashMap;
import com.questdb.std.Chars;
import com.questdb.std.ObjList;
import com.questdb.store.AbstractRecordMetadata;
import com.questdb.store.JournalRuntimeException;
import com.questdb.store.RecordColumnMetadata;
import com.questdb.store.factory.configuration.ColumnName;

public class CollectionRecordMetadata extends AbstractRecordMetadata {
    private final ObjList<RecordColumnMetadata> columns = new ObjList<>();
    private final CharSequenceIntHashMap nameIndexLookup = new CharSequenceIntHashMap();

    public CollectionRecordMetadata add(RecordColumnMetadata meta) {
        if (nameIndexLookup.put(meta.getName(), columns.size())) {
            columns.add(meta);
            return this;
        } else {
            throw new JournalRuntimeException("Duplicate column name");
        }
    }

    public void clear() {
        columns.clear();
        nameIndexLookup.clear();
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {

        int index = nameIndexLookup.get(name);
        if (index > -1) {
            return index;
        }

        if (getAlias() == null) {
            return -1;
        }

        ColumnName columnName = ColumnName.singleton(name);

        if (columnName.alias().length() == 0) {
            return -1;
        }

        if (Chars.equals(columnName.alias(), getAlias())) {
            return nameIndexLookup.get(columnName.name());
        }
        return -1;
    }

    @Override
    public RecordColumnMetadata getColumnQuick(int index) {
        return columns.getQuick(index);
    }

    @Override
    public int getTimestampIndex() {
        return -1;
    }
}
