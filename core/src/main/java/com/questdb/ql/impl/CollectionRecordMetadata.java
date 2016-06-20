/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package com.questdb.ql.impl;

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.AbstractRecordMetadata;
import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.misc.Chars;
import com.questdb.std.ObjHashSet;
import com.questdb.std.ObjList;

public class CollectionRecordMetadata extends AbstractRecordMetadata {
    private final ObjList<RecordColumnMetadata> columns = new ObjList<>();
    private final ObjHashSet<String> columnNames = new ObjHashSet<>();

    public CollectionRecordMetadata add(RecordColumnMetadata meta) {
        if (columnNames.add(meta.getName())) {
            columns.add(meta);
            return this;
        } else {
            throw new JournalRuntimeException("Duplicate column name");
        }
    }

    @Override
    public RecordColumnMetadata getColumn(int index) {
        return columns.get(index);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public int getColumnIndexQuiet(CharSequence name) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (Chars.equals(columns.getQuick(i).getName(), name)) {
                return i;
            }
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
