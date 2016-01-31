/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.ql.impl;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.AbstractRecordMetadata;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.misc.Chars;
import com.nfsdb.std.ObjHashSet;
import com.nfsdb.std.ObjList;

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

    public ObjHashSet<String> getColumnNames() {
        return columnNames;
    }
}
