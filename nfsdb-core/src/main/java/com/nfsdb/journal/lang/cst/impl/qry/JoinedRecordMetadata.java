/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
 */

package com.nfsdb.journal.lang.cst.impl.qry;

import com.nfsdb.journal.column.ColumnType;

public class JoinedRecordMetadata implements RecordMetadata {
    private final RecordSource<? extends Record> master;
    private final RecordSource<? extends Record> slave;

    public JoinedRecordMetadata(RecordSource<? extends Record> master, RecordSource<? extends Record> slave) {
        this.master = master;
        this.slave = slave;
    }

    @Override
    public RecordMetadata nextMetadata() {
        return slave.getMetadata();
    }

    @Override
    public int getColumnCount() {
        return master.getMetadata().getColumnCount();
    }

    @Override
    public ColumnType getColumnType(int index) {
        return master.getMetadata().getColumnType(index);
    }

    @Override
    public int getColumnIndex(CharSequence name) {
        return master.getMetadata().getColumnIndex(name);
    }
}
