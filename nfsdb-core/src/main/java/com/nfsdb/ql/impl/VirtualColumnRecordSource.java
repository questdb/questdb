/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.ObjList;
import com.nfsdb.ql.*;
import com.nfsdb.ql.ops.VirtualColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class VirtualColumnRecordSource extends AbstractImmutableIterator<Record> implements GenericRecordSource, RecordSourceState, RecordCursor<Record> {
    private final RecordSource<? extends Record> recordSource;
    private final RecordMetadata metadata;
    private final VirtualRecord current;
    private RecordCursor<? extends Record> recordCursor;


    public VirtualColumnRecordSource(RecordSource<? extends Record> recordSource, ObjList<VirtualColumn> virtualColumns) {
        this.recordSource = recordSource;
        for (int i = 0, k = virtualColumns.size(); i < k; i++) {
            virtualColumns.getQuick(i).configureSource(this);
        }
        RecordMetadata dm = recordSource.getMetadata();
        this.metadata = new VirtualRecordMetadata(dm, virtualColumns);
        this.current = new VirtualRecord(this.metadata, dm.getColumnCount(), virtualColumns);
    }

    @Override
    public Record currentRecord() {
        return current;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor<Record> prepareCursor() {
        this.recordCursor = recordSource.prepareCursor();
        return this;
    }

    @Override
    public void unprepare() {
        recordSource.unprepare();
    }

    @Override
    public boolean hasNext() {
        return recordCursor.hasNext();
    }

    @Override
    public Record next() {
        current.setBase(recordCursor.next());
        return current;
    }
}
