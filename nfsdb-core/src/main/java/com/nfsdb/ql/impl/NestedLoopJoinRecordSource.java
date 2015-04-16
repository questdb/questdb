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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.RecordSource;

public class NestedLoopJoinRecordSource extends AbstractImmutableIterator<SplitRecord> implements RecordSource<SplitRecord> {
    private final RecordSource<? extends Record> masterSource;
    private final RecordSource<? extends Record> slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private boolean nextSlave = false;

    public NestedLoopJoinRecordSource(RecordSource<? extends Record> masterSource, RecordSource<? extends Record> slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;

        RecordMetadata mm = masterSource.getMetadata();
        this.metadata = new SplitRecordMetadata(mm, slaveSource.getMetadata());
        this.record = new SplitRecord(metadata, mm.getColumnCount());
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterSource.hasNext();
    }

    @Override
    public SplitRecord next() {
        if (!nextSlave) {
            record.setA(masterSource.next());
            slaveSource.reset();
        }

        if (nextSlave || slaveSource.hasNext()) {
            record.setB(slaveSource.next());
            nextSlave = slaveSource.hasNext();
        } else {
            record.setB(null);
            nextSlave = false;
        }
        return record;
    }

    @Override
    public void reset() {
        masterSource.reset();
        slaveSource.reset();
        nextSlave = false;
    }
}
