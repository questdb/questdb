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

package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.lang.cst.impl.qry.SplitRecord;
import com.nfsdb.lang.cst.impl.qry.SplitRecordMetadata;

public class NestedLoopLeftOuterJoin extends AbstractImmutableIterator<SplitRecord> implements RecordSource<SplitRecord> {
    private final RecordSource<? extends Record> masterSource;
    private final RecordSource<? extends Record> slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private boolean nextSlave = false;

    public NestedLoopLeftOuterJoin(RecordSource<? extends Record> masterSource, RecordSource<? extends Record> slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.metadata = new SplitRecordMetadata(masterSource.getMetadata(), slaveSource.getMetadata());
        this.record = new SplitRecord(metadata, masterSource.getMetadata().getColumnCount());
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
