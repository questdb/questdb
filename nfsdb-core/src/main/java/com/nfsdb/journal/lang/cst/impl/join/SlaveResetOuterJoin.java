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

package com.nfsdb.journal.lang.cst.impl.join;

import com.nfsdb.journal.collections.AbstractImmutableIterator;
import com.nfsdb.journal.lang.cst.impl.qry.*;

public class SlaveResetOuterJoin extends AbstractImmutableIterator<Record> implements GenericRecordSource {
    private final RecordSource<? extends Record> masterSource;
    private final RecordSource<? extends Record> slaveSource;
    private final JoinedRecordMetadata metadata;
    private Record joinedData;
    private boolean nextSlave = false;

    public SlaveResetOuterJoin(RecordSource<? extends Record> masterSource, RecordSource<? extends Record> slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.metadata = new JoinedRecordMetadata(masterSource, slaveSource);
    }

    @Override
    public void reset() {
        masterSource.reset();
        slaveSource.reset();
        nextSlave = false;
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
    public Record next() {
        if (!nextSlave) {
            joinedData = masterSource.next();
            slaveSource.reset();
        }

        if (nextSlave || slaveSource.hasNext()) {
            joinedData.setSlave(slaveSource.next());
            nextSlave = slaveSource.hasNext();
        } else {
            joinedData.setSlave(null);
            nextSlave = false;
        }
        return joinedData;
    }
}
