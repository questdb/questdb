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

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.CharSequenceHashSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.collections.LastFixRecordMap;
import com.nfsdb.ql.collections.LastRecordMap;
import com.nfsdb.ql.collections.LastRowIdRecordMap;
import com.nfsdb.ql.collections.LastVarRecordMap;

import java.io.Closeable;

public class AsOfPartitionedJoinRecordSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, RecordCursor<Record>, Closeable {
    private final LastRecordMap map;
    private final RecordSource<? extends Record> master;
    private final RecordSource<? extends Record> slave;
    private final SplitRecordMetadata metadata;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private final SplitRecord record;
    private RecordCursor<? extends Record> masterCursor;
    private RecordCursor<? extends Record> slaveCursor;
    private Record delayedSlave;

    // todo: extract config
    public AsOfPartitionedJoinRecordSource(
            RecordSource<? extends Record> master,
            int masterTimestampIndex,
            RecordSource<? extends Record> slave,
            int slaveTimestampIndex,
            CharSequenceHashSet keyColumns,
            int pageSize
    ) {
        this.master = master;
        this.masterTimestampIndex = masterTimestampIndex;
        this.slave = slave;
        this.slaveTimestampIndex = slaveTimestampIndex;
        if (slave.supportsRowIdAccess()) {
            map = new LastRowIdRecordMap(master.getMetadata(), slave.getMetadata(), keyColumns);
        } else {
            // check if slave has variable length columns
            boolean var = false;
            OUT:
            for (int i = 0, n = slave.getMetadata().getColumnCount(); i < n; i++) {
                switch (slave.getMetadata().getColumnQuick(i).getType()) {
                    case BINARY:
                        throw new JournalRuntimeException("Binary columns are not supported");
                    case STRING:
                        if (!keyColumns.contains(slave.getMetadata().getColumnQuick(i).getName())) {
                            var = true;
                        }
                        break OUT;
                }
            }
            if (var) {
                this.map = new LastVarRecordMap(master.getMetadata(), slave.getMetadata(), keyColumns, pageSize);
            } else {
                this.map = new LastFixRecordMap(master.getMetadata(), slave.getMetadata(), keyColumns, pageSize);
            }
        }
        this.metadata = new SplitRecordMetadata(master.getMetadata(), map.getMetadata());
        this.record = new SplitRecord(this.metadata, master.getMetadata().getColumnCount());
    }

    @Override
    public void close() {
        map.close();
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.masterCursor = master.prepareCursor(factory);
        this.slaveCursor = slave.prepareCursor(factory);
        map.setSlaveCursor(slaveCursor);
        return this;
    }

    @Override
    public void reset() {
        this.master.reset();
        this.slave.reset();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @Override
    public boolean hasNext() {
        return masterCursor.hasNext();
    }

    @Override
    public Record next() {
        Record master = masterCursor.next();
        record.setA(master);

        long ts = master.getDate(masterTimestampIndex);

        if (delayedSlave != null) {
            if (ts > delayedSlave.getDate(slaveTimestampIndex)) {
                map.put(delayedSlave);
                delayedSlave = null;
            } else {
                record.setB(null);
                return record;
            }
        }

        while (slaveCursor.hasNext()) {
            Record slave = slaveCursor.next();
            if (ts > slave.getDate(slaveTimestampIndex)) {
                map.put(slave);
            } else {
                record.setB(map.get(master));
                delayedSlave = slave;
                return record;
            }
        }
        record.setB(null);
        return record;
    }
}
