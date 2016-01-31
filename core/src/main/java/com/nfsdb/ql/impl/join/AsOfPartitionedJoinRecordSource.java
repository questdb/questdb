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

package com.nfsdb.ql.impl.join;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Misc;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.join.asof.*;
import com.nfsdb.std.AbstractImmutableIterator;
import com.nfsdb.std.CharSequenceHashSet;

import java.io.Closeable;
import java.io.IOException;

public class AsOfPartitionedJoinRecordSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, RecordCursor<Record>, Closeable {
    private final LastRecordMap map;
    private final RecordHolder holder;
    private final RecordSource<? extends Record> master;
    private final RecordSource<? extends Record> slave;
    private final SplitRecordMetadata metadata;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private final SplitRecord record;
    private final SplitRecordStorageFacade storageFacade;
    private RecordCursor<? extends Record> masterCursor;
    private RecordCursor<? extends Record> slaveCursor;
    private boolean closed = false;

    public AsOfPartitionedJoinRecordSource(
            RecordSource<? extends Record> master,
            int masterTimestampIndex,
            RecordSource<? extends Record> slave,
            int slaveTimestampIndex,
            CharSequenceHashSet masterKeyColumns,
            CharSequenceHashSet slaveKeyColumns,
            int pageSize
    ) {
        this.master = master;
        this.masterTimestampIndex = masterTimestampIndex;
        this.slave = slave;
        this.slaveTimestampIndex = slaveTimestampIndex;
        if (slave.supportsRowIdAccess()) {
            map = new LastRowIdRecordMap(master.getMetadata(), slave.getMetadata(), masterKeyColumns, slaveKeyColumns);
            holder = new RowidRecordHolder();
        } else {
            // check if slave has variable length columns
            boolean var = false;
            OUT:
            for (int i = 0, n = slave.getMetadata().getColumnCount(); i < n; i++) {
                switch (slave.getMetadata().getColumnQuick(i).getType()) {
                    case BINARY:
                        throw new JournalRuntimeException("Binary columns are not supported");
                    case STRING:
                        if (!masterKeyColumns.contains(slave.getMetadata().getColumnName(i))) {
                            var = true;
                        }
                        break OUT;
                }
            }
            if (var) {
                this.map = new LastVarRecordMap(master.getMetadata(), slave.getMetadata(), masterKeyColumns, slaveKeyColumns, pageSize);
                this.holder = new VarRecordHolder(slave.getMetadata());
            } else {
                this.map = new LastFixRecordMap(master.getMetadata(), slave.getMetadata(), masterKeyColumns, slaveKeyColumns, pageSize);
                this.holder = new FixRecordHolder(slave.getMetadata());
            }
        }
//        map.getMetadata().setAlias(slave.getMetadata().getAlias());
        this.metadata = new SplitRecordMetadata(master.getMetadata(), map.getMetadata());
        this.record = new SplitRecord(this.metadata, master.getMetadata().getColumnCount());
        this.storageFacade = new SplitRecordStorageFacade(this.metadata, master.getMetadata().getColumnCount());
    }

    @Override
    public void close() throws IOException {
        assert !closed;
        Misc.free(map);
        Misc.free(holder);
        Misc.free(master);
        Misc.free(slave);
        closed = true;
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
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
        holder.setCursor(slaveCursor);
        storageFacade.prepare(factory, masterCursor.getStorageFacade(), map.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        this.master.reset();
        this.slave.reset();
        this.map.reset();
        this.holder.clear();
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

        Record delayed = holder.peek();
        if (delayed != null) {
            if (ts > delayed.getDate(slaveTimestampIndex)) {
                map.put(delayed);
                holder.clear();
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
                holder.write(slave);
                record.setB(map.get(master));
                return record;
            }
        }
        record.setB(map.get(master));
        return record;
    }
}
