/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
import com.nfsdb.ql.impl.join.asof.FixRecordHolder;
import com.nfsdb.ql.impl.join.asof.RecordHolder;
import com.nfsdb.ql.impl.join.asof.RowidRecordHolder;
import com.nfsdb.ql.impl.join.asof.VarRecordHolder;
import com.nfsdb.ql.ops.AbstractRecordSource;

import java.io.Closeable;
import java.io.IOException;

public class AsOfJoinRecordSource extends AbstractRecordSource implements Closeable {
    private final RecordSource master;
    private final RecordSource slave;
    private final SplitRecordMetadata metadata;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private final SplitRecord record;
    private final RecordHolder recordHolder;
    private final RecordHolder delayedHolder;
    private final SplitRecordStorageFacade storageFacade;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;

    public AsOfJoinRecordSource(
            RecordSource master,
            int masterTimestampIndex,
            RecordSource slave,
            int slaveTimestampIndex
    ) {
        this.master = master;
        this.masterTimestampIndex = masterTimestampIndex;
        this.slave = slave;
        this.slaveTimestampIndex = slaveTimestampIndex;
        this.metadata = new SplitRecordMetadata(master.getMetadata(), slave.getMetadata());
        this.record = new SplitRecord(this.metadata, master.getMetadata().getColumnCount());

        if (slave.supportsRowIdAccess()) {
            this.recordHolder = new RowidRecordHolder();
            this.delayedHolder = new RowidRecordHolder();
        } else {
            // check if slave has variable length columns
            boolean var = false;
            OUT:
            for (int i = 0, n = slave.getMetadata().getColumnCount(); i < n; i++) {
                switch (slave.getMetadata().getColumnQuick(i).getType()) {
                    case BINARY:
                        throw new JournalRuntimeException("Binary columns are not supported");
                    case STRING:
                        var = true;
                        break OUT;
                    default:
                        break;
                }
            }
            if (var) {
                this.recordHolder = new VarRecordHolder(slave.getMetadata());
                this.delayedHolder = new VarRecordHolder(slave.getMetadata());
            } else {
                this.recordHolder = new FixRecordHolder(slave.getMetadata());
                this.delayedHolder = new FixRecordHolder(slave.getMetadata());
            }
        }
        this.storageFacade = new SplitRecordStorageFacade(this.metadata, master.getMetadata().getColumnCount());
    }

    @Override
    public void close() throws IOException {
        Misc.free(recordHolder);
        Misc.free(delayedHolder);
        Misc.free(master);
        Misc.free(slave);
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
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.masterCursor = master.prepareCursor(factory);
        this.slaveCursor = slave.prepareCursor(factory);
        this.recordHolder.setCursor(slaveCursor);
        this.delayedHolder.setCursor(slaveCursor);
        this.storageFacade.prepare(factory, masterCursor.getStorageFacade(), slaveCursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        this.master.reset();
        this.slave.reset();
        recordHolder.clear();
        delayedHolder.clear();
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
        Record delayed = delayedHolder.peek();
        if (delayed != null) {
            if (ts > delayed.getDate(slaveTimestampIndex)) {
                recordHolder.write(delayed);
                delayedHolder.clear();
            } else {
                record.setB(null);
                return record;
            }
        }

        while (slaveCursor.hasNext()) {
            Record slave = slaveCursor.next();
            if (ts > slave.getDate(slaveTimestampIndex)) {
                recordHolder.write(slave);
            } else {
                record.setB(recordHolder.peek());
                recordHolder.clear();
                delayedHolder.write(slave);
                return record;
            }
        }
        record.setB(recordHolder.peek());
        return record;
    }
}
