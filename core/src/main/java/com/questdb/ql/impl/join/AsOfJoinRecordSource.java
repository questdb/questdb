/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.impl.join;

import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.SplitRecordMetadata;
import com.questdb.ql.impl.join.asof.FixRecordHolder;
import com.questdb.ql.impl.join.asof.RecordHolder;
import com.questdb.ql.impl.join.asof.RowidRecordHolder;
import com.questdb.ql.impl.join.asof.VarRecordHolder;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;

import java.io.Closeable;

public class AsOfJoinRecordSource extends AbstractCombinedRecordSource implements Closeable {
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
        this.record = new SplitRecord(master.getMetadata().getColumnCount());

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
        this.storageFacade = new SplitRecordStorageFacade(master.getMetadata().getColumnCount());
    }

    @Override
    public void close() {
        Misc.free(recordHolder);
        Misc.free(delayedHolder);
        Misc.free(master);
        Misc.free(slave);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) throws JournalException {
        this.masterCursor = master.prepareCursor(factory, cancellationHandler);
        this.slaveCursor = slave.prepareCursor(factory, cancellationHandler);
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
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public Record newRecord() {
        return null;
    }

    @Override
    public Record recordAt(long rowId) {
        return null;
    }

    @Override
    public void recordAt(Record record, long atRowId) {
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

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("AsOfJoinRecordSource").put(',');
        sink.putQuoted("master").put(':').put(master).put(',');
        sink.putQuoted("slave").put(':').put(slave).put(',');
        sink.putQuoted("masterTsIndex").put(':').put(masterTimestampIndex).put(',');
        sink.putQuoted("slaveTsIndex").put(':').put(slaveTimestampIndex);
        sink.put('}');
    }
}
