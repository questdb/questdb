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

import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.SplitRecordMetadata;
import com.questdb.ql.impl.join.asof.*;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.CharSink;

import java.io.Closeable;

public class AsOfPartitionedJoinRecordSource extends AbstractCombinedRecordSource implements Closeable {
    private final LastRecordMap map;
    private final RecordHolder holder;
    private final RecordSource master;
    private final RecordSource slave;
    private final SplitRecordMetadata metadata;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private final SplitRecord record;
    private final SplitRecordStorageFacade storageFacade;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private boolean closed = false;

    public AsOfPartitionedJoinRecordSource(
            RecordSource master,
            int masterTimestampIndex,
            RecordSource slave,
            int slaveTimestampIndex,
            CharSequenceHashSet masterKeyColumns,
            CharSequenceHashSet slaveKeyColumns,
            int dataPageSize,
            int offsetPageSize,
            int rowIdPageSize
    ) {
        this.master = master;
        this.masterTimestampIndex = masterTimestampIndex;
        this.slave = slave;
        this.slaveTimestampIndex = slaveTimestampIndex;
        if (slave.supportsRowIdAccess()) {
            map = new LastRowIdRecordMap(master.getMetadata(), slave.getMetadata(), masterKeyColumns, slaveKeyColumns, rowIdPageSize);
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
                    default:
                        break;
                }
            }
            if (var) {
                this.map = new LastVarRecordMap(master.getMetadata(), slave.getMetadata(), masterKeyColumns, slaveKeyColumns, dataPageSize, offsetPageSize);
                this.holder = new VarRecordHolder(slave.getMetadata());
            } else {
                this.map = new LastFixRecordMap(master.getMetadata(), slave.getMetadata(), masterKeyColumns, slaveKeyColumns, dataPageSize, offsetPageSize);
                this.holder = new FixRecordHolder(slave.getMetadata());
            }
        }
//        map.getMetadata().setAlias(slave.getMetadata().getAlias());
        this.metadata = new SplitRecordMetadata(master.getMetadata(), map.getMetadata());
        this.record = new SplitRecord(master.getMetadata().getColumnCount());
        this.storageFacade = new SplitRecordStorageFacade(master.getMetadata().getColumnCount());
    }

    @Override
    public void close() {
        assert !closed;
        Misc.free(map);
        Misc.free(holder);
        Misc.free(master);
        Misc.free(slave);
        closed = true;
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
        this.masterCursor = master.prepareCursor(factory, cancellationHandler);
        this.slaveCursor = slave.prepareCursor(factory, cancellationHandler);
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
    public StorageFacade getStorageFacade() {
        return storageFacade;
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

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("AsOfPartitionedJoinRecordSource").put(',');
        sink.putQuoted("master").put(':').put(master).put(',');
        sink.putQuoted("slave").put(':').put(slave).put(',');
        sink.putQuoted("masterTsIndex").put(':').put(masterTimestampIndex).put(',');
        sink.putQuoted("slaveTsIndex").put(':').put(slaveTimestampIndex);
        sink.put('}');
    }
}
