/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.ql.join;

import com.questdb.ql.CancellationHandler;
import com.questdb.ql.NullableRecord;
import com.questdb.ql.RecordSource;
import com.questdb.ql.SplitRecordMetadata;
import com.questdb.ql.join.asof.*;
import com.questdb.ql.map.RecordKeyCopier;
import com.questdb.ql.map.RecordKeyCopierCompiler;
import com.questdb.ql.map.TypeListResolver;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntList;
import com.questdb.std.Misc;
import com.questdb.std.str.CharSink;
import com.questdb.store.*;
import com.questdb.store.factory.ReaderFactory;

import java.io.Closeable;

public class AsOfPartitionedJoinRecordSource extends AbstractCombinedRecordSource implements Closeable {
    private static final TypeListResolver.TypeListResolverThreadLocal tlTypeListResolver = new TypeListResolver.TypeListResolverThreadLocal();
    private final LastRecordMap map;
    private final RecordHolder holder;
    private final RecordSource master;
    private final RecordSource slave;
    private final SplitRecordMetadata metadata;
    private final int masterTimestampIndex;
    private final int slaveTimestampIndex;
    private final SplitRecord record;
    private final SplitRecordStorageFacade storageFacade;
    private final NullableRecord nullableRecord;
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
            int rowIdPageSize,
            RecordKeyCopierCompiler compiler
    ) {
        this.master = master;
        this.masterTimestampIndex = masterTimestampIndex;
        this.slave = slave;
        this.slaveTimestampIndex = slaveTimestampIndex;

        IntList indices = new IntList();
        IntList types = new IntList();

        RecordMetadata m = master.getMetadata();
        for (int i = 0, n = masterKeyColumns.size(); i < n; i++) {
            int index = m.getColumnIndex(masterKeyColumns.get(i));
            indices.add(index);
            types.add(m.getColumnQuick(index).getType());
        }

        RecordKeyCopier masterCopier = compiler.compile(m, indices, true);
        indices.clear();

        m = slave.getMetadata();
        for (int i = 0, n = slaveKeyColumns.size(); i < n; i++) {
            indices.add(m.getColumnIndex(slaveKeyColumns.get(i)));
        }
        RecordKeyCopier slaveCopier = compiler.compile(m, indices, true);

        if (slave.supportsRowIdAccess()) {
            map = new LastRowIdRecordMap(
                    tlTypeListResolver.get().of(types),
                    slave.getMetadata(),
                    masterCopier,
                    slaveCopier,
                    rowIdPageSize,
                    slave.getRecord());
            holder = new RowidRecordHolder();
        } else {
            // check if slave has variable length columns
            boolean var = false;
            OUT:
            for (int i = 0, n = slave.getMetadata().getColumnCount(); i < n; i++) {
                switch (slave.getMetadata().getColumnQuick(i).getType()) {
                    case ColumnType.BINARY:
                        throw new JournalRuntimeException("Binary columns are not supported");
                    case ColumnType.STRING:
                        if (masterKeyColumns.excludes(slave.getMetadata().getColumnName(i))) {
                            var = true;
                        }
                        break OUT;
                    default:
                        break;
                }
            }
            if (var) {
                this.map = new LastVarRecordMap(
                        tlTypeListResolver.get().of(types),
                        slave.getMetadata(),
                        masterCopier,
                        slaveCopier,
                        dataPageSize, offsetPageSize);
                this.holder = new VarRecordHolder(slave.getMetadata());
            } else {
                this.map = new LastFixRecordMap(
                        tlTypeListResolver.get().of(types),
                        slave.getMetadata(),
                        masterCopier,
                        slaveCopier,
                        dataPageSize,
                        offsetPageSize
                );
                this.holder = new FixRecordHolder(slave.getMetadata());
            }
        }
        this.metadata = new SplitRecordMetadata(master.getMetadata(), map.getMetadata());
        this.nullableRecord = new NullableRecord(map.getRecord());
        this.record = new SplitRecord(
                master.getMetadata().getColumnCount(),
                map.getMetadata().getColumnCount(),
                master.getRecord(),
                nullableRecord);
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
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        this.map.reset();
        this.holder.clear();
        this.masterCursor = master.prepareCursor(factory, cancellationHandler);
        this.slaveCursor = slave.prepareCursor(factory, cancellationHandler);
        map.setSlaveCursor(slaveCursor);
        holder.setCursor(slaveCursor);
        storageFacade.prepare(masterCursor.getStorageFacade(), map.getStorageFacade());
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new SplitRecord(master.getMetadata().getColumnCount(), map.getMetadata().getColumnCount(), master.getRecord(), nullableRecord);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void releaseCursor() {
        this.map.reset();
        this.holder.clear();
        this.masterCursor.releaseCursor();
        this.slaveCursor.releaseCursor();
    }

    @Override
    public void toTop() {
        this.map.reset();
        this.holder.clear();
        this.masterCursor.toTop();
        this.slaveCursor.toTop();
    }

    @Override
    public boolean hasNext() {
        return masterCursor.hasNext();
    }

    @Override
    public Record next() {
        Record master = masterCursor.next();
        long ts = master.getDate(masterTimestampIndex);

        Record delayed = holder.peek();
        if (delayed != null) {
            if (ts > delayed.getDate(slaveTimestampIndex)) {
                map.put(delayed);
                holder.clear();
            } else {
                nullableRecord.set_null(true);
                return record;
            }
        }

        while (slaveCursor.hasNext()) {
            Record slave = slaveCursor.next();
            if (ts > slave.getDate(slaveTimestampIndex)) {
                map.put(slave);
            } else {
                holder.write(slave);
                nullableRecord.set_null(map.get(master) == null);
                return record;
            }
        }
        nullableRecord.set_null(map.get(master) == null);
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
