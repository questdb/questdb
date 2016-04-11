/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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
import com.nfsdb.ql.ops.AbstractCombinedRecordSource;
import com.nfsdb.std.CharSequenceHashSet;

import java.io.Closeable;
import java.io.IOException;

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
                    default:
                        break;
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
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
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
