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
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.Misc;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;

public class CrossJoinRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource masterSource;
    private final RecordSource slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private final SplitRecordStorageFacade storageFacade;
    private final NullableRecord nullableRecord;
    private final int split;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private boolean nextSlave = false;

    public CrossJoinRecordSource(RecordSource masterSource, RecordSource slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.metadata = new SplitRecordMetadata(masterSource.getMetadata(), slaveSource.getMetadata());
        this.split = masterSource.getMetadata().getColumnCount();
        this.nullableRecord = new NullableRecord(slaveSource.getRecord());
        this.record = new SplitRecord(split, slaveSource.getMetadata().getColumnCount(), masterSource.getRecord(), nullableRecord);
        this.storageFacade = new SplitRecordStorageFacade(split);
    }

    @Override
    public void close() {
        Misc.free(masterSource);
        Misc.free(slaveSource);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        nextSlave = false;
        masterCursor = masterSource.prepareCursor(factory, cancellationHandler);
        slaveCursor = slaveSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(masterCursor.getStorageFacade(), slaveCursor.getStorageFacade());
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new SplitRecord(split, slaveSource.getMetadata().getColumnCount(), masterSource.getRecord(), nullableRecord);
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void releaseCursor() {
        this.masterCursor.releaseCursor();
        this.slaveCursor.releaseCursor();
    }

    @Override
    public void toTop() {
        nextSlave = false;
        masterCursor.toTop();
        slaveCursor.toTop();
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterCursor.hasNext();
    }

    @Override
    public Record next() {
        if (!nextSlave) {
            masterCursor.next();
            slaveCursor.toTop();
        }

        if (nextSlave || slaveCursor.hasNext()) {
            nullableRecord.set_null(slaveCursor.next() == null);
            nextSlave = slaveCursor.hasNext();
        } else {
            nullableRecord.set_null(true);
            nextSlave = false;
        }
        return record;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("CrossJoinRecordSource").put(',');
        sink.putQuoted("master").put(':').put(masterSource).put(',');
        sink.putQuoted("slave").put(':').put(slaveSource);
        sink.put('}');
    }
}
