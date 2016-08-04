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

import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Misc;
import com.questdb.ql.*;
import com.questdb.ql.impl.SplitRecordMetadata;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;

public class CrossJoinRecordSource extends AbstractCombinedRecordSource {
    private final RecordSource masterSource;
    private final RecordSource slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private final SplitRecordStorageFacade storageFacade;
    private JournalReaderFactory factory;
    private RecordCursor masterCursor;
    private RecordCursor slaveCursor;
    private boolean nextSlave = false;

    public CrossJoinRecordSource(RecordSource masterSource, RecordSource slaveSource) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.metadata = new SplitRecordMetadata(masterSource.getMetadata(), slaveSource.getMetadata());
        int split = masterSource.getMetadata().getColumnCount();
        this.record = new SplitRecord(split);
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
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
        nextSlave = false;
        this.factory = factory;
        masterCursor = masterSource.prepareCursor(factory, cancellationHandler);
        slaveCursor = slaveSource.prepareCursor(factory, cancellationHandler);
        this.storageFacade.prepare(factory, masterCursor.getStorageFacade(), slaveCursor.getStorageFacade());
        return this;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public boolean hasNext() {
        return nextSlave || masterCursor.hasNext();
    }

    @Override
    public Record next() {
        if (!nextSlave) {
            record.setA(masterCursor.next());
            slaveCursor = slaveSource.prepareCursor(factory);
        }

        if (nextSlave || slaveCursor.hasNext()) {
            record.setB(slaveCursor.next());
            nextSlave = slaveCursor.hasNext();
        } else {
            record.setB(null);
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
