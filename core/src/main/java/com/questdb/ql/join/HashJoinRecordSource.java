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
import com.questdb.ql.join.hash.FakeRecord;
import com.questdb.ql.join.hash.MultiRecordMap;
import com.questdb.ql.map.MapUtils;
import com.questdb.ql.map.MetadataTypeResolver;
import com.questdb.ql.map.RecordKeyCopier;
import com.questdb.ql.map.RecordKeyCopierCompiler;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.IntList;
import com.questdb.std.Misc;
import com.questdb.std.str.CharSink;
import com.questdb.store.Record;
import com.questdb.store.RecordCursor;
import com.questdb.store.RecordMetadata;
import com.questdb.store.StorageFacade;
import com.questdb.store.factory.ReaderFactory;

import java.io.Closeable;

public class HashJoinRecordSource extends AbstractCombinedRecordSource implements Closeable {
    private static final MetadataTypeResolver.MetadataTypeResolverThreadLocal tlMetadataResolver = new MetadataTypeResolver.MetadataTypeResolverThreadLocal();
    private final RecordSource master;
    private final RecordSource slave;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private final SplitRecordStorageFacade storageFacade;
    private final IntList masterColIndex;
    private final IntList slaveColIndex;
    private final FakeRecord fakeRecord = new FakeRecord();
    private final boolean byRowId;
    private final boolean outer;
    private final MultiRecordMap recordMap;
    private final NullableRecord nullableRecord;
    private final RecordKeyCopier masterCopier;
    private final RecordKeyCopier slaveCopier;
    private RecordCursor slaveCursor;
    private RecordCursor masterCursor;
    private RecordCursor hashTableCursor;

    public HashJoinRecordSource(
            RecordSource master,
            IntList masterColIndices,
            RecordSource slave,
            IntList slaveColIndices,
            boolean outer,
            int keyPageSize,
            int dataPageSize,
            int rowIdPageSize,
            RecordKeyCopierCompiler compiler
    ) {
        this.master = master;
        this.slave = slave;
        final RecordMetadata mrm = master.getMetadata();
        final RecordMetadata srm = slave.getMetadata();
        this.metadata = new SplitRecordMetadata(mrm, srm);
        this.byRowId = slave.supportsRowIdAccess();
        this.masterColIndex = masterColIndices;
        this.slaveColIndex = slaveColIndices;
        this.recordMap = byRowId ? new MultiRecordMap(tlMetadataResolver.get().of(srm, slaveColIndices), MapUtils.ROWID_RECORD_METADATA, keyPageSize, rowIdPageSize) :
                new MultiRecordMap(tlMetadataResolver.get().of(srm, slaveColIndices), srm, keyPageSize, dataPageSize);
        this.nullableRecord = new NullableRecord(byRowId ? slave.getRecord() : recordMap.getRecord());
        this.record = new SplitRecord(mrm.getColumnCount(), srm.getColumnCount(), master.getRecord(), nullableRecord);
        this.outer = outer;
        this.storageFacade = new SplitRecordStorageFacade(mrm.getColumnCount());
        this.masterCopier = compiler.compile(mrm, masterColIndices);
        this.slaveCopier = compiler.compile(srm, slaveColIndices);
    }

    @Override
    public void close() {
        Misc.free(recordMap);
        Misc.free(master);
        Misc.free(slave);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(ReaderFactory factory, CancellationHandler cancellationHandler) {
        this.hashTableCursor = null;
        this.recordMap.clear();
        this.slaveCursor = slave.prepareCursor(factory, cancellationHandler);
        this.masterCursor = master.prepareCursor(factory, cancellationHandler);
        buildHashTable(cancellationHandler);
        recordMap.setStorageFacade(slaveCursor.getStorageFacade());
        storageFacade.prepare(masterCursor.getStorageFacade(), slaveCursor.getStorageFacade());
        return this;
    }

    @Override
    public Record getRecord() {
        return record;
    }

    @Override
    public Record newRecord() {
        return new SplitRecord(master.getMetadata().getColumnCount(), slave.getMetadata().getColumnCount(), master.getRecord(), slave.getRecord());
    }

    @Override
    public StorageFacade getStorageFacade() {
        return storageFacade;
    }

    @Override
    public void releaseCursor() {
        this.recordMap.clear();
        this.slaveCursor.releaseCursor();
        this.masterCursor.releaseCursor();
    }

    @Override
    public void toTop() {
        this.slaveCursor.toTop();
        this.masterCursor.toTop();
        this.hashTableCursor = null;
    }

    @Override
    public boolean hasNext() {
        if (hashTableCursor != null && hashTableCursor.hasNext()) {
            advanceSlaveCursor();
            return true;
        }
        return hasNext0();
    }

    @Override
    public SplitRecord next() {
        return record;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("HashJoinRecordSource").put(',');
        sink.putQuoted("master").put(':').put(master).put(',');
        sink.putQuoted("slave").put(':').put(slave).put(',');
        sink.putQuoted("joinOn").put(':').put('[');
        sink.put('[');
        RecordMetadata mm = master.getMetadata();
        for (int i = 0, n = masterColIndex.size(); i < n; i++) {
            if (i > 0) {
                sink.put(',');
            }
            sink.putQuoted(mm.getColumnQuick(masterColIndex.getQuick(i)).getName());
        }
        sink.put(']').put(',');
        sink.put('[');
        RecordMetadata sm = slave.getMetadata();
        for (int i = 0, n = slaveColIndex.size(); i < n; i++) {
            if (i > 0) {
                sink.put(',');
            }
            sink.putQuoted(sm.getColumnQuick(slaveColIndex.getQuick(i)).getName());
        }
        sink.put("]]}");
    }

    private void advanceSlaveCursor() {
        Record rec = hashTableCursor.next();
        nullableRecord.set_null((byRowId ? slaveCursor.recordAt(rec.getLong(0)) : rec) == null);
    }

    private void buildHashTable(CancellationHandler cancellationHandler) {
        for (Record r : slaveCursor) {
            cancellationHandler.check();
            recordMap.locate(slaveCopier, r);
            if (byRowId) {
                recordMap.add(fakeRecord.of(r.getRowId()));
            } else {
                recordMap.add(r);
            }
        }
    }

    private boolean hasNext0() {
        while (masterCursor.hasNext()) {
            Record r = masterCursor.next();
            recordMap.locate(masterCopier, r);
            hashTableCursor = recordMap.get();
            if (hashTableCursor.hasNext()) {
                advanceSlaveCursor();
                return true;
            } else if (outer) {
                hashTableCursor = null;
                nullableRecord.set_null(true);
                return true;
            }
        }
        return false;
    }
}
