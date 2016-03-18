/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ql.impl.join;

import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.factory.configuration.RecordMetadata;
import com.nfsdb.misc.Misc;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordCursor;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.ql.StorageFacade;
import com.nfsdb.ql.impl.join.hash.FakeRecord;
import com.nfsdb.ql.impl.join.hash.MultiRecordMap;
import com.nfsdb.ql.impl.join.hash.NullRecord;
import com.nfsdb.ql.impl.map.MultiMap;
import com.nfsdb.ql.ops.AbstractCombinedRecordSource;
import com.nfsdb.std.IntList;
import com.nfsdb.std.ObjHashSet;
import com.nfsdb.std.ObjList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

import static com.nfsdb.ql.impl.join.hash.KeyWriterHelper.setKey;


public class HashJoinRecordSource extends AbstractCombinedRecordSource implements Closeable {
    private final RecordSource master;
    private final RecordSource slave;
    private final SplitRecordMetadata metadata;
    private final SplitRecord currentRecord;
    private final SplitRecordStorageFacade storageFacade;
    private final ObjList<RecordColumnMetadata> masterColumns = new ObjList<>();
    private final ObjList<RecordColumnMetadata> slaveColumns = new ObjList<>();
    private final IntList masterColIndex;
    private final IntList slaveColIndex;
    private final FakeRecord fakeRecord = new FakeRecord();
    private final boolean byRowId;
    private final boolean outer;
    private final NullRecord nullRecord;
    private final MultiRecordMap recordMap;
    private RecordCursor slaveCursor;
    private RecordCursor masterCursor;
    private RecordCursor hashTableCursor;

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public HashJoinRecordSource(
            RecordSource master,
            IntList masterColIndices,
            RecordSource slave,
            IntList slaveColIndices,
            boolean outer) {
        this.master = master;
        this.slave = slave;
        this.metadata = new SplitRecordMetadata(master.getMetadata(), slave.getMetadata());
        this.currentRecord = new SplitRecord(metadata, master.getMetadata().getColumnCount());
        this.byRowId = slave.supportsRowIdAccess();
        this.masterColIndex = masterColIndices;
        this.slaveColIndex = slaveColIndices;
        this.recordMap = createRecordMap(master, slave);
        this.outer = outer;
        this.nullRecord = new NullRecord(slave.getMetadata());
        this.storageFacade = new SplitRecordStorageFacade(metadata, master.getMetadata().getColumnCount());
    }

    @Override
    public void close() throws IOException {
        Misc.free(recordMap);
        Misc.free(master);
        Misc.free(slave);
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
        this.slaveCursor = slave.prepareCursor(factory);
        this.masterCursor = master.prepareCursor(factory);
        buildHashTable();
        recordMap.setStorageFacade(slaveCursor.getStorageFacade());
        storageFacade.prepare(factory, masterCursor.getStorageFacade(), slaveCursor.getStorageFacade());
        return this;
    }

    @Override
    public void reset() {
        hashTableCursor = null;
        master.reset();
        recordMap.clear();
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @Override
    public boolean hasNext() {
        if (hashTableCursor != null && hashTableCursor.hasNext()) {
            Record rec = hashTableCursor.next();
            currentRecord.setB(byRowId ? slaveCursor.getByRowId(rec.getLong(0)) : rec);
            return true;
        }
        return hasNext0();
    }

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public SplitRecord next() {
        return currentRecord;
    }

    private void buildHashTable() {
        for (Record r : slaveCursor) {
            MultiMap.KeyWriter key = recordMap.claimKey();
            for (int i = 0, k = slaveColumns.size(); i < k; i++) {
                setKey(key, r, slaveColIndex.getQuick(i), slaveColumns.getQuick(i).getType());
            }
            if (byRowId) {
                recordMap.add(key, fakeRecord.of(r.getRowId()));
            } else {
                recordMap.add(key, r);
            }
        }
    }

    private MultiRecordMap createRecordMap(RecordSource masterSource,
                                           RecordSource slaveSource) {
        RecordMetadata mm = masterSource.getMetadata();
        for (int i = 0, k = masterColIndex.size(); i < k; i++) {
            this.masterColumns.add(mm.getColumnQuick(masterColIndex.getQuick(i)));
        }

        RecordMetadata sm = slaveSource.getMetadata();
        ObjHashSet<String> keyCols = new ObjHashSet<>();
        for (int i = 0, k = slaveColIndex.size(); i < k; i++) {
            int index = slaveColIndex.getQuick(i);
            this.slaveColumns.add(sm.getColumnQuick(index));
            keyCols.add(sm.getColumnName(index));
        }
        return byRowId ? new MultiRecordMap(sm, keyCols, fakeRecord.getMetadata()) : new MultiRecordMap(sm, keyCols, slaveSource.getMetadata());
    }

    private boolean hasNext0() {
        while (masterCursor.hasNext()) {
            Record r = masterCursor.next();
            currentRecord.setA(r);

            MultiMap.KeyWriter key = recordMap.claimKey();

            for (int i = 0, k = masterColumns.size(); i < k; i++) {
                setKey(key, r, masterColIndex.getQuick(i), masterColumns.getQuick(i).getType());
            }

            hashTableCursor = recordMap.get(key);

            if (hashTableCursor.hasNext()) {
                if (byRowId) {
                    currentRecord.setB(slaveCursor.getByRowId(hashTableCursor.next().getLong(0)));
                } else {
                    currentRecord.setB(hashTableCursor.next());
                }
                return true;
            } else if (outer) {
                hashTableCursor = null;
                currentRecord.setB(nullRecord);
                return true;
            }
        }
        return false;
    }
}
