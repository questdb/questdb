/*
 * Copyright (c) 2014-2015. NFSdb.
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

package com.nfsdb.ql.impl;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.collections.DirectIntList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.mmap.MultiMap;
import com.nfsdb.collections.mmap.MultiRecordMap;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.RandomAccessRecordSource;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.RecordSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.nfsdb.ql.impl.KeyWriterHelper.setKey;

public class HashJoinRecordSource extends AbstractImmutableIterator<SplitRecord> implements RecordSource<SplitRecord> {
    private final RecordSource<? extends Record> masterSource;
    private final RandomAccessRecordSource<? extends Record> slaveSource;
    private final SplitRecordMetadata metadata;
    private final MultiRecordMap hashTable;
    private final SplitRecord currentRecord;
    private final ObjList<RecordColumnMetadata> masterColumns = new ObjList<>();
    private final ObjList<RecordColumnMetadata> slaveColumns = new ObjList<>();
    private final DirectIntList masterColIndex = new DirectIntList();
    private final DirectIntList slaveColIndex = new DirectIntList();
    private final RowIdHolderRecord rowIdRecord = new RowIdHolderRecord();
    private RecordSource<? extends Record> hashTableSource;
    private boolean hashTablePending = true;

    public HashJoinRecordSource(RecordSource<? extends Record> masterSource, ObjList<String> masterColumns, RandomAccessRecordSource<? extends Record> slaveSource, ObjList<String> slaveColumns) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.metadata = new SplitRecordMetadata(masterSource.getMetadata(), slaveSource.getMetadata());
        this.currentRecord = new SplitRecord(metadata, masterSource.getMetadata().getColumnCount());

        RecordMetadata mm = masterSource.getMetadata();
        for (int i = 0, k = masterColumns.size(); i < k; i++) {
            int index = mm.getColumnIndex(masterColumns.get(i));
            this.masterColIndex.add(index);
            this.masterColumns.add(mm.getColumn(index));
        }

        MultiRecordMap.Builder builder = new MultiRecordMap.Builder();
        RecordMetadata sm = slaveSource.getMetadata();
        for (int i = 0, k = slaveColumns.size(); i < k; i++) {
            int index = sm.getColumnIndex(slaveColumns.get(i));
            this.slaveColIndex.add(index);
            this.slaveColumns.add(sm.getColumn(index));
            builder.keyColumn(sm.getColumn(index));
        }
        builder.setRecordMetadata(rowIdRecord.getMetadata());
        this.hashTable = builder.build();
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public boolean hasNext() {
        if (hashTableSource != null && hashTableSource.hasNext()) {
            currentRecord.setB(slaveSource.getByRowId(hashTableSource.next().getLong(0)));
            return true;
        }
        return hasNext0();
    }

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public SplitRecord next() {
        return currentRecord;
    }

    @Override
    public void reset() {
        hashTableSource = null;
        masterSource.reset();
    }

    private void buildHashTable() {
        for (Record r : slaveSource) {
            MultiMap.KeyWriter key = hashTable.claimKey();
            for (int i = 0, k = slaveColumns.size(); i < k; i++) {
                setKey(key, r, slaveColumns.get(i).getType(), i);
            }
            hashTable.add(key, rowIdRecord.init(r.getRowId()));
        }
    }

    private boolean hasNext0() {

        if (hashTablePending) {
            buildHashTable();
            hashTablePending = false;
        }

        while (masterSource.hasNext()) {

            Record r = masterSource.next();
            currentRecord.setA(r);

            MultiMap.KeyWriter key = hashTable.claimKey();

            for (int i = 0, k = masterColumns.size(); i < k; i++) {
                setKey(key, r, masterColumns.get(i).getType(), masterColIndex.get(i));
            }

            hashTableSource = hashTable.get(key);

            if (hashTableSource.hasNext()) {
                currentRecord.setB(slaveSource.getByRowId(hashTableSource.next().getLong(0)));
                return true;
            }
        }
        return false;
    }
}
