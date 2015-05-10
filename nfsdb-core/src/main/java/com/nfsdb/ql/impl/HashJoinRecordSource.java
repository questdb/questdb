/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.RandomAccessRecordSource;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordMetadata;
import com.nfsdb.ql.RecordSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

import static com.nfsdb.ql.impl.KeyWriterHelper.setKey;

public class HashJoinRecordSource extends AbstractImmutableIterator<SplitRecord> implements RecordSource<SplitRecord>, Closeable {
    private final RecordSource<? extends Record> masterSource;
    private final RecordSource<? extends Record> slaveSource;
    private final HashJoinStoreStrategyContext hashStrategyContext;
    private final SplitRecordMetadata metadata;
    private final SplitRecord currentRecord;
    private final ObjList<RecordColumnMetadata> masterColumns = new ObjList<>();
    private final ObjList<RecordColumnMetadata> slaveColumns = new ObjList<>();
    private final DirectIntList masterColIndex = new DirectIntList();
    private final DirectIntList slaveColIndex = new DirectIntList();
    private MultiRecordMap hashTable;
    private RecordSource<? extends Record> hashTableSource;
    private boolean hashTablePending = true;

    private HashJoinRecordSource(RecordSource<? extends Record> masterSource, ObjList<String> masterColumns,
                                    RecordSource<? extends Record> slaveSource, ObjList<String> slaveColumns, HashJoinStoreStrategyContext hashStrategyContext) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.hashStrategyContext = hashStrategyContext;
        this.metadata = new SplitRecordMetadata(masterSource.getMetadata(), slaveSource.getMetadata());
        this.currentRecord = new SplitRecord(metadata, masterSource.getMetadata().getColumnCount());
        this.hashTable = buildHashTable(masterSource, masterColumns, slaveSource, slaveColumns);
    }

    public static HashJoinRecordSource fromRandomAccessSource(RecordSource<? extends Record> masterSource, ObjList<String> masterColumns,
                                                              RandomAccessRecordSource<? extends Record> slaveSource, ObjList<String> slaveColumns) {
        return new HashJoinRecordSource(masterSource, masterColumns, slaveSource, slaveColumns, new HashJoinRowIdStoreStrategyContext(slaveSource));
    }

    public static HashJoinRecordSource fromSource(RecordSource<? extends Record> masterSource, ObjList<String> masterColumns,
                                                              RecordSource<? extends Record> slaveSource, ObjList<String> slaveColumns) {
        return new HashJoinRecordSource(masterSource, masterColumns, slaveSource, slaveColumns, new HashJoinRecordStoreStrategyContext(slaveSource));
    }

    @Override
    public void close() throws IOException {
        if (hashTable != null) {
            hashTable.close();
            hashTable = null;
        }
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void reset() {
        hashTableSource = null;
        masterSource.reset();
    }

    @Override
    public boolean hasNext() {
        if (hashTableSource != null && hashTableSource.hasNext()) {
            currentRecord.setB(hashStrategyContext.getNextSlave(hashTableSource.next()));
            return true;
        }
        return hasNext0();
    }

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public SplitRecord next() {
        return currentRecord;
    }

    private MultiRecordMap buildHashTable(RecordSource<? extends Record> masterSource, ObjList<String> masterColumns, RecordSource<? extends Record> slaveSource, ObjList<String> slaveColumns) {
        RecordMetadata mm = masterSource.getMetadata();
        for (int i = 0, k = masterColumns.size(); i < k; i++) {
            int index = mm.getColumnIndex(masterColumns.getQuick(i));
            this.masterColIndex.add(index);
            this.masterColumns.add(mm.getColumn(index));
        }

        MultiRecordMap.Builder builder = new MultiRecordMap.Builder();
        RecordMetadata sm = slaveSource.getMetadata();
        for (int i = 0, k = slaveColumns.size(); i < k; i++) {
            int index = sm.getColumnIndex(slaveColumns.getQuick(i));
            this.slaveColIndex.add(index);
            this.slaveColumns.add(sm.getColumn(index));
            builder.keyColumn(sm.getColumn(index));
        }
        builder.setRecordMetadata(hashStrategyContext.getHashRecordMetadata());
        return builder.build();
    }

    private void buildHashTable() {
        for (Record r : slaveSource) {
            MultiMap.KeyWriter key = hashTable.claimKey();
            for (int i = 0, k = slaveColumns.size(); i < k; i++) {
                setKey(key, r, slaveColumns.getQuick(i).getType(), slaveColIndex.get(i));
            }
            hashTable.add(key, hashStrategyContext.getHashTableRecord(r));
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
                setKey(key, r, masterColumns.getQuick(i).getType(), masterColIndex.get(i));
            }

            hashTableSource = hashTable.get(key);

            if (hashTableSource.hasNext()) {
                currentRecord.setB(hashStrategyContext.getNextSlave(hashTableSource.next()));
                return true;
            }
        }
        return false;
    }
}
