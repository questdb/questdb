/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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
    private final RowidHolderRecord rowidRecord = new RowidHolderRecord();
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
        builder.setRecordMetadata(rowidRecord.getMetadata());
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

    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT"})
    private void buildHashTable() {
        for (Record r : slaveSource) {
            MultiMap.KeyWriter key = hashTable.claimKey();
            for (int i = 0, k = slaveColumns.size(); i < k; i++) {
                switch (slaveColumns.get(i).getType()) {
                    case BOOLEAN:
                        key.putBoolean(r.getBool(slaveColIndex.get(i)));
                        break;
                    case BYTE:
                        key.putByte(r.get(slaveColIndex.get(i)));
                        break;
                    case DOUBLE:
                        key.putDouble(r.getDouble(slaveColIndex.get(i)));
                        break;
                    case INT:
                        key.putInt(r.getInt(slaveColIndex.get(i)));
                        break;
                    case LONG:
                        key.putLong(r.getLong(slaveColIndex.get(i)));
                        break;
                    case SHORT:
                        key.putShort(r.getShort(slaveColIndex.get(i)));
                        break;
                    case STRING:
                        key.putStr(r.getFlyweightStr(slaveColIndex.get(i)));
                        break;
                    case SYMBOL:
                        key.putStr(r.getSym(slaveColIndex.get(i)));
                        break;
                    case BINARY:
                        key.putBin(r.getBin(slaveColIndex.get(i)));
                        break;
                    case DATE:
                        key.putLong(r.getDate(slaveColIndex.get(i)));
                        break;
                }
            }
            hashTable.add(key, rowidRecord.init(r.getRowId()));
        }
    }

    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT"})
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
                switch (masterColumns.get(i).getType()) {
                    case BOOLEAN:
                        key.putBoolean(r.getBool(masterColIndex.get(i)));
                        break;
                    case BYTE:
                        key.putByte(r.get(masterColIndex.get(i)));
                        break;
                    case DOUBLE:
                        key.putDouble(r.getDouble(masterColIndex.get(i)));
                        break;
                    case INT:
                        key.putInt(r.getInt(masterColIndex.get(i)));
                        break;
                    case LONG:
                        key.putLong(r.getLong(masterColIndex.get(i)));
                        break;
                    case SHORT:
                        key.putShort(r.getShort(masterColIndex.get(i)));
                        break;
                    case STRING:
                        key.putStr(r.getFlyweightStr(masterColIndex.get(i)));
                        break;
                    case SYMBOL:
                        key.putStr(r.getSym(masterColIndex.get(i)));
                        break;
                    case BINARY:
                        key.putBin(r.getBin(masterColIndex.get(i)));
                        break;
                    case DATE:
                        key.putLong(r.getDate(masterColIndex.get(i)));
                        break;
                }
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
