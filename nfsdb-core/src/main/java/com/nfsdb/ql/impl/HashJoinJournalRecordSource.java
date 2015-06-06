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
import com.nfsdb.collections.IntList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.collections.mmap.MultiMap;
import com.nfsdb.collections.mmap.MultiRecordMap;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.RecordColumnMetadata;
import com.nfsdb.ql.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;

import static com.nfsdb.ql.impl.KeyWriterHelper.setKey;

public class HashJoinJournalRecordSource extends AbstractImmutableIterator<Record> implements RecordSource<Record>, Closeable, RecordCursor<Record> {
    private final RecordSource<? extends Record> masterSource;
    private final JournalRecordSource<? extends Record> slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord currentRecord;
    private final ObjList<RecordColumnMetadata> masterColumns = new ObjList<>();
    private final ObjList<RecordColumnMetadata> slaveColumns = new ObjList<>();
    private final IntList masterColIndex = new IntList();
    private final IntList slaveColIndex = new IntList();
    private final RowIdHolderRecord rowIdRecord = new RowIdHolderRecord();
    private RandomAccessRecordCursor<? extends Record> slaveCursor;
    private RecordCursor<? extends Record> masterCursor;
    private MultiRecordMap hashTable;
    private RecordCursor<? extends Record> hashTableCursor;

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
    public HashJoinJournalRecordSource(
            RecordSource<? extends Record> masterSource,
            ObjList<String> masterColumns,
            JournalRecordSource<? extends Record> slaveSource,
            ObjList<String> slaveColumns) {
        this.masterSource = masterSource;
        this.slaveSource = slaveSource;
        this.metadata = new SplitRecordMetadata(masterSource.getMetadata(), slaveSource.getMetadata());
        this.currentRecord = new SplitRecord(metadata, masterSource.getMetadata().getColumnCount());
        this.hashTable = buildHashTable(masterSource, masterColumns, slaveSource, slaveColumns);
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
    public RecordCursor<Record> prepareCursor(JournalReaderFactory factory) throws JournalException {
        this.slaveCursor = slaveSource.prepareCursor(factory);
        this.masterCursor = masterSource.prepareCursor(factory);
        buildHashTable();
        return this;
    }

    @Override
    public void reset() {
        hashTableCursor = null;
        masterSource.reset();
        hashTable.clear();
    }

    @Override
    public SymFacade getSymFacade() {
        return null;
    }

    @Override
    public boolean hasNext() {
        if (hashTableCursor != null && hashTableCursor.hasNext()) {
            currentRecord.setB(slaveCursor.getByRowId(hashTableCursor.next().getLong(0)));
            return true;
        }
        return hasNext0();
    }

    @SuppressFBWarnings({"IT_NO_SUCH_ELEMENT"})
    @Override
    public SplitRecord next() {
        return currentRecord;
    }

    private MultiRecordMap buildHashTable(RecordSource<? extends Record> masterSource,
                                          ObjList<String> masterColumns,
                                          RecordSource<? extends Record> slaveSource,
                                          ObjList<String> slaveColumns) {
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
        builder.setRecordMetadata(rowIdRecord.getMetadata());
        return builder.build();
    }

    private void buildHashTable() {
        for (Record r : slaveCursor) {
            MultiMap.KeyWriter key = hashTable.claimKey();
            for (int i = 0, k = slaveColumns.size(); i < k; i++) {
                setKey(key, r, slaveColumns.getQuick(i).getType(), slaveColIndex.getQuick(i));
            }
            hashTable.add(key, rowIdRecord.init(r.getRowId()));
        }
    }

    private boolean hasNext0() {
        while (masterCursor.hasNext()) {

            Record r = masterCursor.next();
            currentRecord.setA(r);

            MultiMap.KeyWriter key = hashTable.claimKey();

            for (int i = 0, k = masterColumns.size(); i < k; i++) {
                setKey(key, r, masterColumns.getQuick(i).getType(), masterColIndex.getQuick(i));
            }

            hashTableCursor = hashTable.get(key);

            if (hashTableCursor.hasNext()) {
                currentRecord.setB(slaveCursor.getByRowId(hashTableCursor.next().getLong(0)));
                return true;
            }
        }
        return false;
    }
}
