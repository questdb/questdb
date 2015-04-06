/*
 * NFSdb. Copyright (c) 2014-2015.
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
package com.nfsdb.lang.cst.impl.join;

import com.nfsdb.collections.AbstractImmutableIterator;
import com.nfsdb.lang.cst.Record;
import com.nfsdb.lang.cst.RecordMetadata;
import com.nfsdb.lang.cst.RecordSource;
import com.nfsdb.lang.cst.impl.qry.*;
import com.nfsdb.storage.ColumnType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class HashJoin extends AbstractImmutableIterator<SplitRecord> implements RecordSource<SplitRecord> {
    private final RecordSource<? extends Record> masterSource;
    private final SplitRecordMetadata metadata;
    private final JoinHashTable joinHashTable;
    private final SplitRecord currentRecord;
    private final int masterColIndex;
    private final ColumnType masterColType;
    private RecordSource<? extends Record> hashedValues;

    public HashJoin(RecordSource<? extends Record> masterSource, String masterColumn, RecordSource<? extends Record> hashedSource, String hashedSourceColumn) {
        this.masterSource = masterSource;
        this.metadata = new SplitRecordMetadata(masterSource.getMetadata(), hashedSource.getMetadata());
        this.joinHashTable = buildColumnHash(hashedSource, hashedSourceColumn);
        this.currentRecord = new SplitRecord(metadata, masterSource.getMetadata().getColumnCount());
        this.masterColType = masterSource.getMetadata().getColumn(
                this.masterColIndex = masterSource.getMetadata().getColumnIndex(masterColumn)).getType();
    }

    private static JoinHashTable buildColumnHash(RecordSource<? extends Record> source, String expression) {
        // Supports getRowById.
        if (source instanceof RandomAccessRecordSource) {
            return buildRandomAccessColumnHash((RandomAccessRecordSource<? extends Record>) source, expression);
        }

        // Supports sequential read only.
        throw new NotImplementedException();
    }

    private static JoinHashTable buildRandomAccessColumnHash(RandomAccessRecordSource<? extends Record> source, String expression) {
        return new RowIdJoinHashTable(source, expression);
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void reset() {
        hashedValues = null;
        masterSource.reset();
    }

    @Override
    public boolean hasNext() {
        if (hashedValues != null && hashedValues.hasNext()) {
            currentRecord.setB(hashedValues.next());
            return true;
        }

        while (masterSource.hasNext()) {
            Record masterRec = masterSource.next();
            currentRecord.setA(masterRec);
            hashedValues = joinHashTable.getRows(masterRec, masterColIndex, masterColType);

            if (hashedValues.hasNext()) {
                currentRecord.setB(hashedValues.next());
                return true;
            }
        }

        return false;
    }

    @Override
    public SplitRecord next() {
        return currentRecord;
    }
}
