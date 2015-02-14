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
import com.nfsdb.column.ColumnType;
import com.nfsdb.column.SymbolTable;
import com.nfsdb.lang.cst.impl.qry.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Closeable;
import java.io.IOException;

public class HashJoin extends AbstractImmutableIterator<SplitRecord> implements RecordSource<SplitRecord>, Closeable {
    private final RecordSource<? extends Record> hashedSource;
    private final RecordSource<? extends Record> slaveSource;
    private final SplitRecordMetadata metadata;
    private final SplitRecord record;
    private final JoinHashTable masterSymbolTable;

    public HashJoin(RecordSource<? extends Record> hashedSource, String hashedSourceColumn, RecordSource<? extends Record> slaveSource, String slaveColumn) {
        this.hashedSource = hashedSource;
        this.slaveSource = slaveSource;
        this.metadata = new SplitRecordMetadata(this.hashedSource.getMetadata(), slaveSource.getMetadata());
        this.record = new SplitRecord(metadata, this.hashedSource.getMetadata().getColumnCount());
        this.masterSymbolTable = buildColumnHash(this.hashedSource, hashedSourceColumn);
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
        throw new NotImplementedException();
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public void reset() {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public SplitRecord next() {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
