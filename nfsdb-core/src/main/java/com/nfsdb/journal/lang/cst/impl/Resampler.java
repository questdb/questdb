///*
// * Copyright (c) 2014-2015. Vlad Ilyushchenko
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.nfsdb.journal.lang.cst.impl;
//
//import com.nfsdb.journal.collections.AbstractImmutableIterator;
//import com.nfsdb.journal.collections.mmap.MultiMap;
//import com.nfsdb.journal.factory.configuration.ColumnMetadata;
//import com.nfsdb.journal.lang.cst.AggregatorFunction;
//import com.nfsdb.journal.lang.cst.impl.qry.Record;
//import com.nfsdb.journal.lang.cst.impl.qry.GenericRecordSource;
//
//import java.util.Iterator;
//import java.util.List;
//
//public class Resampler extends AbstractImmutableIterator<Record> implements GenericRecordSource {
//
//    private final MultiMap map;
//    private final GenericRecordSource rowSource;
//    private Iterator<MultiMap.Record> mapIterator;
//    private final int tsIndex;
//
//    public Resampler(GenericRecordSource rowSource, List<ColumnMetadata> keyColumns, List<AggregatorFunction> aggregators, int tsIndex) {
//
//        MultiMap.Builder builder = new MultiMap.Builder();
//        // define key columns
//        for (int i = 0, keyColumnsSize = keyColumns.size(); i < keyColumnsSize; i++) {
//            builder.keyColumn(keyColumns.get(i));
//        }
//
//        // take value columns from aggregator function
//        int index = 0;
//        for (int i = 0, sz = aggregators.size(); i < sz; i++) {
//            AggregatorFunction func = aggregators.get(i);
//            ColumnMetadata[] columns = func.getColumns();
//            for (int k = 0, len = columns.length; k < len; k++) {
//                builder.valueColumn(columns[k]);
//                func.mapColumn(k, index++);
//            }
//        }
//
//        this.map = builder.build();
//        this.rowSource = rowSource;
//        this.tsIndex = tsIndex;
//    }
//
//    @Override
//    public void reset() {
//
//    }
//
//    @Override
//    public boolean hasNext() {
//        if (mapIterator != null && mapIterator.hasNext()) {
//            return true;
//        }
//        mapIterator = null;
//        return rowSource.hasNext();
//
//    }
//
//    @Override
//    public Record next() {
//        if (mapIterator == null) {
//            nextBatch();
//        }
//        return mapIterator.next();
//    }
//
//    private void nextBatch() {
//        while (rowSource.hasNext()) {
//            Record record = rowSource.next();
////            row.
////            MultiMap.Key key = map.claimKey();
//
//
//        }
//    }
//}
