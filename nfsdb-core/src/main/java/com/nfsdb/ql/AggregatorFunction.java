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

package com.nfsdb.ql;

import com.nfsdb.collections.mmap.MapValues;
import com.nfsdb.factory.configuration.ColumnMetadata;

public interface AggregatorFunction {

    void calculate(Record rec, MapValues values);

    /**
     * Columns that aggregation function writes out. Out of {#ColumnMetadata} objects
     * returned significant fields are "name" and "type". All other fields are ignored.
     * <p/>
     * Complex functions, such as average, may need to write three columns, "count", "sum" and
     * possibly "average" itself.
     * <p/>
     * Returned array and its objects are immutable by convention. There is no need to
     * be defensive and copy arrays.
     * <p/>
     * Also it is possible that this method is called multiple times, so expensive
     * operations must be cached by implementation.
     *
     * @return array of required columns
     */
    ColumnMetadata[] getColumns();

    /**
     * When calculating values implementing classes will be sharing columns of {#Record}. Each
     * aggregator columns indexes need to be mapped into space of "record" column indexes. This
     * is done once when aggregator is prepared. Implementing class must maintain column mapping
     * in an int[] array. For example:
     * <p/>
     * <pre>
     *     private int map[] = new int[columnCount];
     *     ...
     *     map[k] = i;
     * </pre>
     *
     * @param k column index in space of aggregator function
     * @param i column index in space of "record"
     */
    void mapColumn(int k, int i);

    /**
     * Callback to give implementation opportunity to resolve column names to their indexes.
     *
     * @param source record source
     */
    void prepareSource(RecordSource<? extends Record> source);
}
