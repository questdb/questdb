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

package com.nfsdb.ql.ops;

import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.ql.AggregatorFunction;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EI_EXPOSE_REP"})
public abstract class AbstractSingleColumnAggregatorFunction implements AggregatorFunction {
    private final ColumnMetadata[] meta = new ColumnMetadata[1];
    protected int recordIndex;
    protected int valueIndex;

    public AbstractSingleColumnAggregatorFunction(ColumnMetadata meta) {
        this.meta[0] = meta;
    }

    @Override
    public ColumnMetadata[] getColumns() {
        return meta;
    }

    @Override
    public void mapColumn(int k, int i) {
        valueIndex = i;
    }

    @Override
    public void prepareSource(RecordSource<? extends Record> source) {
        this.recordIndex = source.getMetadata().getColumnIndex(meta[0].name);
    }
}
