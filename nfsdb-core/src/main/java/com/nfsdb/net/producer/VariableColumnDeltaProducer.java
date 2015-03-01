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

package com.nfsdb.net.producer;

import com.nfsdb.storage.VariableColumn;

import java.util.List;

public class VariableColumnDeltaProducer extends ChannelProducerGroup<ColumnDeltaProducer> implements ColumnDeltaProducer {

    public VariableColumnDeltaProducer(VariableColumn column) {
        addProducer(new FixedColumnDeltaProducer(column));
        addProducer(new FixedColumnDeltaProducer(column.getIndexColumn()));
    }

    public void configure(long localRowID, long limit) {
        List<ColumnDeltaProducer> producers = getProducers();
        for (int i = 0, sz = producers.size(); i < sz; i++) {
            producers.get(i).configure(localRowID, limit);
        }
        computeHasContent();
    }
}
