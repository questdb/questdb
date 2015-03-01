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

import com.nfsdb.Partition;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.storage.AbstractColumn;
import com.nfsdb.storage.VariableColumn;

import java.nio.channels.WritableByteChannel;
import java.util.List;

public class PartitionDeltaProducer extends ChannelProducerGroup<ColumnDeltaProducer> {

    private final Partition partition;

    public PartitionDeltaProducer(Partition partition) {
        this.partition = partition;
        for (int i = 0, c = partition.getJournal().getMetadata().getColumnCount(); i < c; i++) {
            AbstractColumn col = partition.getAbstractColumn(i);
            addProducer(col instanceof VariableColumn ? new VariableColumnDeltaProducer((VariableColumn) col) : new FixedColumnDeltaProducer(col));
        }
    }

    public void configure(long localRowID) throws JournalException {
        partition.open();
        long limit = partition.size();
        List<ColumnDeltaProducer> producers = getProducers();
        for (int i = 0, sz = producers.size(); i < sz; i++) {
            producers.get(i).configure(localRowID, limit);
        }
        computeHasContent();
    }

    public Partition getPartition() {
        return partition;
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        super.write(channel);
        // long running sync operation may make partition look like it hasn't been
        // accessed for a while. We need to make sure partition remains open after
        // being delivered to client
        try {
            partition.open();
        } catch (JournalException e) {
            throw new JournalNetworkException(e);
        }
    }
}
