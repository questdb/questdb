/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.net.ha.producer;

import com.questdb.std.ObjList;
import com.questdb.std.ex.JournalException;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.AbstractColumn;
import com.questdb.store.Partition;
import com.questdb.store.VariableColumn;

import java.nio.channels.WritableByteChannel;

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
        ObjList<ColumnDeltaProducer> producers = getProducers();
        for (int i = 0, sz = producers.size(); i < sz; i++) {
            producers.getQuick(i).configure(localRowID, limit);
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
