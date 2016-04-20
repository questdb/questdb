/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 ******************************************************************************/

package com.questdb.net.ha.producer;

import com.questdb.Partition;
import com.questdb.ex.JournalException;
import com.questdb.ex.JournalNetworkException;
import com.questdb.std.ObjList;
import com.questdb.store.AbstractColumn;
import com.questdb.store.VariableColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.channels.WritableByteChannel;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
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
