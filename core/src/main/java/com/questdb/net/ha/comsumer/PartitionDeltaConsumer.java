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

package com.questdb.net.ha.comsumer;

import com.questdb.net.ha.ChannelConsumer;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.AbstractColumn;
import com.questdb.store.Journal;
import com.questdb.store.Partition;
import com.questdb.store.VariableColumn;

public class PartitionDeltaConsumer extends ChannelConsumerGroup {

    private final Partition partition;
    private long oldSize;

    public PartitionDeltaConsumer(Partition partition) {
        super(getColumnConsumers(partition));
        this.partition = partition;
        this.oldSize = partition.size();
    }

    private static ChannelConsumer[] getColumnConsumers(Partition partition) {
        ChannelConsumer[] consumers = new ChannelConsumer[partition.getJournal().getMetadata().getColumnCount()];
        for (int i = 0; i < consumers.length; i++) {
            AbstractColumn column = partition.getAbstractColumn(i);
            if (column instanceof VariableColumn) {
                consumers[i] = new VariableColumnDeltaConsumer((VariableColumn) column);
            } else {
                consumers[i] = new FixedColumnDeltaConsumer(column);
            }
        }
        return consumers;
    }

    @Override
    protected void commit() throws JournalNetworkException {
        super.commit();
        partition.commitColumns();
        partition.applyTx(Journal.TX_LIMIT_EVAL, null);
        long size = partition.size();
        partition.updateIndexes(oldSize, size);
        oldSize = size;
    }
}
