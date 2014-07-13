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

package com.nfsdb.journal.net.comsumer;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.AbstractColumn;
import com.nfsdb.journal.column.VariableColumn;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.net.ChannelConsumer;

public class PartitionDeltaConsumer extends ChannelConsumerGroup {

    private final Partition partition;
    private long oldSize;

    public PartitionDeltaConsumer(Partition partition) {
        super(getColumnConsumers(partition));
        this.partition = partition;
        this.oldSize = partition.size();
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

    private static ChannelConsumer[] getColumnConsumers(Partition partition) {
        ChannelConsumer consumers[] = new ChannelConsumer[partition.getJournal().getMetadata().getColumnCount() + 1];
        consumers[0] = new FixedColumnDeltaConsumer(partition.getNullsColumn());
        for (int i = 1; i < consumers.length; i++) {
            AbstractColumn column = partition.getAbstractColumn(i - 1);
            if (column instanceof VariableColumn) {
                consumers[i] = new VariableColumnDeltaConsumer((VariableColumn) column);
            } else {
                consumers[i] = new FixedColumnDeltaConsumer(column);
            }
        }
        return consumers;
    }
}
