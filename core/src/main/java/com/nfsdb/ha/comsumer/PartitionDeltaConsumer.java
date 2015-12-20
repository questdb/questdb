/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.ha.comsumer;

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.ChannelConsumer;
import com.nfsdb.storage.AbstractColumn;
import com.nfsdb.storage.VariableColumn;

public class PartitionDeltaConsumer extends ChannelConsumerGroup {

    private final Partition partition;
    private long oldSize;

    public PartitionDeltaConsumer(Partition partition) {
        super(getColumnConsumers(partition));
        this.partition = partition;
        this.oldSize = partition.size();
    }

    private static ChannelConsumer[] getColumnConsumers(Partition partition) {
        ChannelConsumer consumers[] = new ChannelConsumer[partition.getJournal().getMetadata().getColumnCount()];
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
