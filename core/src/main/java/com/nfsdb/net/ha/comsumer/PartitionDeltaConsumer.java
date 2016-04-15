/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 *
 ******************************************************************************/

package com.nfsdb.net.ha.comsumer;

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.ex.JournalNetworkException;
import com.nfsdb.net.ha.ChannelConsumer;
import com.nfsdb.store.AbstractColumn;
import com.nfsdb.store.VariableColumn;

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
