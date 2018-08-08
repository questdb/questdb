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

import com.questdb.ex.IncompatibleJournalException;
import com.questdb.net.ha.AbstractChannelConsumer;
import com.questdb.net.ha.model.JournalServerState;
import com.questdb.std.ObjList;
import com.questdb.std.ex.JournalException;
import com.questdb.std.ex.JournalNetworkException;
import com.questdb.store.Interval;
import com.questdb.store.JournalEvents;
import com.questdb.store.JournalWriter;
import com.questdb.store.Partition;

import java.nio.channels.ReadableByteChannel;

public class JournalDeltaConsumer extends AbstractChannelConsumer {

    private final JournalWriter journal;
    private final JournalServerStateConsumer journalServerStateConsumer = new JournalServerStateConsumer();
    private final JournalSymbolTableConsumer journalSymbolTableConsumer;
    private final ObjList<PartitionDeltaConsumer> partitionDeltaConsumers = new ObjList<>();
    private JournalServerState state;
    private PartitionDeltaConsumer lagPartitionDeltaConsumer;

    public JournalDeltaConsumer(JournalWriter journal) {
        this.journal = journal;
        this.journalSymbolTableConsumer = new JournalSymbolTableConsumer(journal);
    }

    @Override
    public void free() {
        journalServerStateConsumer.free();
        journalSymbolTableConsumer.free();
        for (int i = 0, k = partitionDeltaConsumers.size(); i < k; i++) {
            PartitionDeltaConsumer dc = partitionDeltaConsumers.getQuick(i);
            if (dc != null) {
                dc.free();
            }
        }
        if (lagPartitionDeltaConsumer != null) {
            lagPartitionDeltaConsumer.free();
        }
    }

    @Override
    protected void commit() throws JournalNetworkException {
        try {
            journal.commit(false, state.getTxn(), state.getTxPin());
        } catch (JournalException e) {
            throw new JournalNetworkException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {

        try {

            reset();
            journalServerStateConsumer.read(channel);
            this.state = journalServerStateConsumer.getValue();

            if (state.getTxn() == -1) {
                journal.notifyListener(JournalEvents.EVT_JNL_TRANSACTION_REFUSED);
                throw new IncompatibleJournalException("Server refused txn for %s", journal.getLocation());
            }

            if (state.getTxn() < journal.getTxn()) {
                journal.rollback(state.getTxn(), state.getTxPin());
                return;
            }

            journal.beginTx();
            createPartitions(state);

            if (state.isSymbolTables()) {
                journalSymbolTableConsumer.read(channel);
            }

            for (int i = 0, k = state.getNonLagPartitionCount(); i < k; i++) {
                JournalServerState.PartitionMetadata meta = state.getMeta(i);
                if (meta.getEmpty() == 0) {
                    PartitionDeltaConsumer partitionDeltaConsumer = getPartitionDeltaConsumer(meta.getPartitionIndex());
                    partitionDeltaConsumer.read(channel);
                }
            }

            if (state.getLagPartitionName() == null && journal.hasIrregularPartition()) {
                // delete lag partition
                journal.removeIrregularPartition();
            } else if (state.getLagPartitionName() != null) {
                if (lagPartitionDeltaConsumer == null || !journal.hasIrregularPartition()
                        || !state.getLagPartitionName().equals(journal.getIrregularPartition().getName())) {
                    Partition temp = journal.createTempPartition(state.getLagPartitionName());
                    lagPartitionDeltaConsumer = new PartitionDeltaConsumer(temp.open());
                    journal.setIrregularPartition(temp);
                }
                lagPartitionDeltaConsumer.read(channel);
            }
        } catch (JournalException e) {
            throw new JournalNetworkException(e);
        }
    }

    private void createPartitions(JournalServerState metadata) throws JournalException {
        int pc = journal.nonLagPartitionCount() - 1;
        for (int i = 0, k = metadata.getNonLagPartitionCount(); i < k; i++) {
            JournalServerState.PartitionMetadata partitionMetadata = metadata.getMeta(i);
            if (partitionMetadata.getPartitionIndex() > pc) {
                Interval interval = new Interval(partitionMetadata.getIntervalEnd(), partitionMetadata.getIntervalStart());
                journal.createPartition(interval, partitionMetadata.getPartitionIndex());
            }
        }
    }

    private PartitionDeltaConsumer getPartitionDeltaConsumer(int partitionIndex) throws JournalException {
        PartitionDeltaConsumer consumer = partitionDeltaConsumers.getQuiet(partitionIndex);
        if (consumer == null) {
            consumer = new PartitionDeltaConsumer(journal.getPartition(partitionIndex, true));
            partitionDeltaConsumers.extendAndSet(partitionIndex, consumer);
        }

        return consumer;
    }

    private void reset() throws JournalException {
        for (int i = 0, k = partitionDeltaConsumers.size(); i < k; i++) {
            PartitionDeltaConsumer c = partitionDeltaConsumers.getAndSetQuick(i, null);
            if (c != null) {
                c.free();
            }
            journal.getPartition(i, false).close();
        }
    }
}
