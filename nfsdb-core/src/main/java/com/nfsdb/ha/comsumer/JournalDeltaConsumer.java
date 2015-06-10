/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ha.comsumer;

import com.nfsdb.JournalWriter;
import com.nfsdb.Partition;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.IncompatibleJournalException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.AbstractChannelConsumer;
import com.nfsdb.ha.model.JournalServerState;
import com.nfsdb.utils.Interval;

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
            partitionDeltaConsumers.getQuick(i).free();
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
                journal.notifyTxError();
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
