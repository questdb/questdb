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

package com.nfsdb.ha.comsumer;

import com.nfsdb.JournalWriter;
import com.nfsdb.Partition;
import com.nfsdb.exceptions.IncompatibleJournalException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.ha.AbstractChannelConsumer;
import com.nfsdb.ha.model.JournalServerState;
import com.nfsdb.utils.Interval;
import com.nfsdb.utils.Lists;

import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;

public class JournalDeltaConsumer extends AbstractChannelConsumer {

    private final JournalWriter journal;
    private final JournalServerStateConsumer journalServerStateConsumer = new JournalServerStateConsumer();
    private final JournalSymbolTableConsumer journalSymbolTableConsumer;
    private final ArrayList<PartitionDeltaConsumer> partitionDeltaConsumers = new ArrayList<>();
    private JournalServerState state;
    private PartitionDeltaConsumer lagPartitionDeltaConsumer;
    private int metaIndex = -1;
    private boolean lagProcessed = false;

    public JournalDeltaConsumer(JournalWriter journal) {
        this.journal = journal;
        this.journalSymbolTableConsumer = new JournalSymbolTableConsumer(journal);
    }

    @Override
    public void free() {
        super.free();
        journalServerStateConsumer.free();
        journalSymbolTableConsumer.free();
        for (int i = 0; i < partitionDeltaConsumers.size(); i++) {
            partitionDeltaConsumers.get(i).free();
        }
        if (lagPartitionDeltaConsumer != null) {
            lagPartitionDeltaConsumer.free();
        }
    }

    @Override
    public void reset() {
        super.reset();
        journalServerStateConsumer.reset();
        state = null;
        metaIndex = -1;
        lagProcessed = false;
        journalSymbolTableConsumer.reset();
        if (lagPartitionDeltaConsumer != null) {
            lagPartitionDeltaConsumer.reset();
        }

        int recent = partitionDeltaConsumers.size() - 1;

        if (recent < 0) {
            return;
        }

        try {
            PartitionDeltaConsumer c = partitionDeltaConsumers.get(recent);

            if (c != null) {
                if (!journal.getPartition(recent, false).isOpen()) {
                    partitionDeltaConsumers.set(recent, null).free();
                } else {
                    c.reset();
                }
            }

            for (int i = 0; i < recent; i++) {
                c = partitionDeltaConsumers.set(i, null);
                if (c != null) {
                    c.free();
                }
                journal.getPartition(i, false).close();
            }
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
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
        journalServerStateConsumer.read(channel);
        if (journalServerStateConsumer.isComplete()) {

            this.state = journalServerStateConsumer.getValue();

            try {

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

                if (!state.isSymbolTables() || journalSymbolTableConsumer.isComplete()) {
                    if (metaIndex == -1) {
                        metaIndex = 0;
                    }

                    while (metaIndex < state.getNonLagPartitionCount()) {
                        JournalServerState.PartitionMetadata meta = state.getMeta(metaIndex);
                        if (meta.getEmpty() == 0) {
                            PartitionDeltaConsumer partitionDeltaConsumer = getPartitionDeltaConsumer(meta.getPartitionIndex());
                            partitionDeltaConsumer.read(channel);

                            if (partitionDeltaConsumer.isComplete()) {
                                metaIndex++;
                            } else {
                                break;
                            }
                        } else {
                            metaIndex++;
                        }
                    }

                    if (metaIndex >= state.getNonLagPartitionCount() && !lagProcessed) {
                        if (state.getLagPartitionName() == null && journal.hasIrregularPartition()) {
                            // delete lag partition
                            journal.removeIrregularPartition();
                            lagProcessed = true;
                        } else if (state.getLagPartitionName() == null && !journal.hasIrregularPartition()) {
                            lagProcessed = true;
                        } else if (state.getLagPartitionName() != null) {
                            if (lagPartitionDeltaConsumer == null || !journal.hasIrregularPartition()
                                    || !state.getLagPartitionName().equals(journal.getIrregularPartition().getName())) {
                                Partition temp = journal.createTempPartition(state.getLagPartitionName());
                                lagPartitionDeltaConsumer = new PartitionDeltaConsumer(temp.open());
                                journal.setIrregularPartition(temp);
                            }
                            lagPartitionDeltaConsumer.read(channel);
                            lagProcessed = lagPartitionDeltaConsumer.isComplete();
                        }
                    }

                }
            } catch (JournalException e) {
                throw new JournalNetworkException(e);
            }
        }
    }

    @Override
    public boolean isComplete() {
        return state != null && metaIndex >= state.getNonLagPartitionCount() && lagProcessed;
    }

    private void createPartitions(JournalServerState metadata) throws JournalException {
        int pc = journal.nonLagPartitionCount();
        for (int i = 0; i < metadata.getNonLagPartitionCount(); i++) {
            JournalServerState.PartitionMetadata partitionMetadata = metadata.getMeta(i);
            if (partitionMetadata.getPartitionIndex() >= pc) {
                Interval interval = new Interval(partitionMetadata.getIntervalEnd(), partitionMetadata.getIntervalStart());
                journal.createPartition(interval, partitionMetadata.getPartitionIndex());
            }
        }
    }

    private PartitionDeltaConsumer getPartitionDeltaConsumer(int partitionIndex) throws JournalException {
        Lists.advance(partitionDeltaConsumers, partitionIndex);

        PartitionDeltaConsumer consumer = partitionDeltaConsumers.get(partitionIndex);
        if (consumer == null) {
            consumer = new PartitionDeltaConsumer(journal.getPartition(partitionIndex, true));
            partitionDeltaConsumers.set(partitionIndex, consumer);
        }

        return consumer;
    }
}
