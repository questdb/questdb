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

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.net.AbstractChannelConsumer;
import com.nfsdb.journal.net.model.JournalServerState;
import com.nfsdb.journal.utils.Dates;
import com.nfsdb.journal.utils.Lists;
import org.joda.time.Interval;

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
    public boolean isComplete() {
        return state != null && metaIndex >= state.getNonLagPartitionCount() && lagProcessed;
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

        // reset partition consumers
        boolean keep = true;
        for (int i = partitionDeltaConsumers.size() - 1; i >= 0; i--) {
            PartitionDeltaConsumer consumer = partitionDeltaConsumers.get(i);
            if (consumer != null) {
                if (!keep) {
                    partitionDeltaConsumers.set(i, null);
                    try {
                        Partition partition = journal.getPartition(i, false);
                        partition.close();
                    } catch (JournalException e) {
                        throw new JournalRuntimeException(e);
                    }
                } else {
                    consumer.reset();
                    keep = false;
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doRead(ReadableByteChannel channel) throws JournalNetworkException {
        journalServerStateConsumer.read(channel);
        if (journalServerStateConsumer.isComplete()) {

            journal.beginTx();

            try {
                if (state == null) {
                    state = journalServerStateConsumer.getValue();
                    createPartitions(state);
                }

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
    protected void commit() throws JournalNetworkException {
        try {
            journal.commit();
        } catch (JournalException e) {
            throw new JournalNetworkException(e);
        }
    }

    private void createPartitions(JournalServerState metadata) throws JournalException {
        for (int i = 0; i < metadata.getNonLagPartitionCount(); i++) {
            JournalServerState.PartitionMetadata partitionMetadata = metadata.getMeta(i);
            if (partitionMetadata.getPartitionIndex() >= journal.nonLagPartitionCount()) {
                Interval interval = Dates.interval(partitionMetadata.getIntervalStart(), partitionMetadata.getIntervalEnd());
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
