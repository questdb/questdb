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

package com.nfsdb.net.producer;

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.collections.Lists;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.logging.Logger;
import com.nfsdb.net.ChannelProducer;
import com.nfsdb.net.model.JournalServerState;
import com.nfsdb.tx.Tx;
import com.nfsdb.utils.Rows;

import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class JournalDeltaProducer implements ChannelProducer {
    private static final Logger LOGGER = Logger.getLogger(JournalDeltaProducer.class);
    private final Journal journal;
    private final JournalServerState journalServerState = new JournalServerState();
    private final JournalServerStateProducer journalServerStateProducer = new JournalServerStateProducer();
    private final List<PartitionDeltaProducer> partitionDeltaProducers = new ArrayList<>();
    private final List<PartitionDeltaProducer> partitionDeltaProducerCache = new ArrayList<>();
    private final JournalSymbolTableProducer journalSymbolTableProducer;
    private PartitionDeltaProducer lagPartitionDeltaProducer;
    private boolean rollback;

    public JournalDeltaProducer(Journal journal) {
        this.journal = journal;
        journalSymbolTableProducer = new JournalSymbolTableProducer(journal);
    }

    public void configure(long txn, long txPin) throws JournalException {

        journalServerState.reset();

        // ignore return value because client can be significantly behind server
        // even though journal has not refreshed we have to compare client and server txns

        journal.refresh();
        long thisTxn = journal.getTxn();
        this.rollback = thisTxn < txn;
        journalServerState.setTxn(thisTxn);
        journalServerState.setTxPin(journal.getTxPin());

        if (thisTxn > txn) {
            configure0(journal.find(txn, txPin));
        }
    }

    @Override
    public boolean hasContent() {
        return rollback || journalServerState.notEmpty();
    }

    @Override
    public void write(WritableByteChannel channel) throws JournalNetworkException {
        journalServerStateProducer.write(channel, journalServerState);

        if (journalSymbolTableProducer.hasContent()) {
            journalSymbolTableProducer.write(channel);
        }

        for (int i = 0, sz = partitionDeltaProducers.size(); i < sz; i++) {
            partitionDeltaProducers.get(i).write(channel);
        }

        if (lagPartitionDeltaProducer != null && lagPartitionDeltaProducer.hasContent()) {
            lagPartitionDeltaProducer.write(channel);
        }

        journalServerState.reset();
        journal.expireOpenFiles();
    }

    private void configure0(Tx tx) throws JournalException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Journal %s size: %d", journal.getLocation(), journal.size());
        }

        int startPartitionIndex;
        long localRowID;

        journalSymbolTableProducer.configure(tx);
        journalServerState.setSymbolTables(journalSymbolTableProducer.hasContent());

        // get non lag partition information
        int nonLagPartitionCount = journal.nonLagPartitionCount();

        if (tx.journalMaxRowID == -1) {
            startPartitionIndex = 0;
            localRowID = 0;
            journalServerState.setNonLagPartitionCount(nonLagPartitionCount);
        } else {
            startPartitionIndex = Rows.toPartitionIndex(tx.journalMaxRowID);
            localRowID = Rows.toLocalRowID(tx.journalMaxRowID);

            if (startPartitionIndex < nonLagPartitionCount) {
                // if slave partition is exactly the same as master partition, advance one partition forward
                // and start building fragment from that
                if (localRowID >= journal.getPartition(startPartitionIndex, true).size()) {
                    localRowID = 0;
                    startPartitionIndex = startPartitionIndex + 1;
                }
                journalServerState.setNonLagPartitionCount(Math.max(0, nonLagPartitionCount - startPartitionIndex));
            } else {
                journalServerState.setNonLagPartitionCount(0);
            }
        }

        // non-lag partition producers
        partitionDeltaProducers.clear();
        for (int i = startPartitionIndex; i < nonLagPartitionCount; i++) {
            PartitionDeltaProducer producer = getPartitionDeltaProducer(i);
            producer.configure(localRowID);
            partitionDeltaProducers.add(producer);
            Partition partition = journal.getPartition(i, false);
            journalServerState.addPartitionMetadata(partition.getPartitionIndex()
                    , partition.getInterval().getLo()
                    , partition.getInterval().getHi()
                    , (byte) (producer.hasContent() ? 0 : 1));

            localRowID = 0;
        }

        // lag partition information
        Partition lag = journal.getIrregularPartition();

        journalServerState.setLagPartitionName(null);

        if (lag != null) {

            if (lagPartitionDeltaProducer == null || lagPartitionDeltaProducer.getPartition() != lag) {
                lagPartitionDeltaProducer = new PartitionDeltaProducer(lag.open());
            }

            if (lag.getName().equals(tx.lagName)) {
                lagPartitionDeltaProducer.configure(tx.lagSize);
            } else {
                lagPartitionDeltaProducer.configure(0);
            }

            if (lagPartitionDeltaProducer.hasContent()) {
                journalServerState.setLagPartitionName(lag.getName());
                journalServerState.setLagPartitionMetadata(lag.getPartitionIndex(), lag.getInterval().getLo(), lag.getInterval().getHi(), (byte) 0);
            }
        } else if (tx.lagName != null) {
            journalServerState.setDetachLag(true);
        }
    }

    private PartitionDeltaProducer getPartitionDeltaProducer(int partitionIndex) throws JournalException {
        // make sure we have delegate list populated for random access
        Lists.advance(partitionDeltaProducerCache, partitionIndex);

        PartitionDeltaProducer producer = partitionDeltaProducerCache.get(partitionIndex);

        if (producer == null) {
            producer = new PartitionDeltaProducer(journal.getPartition(partitionIndex, true));
            partitionDeltaProducerCache.set(partitionIndex, producer);
        }

        return producer;
    }
}
