/*
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
 */

package com.nfsdb.ha.producer;

import com.nfsdb.Journal;
import com.nfsdb.Partition;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.ha.ChannelProducer;
import com.nfsdb.ha.model.JournalServerState;
import com.nfsdb.logging.Logger;
import com.nfsdb.storage.Tx;
import com.nfsdb.utils.Rows;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.nio.channels.WritableByteChannel;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
public class JournalDeltaProducer implements ChannelProducer {
    private static final Logger LOGGER = Logger.getLogger(JournalDeltaProducer.class);
    private final Journal journal;
    private final JournalServerState journalServerState = new JournalServerState();
    private final JournalServerStateProducer journalServerStateProducer = new JournalServerStateProducer();
    private final ObjList<PartitionDeltaProducer> partitionDeltaProducers = new ObjList<>();
    private final ObjList<PartitionDeltaProducer> partitionDeltaProducerCache = new ObjList<>();
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
            Tx tx = journal.find(txn, txPin);
            if (tx == null) {
                // indicate to client that their txn is invalid
                journalServerState.setTxn(-1);
            } else {
                configure0(tx);
            }
        }
    }

    @Override
    public void free() {
        journalServerStateProducer.free();
        journalSymbolTableProducer.free();
        if (lagPartitionDeltaProducer != null) {
            lagPartitionDeltaProducer.free();
        }

        for (int i = 0, sz = partitionDeltaProducerCache.size(); i < sz; i++) {
            PartitionDeltaProducer p = partitionDeltaProducerCache.getQuick(i);
            if (p != null) {
                p.free();
            }
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
            partitionDeltaProducers.getQuick(i).write(channel);
        }

        if (lagPartitionDeltaProducer != null && lagPartitionDeltaProducer.hasContent()) {
            lagPartitionDeltaProducer.write(channel);
        }

        journalServerState.reset();
        journal.expireOpenFiles();
    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS"})
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
        PartitionDeltaProducer producer = partitionDeltaProducerCache.getQuiet(partitionIndex);
        if (producer == null) {
            producer = new PartitionDeltaProducer(journal.getPartition(partitionIndex, true));
            partitionDeltaProducerCache.extendAndSet(partitionIndex, producer);
        }

        return producer;
    }
}
