/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net.ha.producer;

import com.questdb.ex.JournalException;
import com.questdb.ex.JournalNetworkException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Rows;
import com.questdb.net.ha.ChannelProducer;
import com.questdb.net.ha.model.JournalServerState;
import com.questdb.std.ObjList;
import com.questdb.store.Journal;
import com.questdb.store.Partition;
import com.questdb.store.Tx;

import java.nio.channels.WritableByteChannel;

public class JournalDeltaProducer implements ChannelProducer {
    private static final Log LOG = LogFactory.getLog(JournalDeltaProducer.class);
    private final Journal journal;
    private final JournalServerState journalServerState = new JournalServerState();
    private final JournalServerStateProducer journalServerStateProducer = new JournalServerStateProducer();
    private final ObjList<PartitionDeltaProducer> partitionDeltaProducers = new ObjList<>();
    private final ObjList<PartitionDeltaProducer> partitionDeltaProducerCache = new ObjList<>();
    private final JournalSymbolTableProducer journalSymbolTableProducer;
    private PartitionDeltaProducer lagPartitionDeltaProducer;

    public JournalDeltaProducer(Journal journal) {
        this.journal = journal;
        journalSymbolTableProducer = new JournalSymbolTableProducer(journal);
    }

    public void configure(long txn, long txPin) throws JournalException {

        //        LOG.debug().$("Configure ").$(loc).$(" {txn:").$(txn).$(",pin:").$(txPin).$('}').$();

        journalServerState.reset();

        // ignore return value because client can be significantly behind server
        // even though journal has not refreshed we have to compare client and server txns

        journal.refresh();

        long thisTxn = journal.getTxn();
        long thisTxnPin = journal.getTxPin();

        if (thisTxn < txn) {
            // refuse
            LOG.info().$("Cannot sync ").$(journal.getName()).$(". Client TXN is ahead of ours").$(" {txn:").$(txn).$(",pin:").$(txPin).$('}').$();
            journalServerState.setTxn(-1);
        } else if (thisTxn == txn) {
            if (thisTxnPin != txPin) {
                // refuse
                LOG.info().$("Cannot sync ").$(journal.getName()).$(". Client TXN PIN is incorrect").$(" {txn:").$(txn).$(",pin:").$(txPin).$('}').$();
                journalServerState.setTxn(-1);
            }
        } else if (thisTxn > txn) {
            Tx tx = journal.find(txn, txPin);
            if (tx == null) {
                // unknown txn
                LOG.info().$("Cannot sync ").$(journal.getName()).$(". Unknown TXN").$(" {txn:").$(txn).$(",pin:").$(txPin).$('}').$();
                journalServerState.setTxn(-1);
            } else {
                journalServerState.setTxn(thisTxn);
                journalServerState.setTxPin(thisTxnPin);
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
        return journalServerState.notEmpty();
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
        journal.expireOpenFiles0();
    }

    private void configure0(Tx tx) throws JournalException {
        if (LOG.isDebugEnabled()) {
            LOG.debug().$("Journal ").$(journal.getLocation()).$(" size: ").$(journal.size()).$();
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
